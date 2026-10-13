#!/usr/bin/env bash
#
# Run consensus_vote_matrix.py from inside Docker with host-side
# consensus-analysis restarts managed by this script.
#

set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/consensus_vote_matrix.py"
REPORT_DIR="$(pwd)/.tmp"
mkdir -p "$REPORT_DIR"

# Ensure docker compose binary is available
if ! command -v docker &>/dev/null; then
    echo "ERROR: docker command not found" >&2
    exit 1
fi

# Function to restart consensus-analysis with given models
restart_consensus() {
    local models="$1"
    local timeout_sec="${2:-5}"
    local retention_sec="${3:-10}"

    echo "[runner] Restarting consensus-analysis: models=$models timeout=$timeout_sec retention=$retention_sec"

    DASMA_SELECTED_MODELS="$models" \
    CONSENSUS_TIMEOUT_SEC="$timeout_sec" \
    CONSENSUS_RETENTION_SEC="$retention_sec" \
    docker compose up --pull never -d --force-recreate consensus-analysis

    sleep 3

    # Wait for service to be ready
    local attempts=0
    while [ $attempts -lt 10 ]; do
        local logs=$(docker compose logs --tail 5 consensus-analysis 2>&1)
        if echo "$logs" | grep -q "active_models="; then
            echo "[runner] consensus-analysis ready"
            return 0
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    echo "[runner] WARNING: consensus-analysis may not be ready" >&2
    return 0
}

# Run one case group
run_suite() {
    local suite="$1"
    local selected_models="$2"
    local report_suffix="${3:-${selected_models//,/-}}"

    restart_consensus "$selected_models"

    local report_path="$REPORT_DIR/consensus-${report_suffix}-report.json"

    docker compose run --rm --no-deps --entrypoint python consensus-analysis \
        "$SCRIPT" \
        --suite "$suite" \
        --selected-models "$selected_models" \
        --no-manage-consensus \
        --skip-influx \
        --report-path "/tmp/consensus-${report_suffix}-report.json" \
        2>&1 | sed 's/^/[docker-run] /'

    # Copy report from container if it exists
    docker compose cp consensus-analysis:/tmp/consensus-${report_suffix}-report.json "$report_path" 2>/dev/null || true

    if [ -f "$report_path" ]; then
        echo "[runner] Report saved to $report_path"
        python3 -c "
import json, sys
with open('$report_path') as f:
    d = json.load(f)
passed = d.get('passed', 0)
total = d.get('total_cases', 0)
print(f'Summary: {passed}/{total} passed')
if d.get('failed', 0) > 0:
    for c in d.get('cases', []):
        if not c.get('passed'):
            print(f'  FAIL: {c[\"case_id\"]} - {c[\"errors\"]}')
"
    fi
}

# Phase 1: Individual models
echo ""
echo "===== PHASE 1: Individual Models ====="

for model in hstree iforest kitnet deepant mdragpoint; do
    echo ""
    echo "--- Testing $model alone ---"
    run_suite "full" "$model" "$model"
done

# Negative control
echo ""
echo "--- Testing kitnet normal (control) ---"
# Note: kitnet is in "simple" suite but we need just the normal control
# The simple suite has kitnet=true, not false. Let me use --only filter
restart_consensus "kitnet"
docker compose run --rm --no-deps --entrypoint python consensus-analysis \
    "$SCRIPT" \
    --suite full \
    --no-manage-consensus \
    --skip-influx \
    --only "normal" \
    --report-path "/tmp/consensus-normal-control-report.json" \
    2>&1 | sed 's/^/[docker-run] /'

# Phase 2: Pairwise
echo ""
echo "===== PHASE 2: Pairwise Combinations ====="

for models in "hstree,kitnet" "iforest,mdragpoint" "deepant,hstree"; do
    suffix="${models//,/-}"
    echo ""
    echo "--- Testing $models ---"
    run_suite "full" "$models" "$suffix"
done

# Note: pair-kitnet-mdragpoint-one needs mixed verdicts, handled specially below

# Phase 3: Higher-order
echo ""
echo "===== PHASE 3: Higher-order Combinations ====="

for models in "hstree,iforest,kitnet" "deepant,kitnet,mdragpoint" "hstree,iforest,kitnet,mdragpoint" "hstree,iforest,kitnet,deepant,mdragpoint"; do
    suffix="${models//,/-}"
    echo ""
    echo "--- Testing $models ---"
    run_suite "full" "$models" "$suffix"
done

# Phase 4: Synchronization (special cases)
echo ""
echo "===== PHASE 4: Synchronization Cases ====="

# 4a: Out-of-order (hstree,iforest,kitnet,mdragpoint)
echo ""
echo "--- Phase 4a: Out-of-order ---"
restart_consensus "hstree,iforest,kitnet,mdragpoint"
docker compose run --rm --no-deps --entrypoint python consensus-analysis \
    "$SCRIPT" \
    --suite full \
    --no-manage-consensus \
    --skip-influx \
    --only "oo-arrival" \
    --report-path "/tmp/consensus-oo-report.json" \
    2>&1 | sed 's/^/[docker-run] /'

# 4b: Same second (kitnet,mdragpoint)
echo ""
echo "--- Phase 4b: Same second events ---"
restart_consensus "kitnet,mdragpoint"
docker compose run --rm --no-deps --entrypoint python consensus-analysis \
    "$SCRIPT" \
    --suite full \
    --no-manage-consensus \
    --skip-influx \
    --only "same-sec" \
    --report-path "/tmp/consensus-samesecond-report.json" \
    2>&1 | sed 's/^/[docker-run] /'

# 4c: Timeout then late completion
echo ""
echo "--- Phase 4c: Timeout/late completion ---"
restart_consensus "hstree,iforest,kitnet" 3 10
docker compose run --rm --no-deps --entrypoint python consensus-analysis \
    "$SCRIPT" \
    --suite full \
    --no-manage-consensus \
    --skip-influx \
    --only "late-complete" \
    --report-path "/tmp/consensus-late-report.json" \
    2>&1 | sed 's/^/[docker-run] /'

echo ""
echo "===== EXPERIMENT COMPLETE ====="
echo "Reports saved to $REPORT_DIR/"
ls -la "$REPORT_DIR"/consensus-*.json 2>/dev/null || echo "(no reports found)"

# Merge all reports
python3 -c "
import json, glob, sys
reports = sorted(glob.glob('$REPORT_DIR/consensus-*-report.json'))
all_passed = 0
all_total = 0
all_cases = []
for path in reports:
    try:
        with open(path) as f:
            d = json.load(f)
        all_passed += d.get('passed', 0)
        all_total += d.get('total_cases', 0)
        for c in d.get('cases', []):
            c['report_file'] = path
        all_cases.extend(d.get('cases', []))
    except Exception as e:
        print(f'WARNING: could not read {path}: {e}', file=sys.stderr)

merged = {
    'start_timestamp': all_cases[0]['report_file'].split('/')[-1] if all_cases else 'N/A',
    'total_cases': all_total,
    'passed': all_passed,
    'failed': all_total - all_passed,
    'cases': all_cases,
}
with open('$REPORT_DIR/consensus-model-vote-experiment-report.json', 'w') as f:
    json.dump(merged, f, indent=2)
print(f'MERGED: {all_passed}/{all_total} passed')
for c in all_cases:
    if not c.get('passed'):
        print(f'  FAIL: {c[\"case_id\"]} - {c[\"errors\"]}')
"