"""Static contracts for the repeatability compose services and observability.

Step 3 of docs/planning/repeatability-reproducibility-spec.md: the generic
repeatability pair (repeatability-v1 / repeatability-v2), the repeatability-diff
service shell, the InfluxDB buckets and the Telegraf flows must exist and stay
mutually consistent — without starting any broker, InfluxDB or container.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
TELEGRAF = (ROOT / "observability" / "telegraf" / "telegraf.conf").read_text(encoding="utf-8")

DETERMINISM_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "PYTHONHASHSEED",
)

MODEL_GROUP_VARS = (
    "ANALYSE_HSTREE_GROUP_ID",
    "ANALYSE_IFOREST_GROUP_ID",
    "ANALYSE_KITNET_GROUP_ID",
    "ANALYSE_DEEPANT_GROUP_ID",
    "ANALYSE_MDRAGPOINT_GROUP_ID",
)


def _compose_service_section(service_name: str) -> str:
    marker = f"  {service_name}:"
    section = COMPOSE.split(marker, 1)[1]
    next_service = re.search(r"\n  [A-Za-z0-9_-]+:", section)
    if next_service is None:
        return section
    return section[: next_service.start()]


def _telegraf_blocks(header: str) -> list[str]:
    blocks = []
    for part in TELEGRAF.split(header)[1:]:
        end = re.search(r"\n\[\[", part)
        blocks.append(part if end is None else part[: end.start()])
    return blocks


def test_repeatability_pair_services_declared_and_disabled() -> None:
    for instance in ("v1", "v2"):
        section = _compose_service_section(f"repeatability-{instance}")
        group = f"repeatability-{instance}-consumer"

        assert (
            f'image: "${{REPEATABILITY_IMAGE:-dasma-kafka-anomaly-light:0.1.0}}"' in section
        ), f"repeatability-{instance} must inject the image from the campaign env"
        assert "build:" not in section, f"repeatability-{instance} must not declare a build block"
        assert (
            'entrypoint: ["python", "/app/${REPEATABILITY_SCRIPT:-runtime/detectors/hstree/hstree_analysis.py}"]'
            in section
        )
        assert 'KAFKA_SOURCE_TOPIC: "source-data"' in section
        assert f'KAFKA_DEST_TOPIC: "repeatability-{instance}"' in section
        assert f'KAFKA_DEST_VIZ_TOPIC: "repeatability-{instance}-viz"' in section
        assert 'TOPIC_PARTITIONS: "1"' in section
        assert 'TOPIC_RF: "3"' in section
        assert f'DASMA_ENABLED: "${{DASMA_REPEATABILITY_ENABLED:-0}}"' in section
        assert 'restart: "no"' in section
        assert "- ./:/app:ro" in section
        assert "working_dir: /app" in section
        assert "networks: [kafka-net]" in section
        for var in MODEL_GROUP_VARS:
            assert f"{var}: \"{group}\"" in section, (
                f"repeatability-{instance} must set {var} to {group}"
            )


def test_repeatability_pair_is_deterministic() -> None:
    for instance in ("v1", "v2"):
        section = _compose_service_section(f"repeatability-{instance}")
        for var in DETERMINISM_VARS[:-1]:
            assert f'{var}: "1"' in section, f"repeatability-{instance} must pin {var}"
        assert 'PYTHONHASHSEED: "0"' in section


def test_repeatability_pair_is_mirror() -> None:
    """The two instance blocks must differ only in their v1/v2 identity."""
    v1 = _compose_service_section("repeatability-v1")
    v2 = _compose_service_section("repeatability-v2")
    assert v1.replace("repeatability-v1", "repeatability-v2") == v2


def test_all_hstree_services_default_to_equal_256_windows() -> None:
    for service in (
        "hstree-analysis",
        "repeatability-v1",
        "repeatability-v2",
        "explainability",
        "explainability-v2",
    ):
        section = _compose_service_section(service)
        assert 'HSTREE_INITIAL_WINDOW_SIZE: "${HSTREE_INITIAL_WINDOW_SIZE:-256}"' in section
        assert 'HSTREE_WINDOW_SIZE: "${HSTREE_WINDOW_SIZE:-256}"' in section
    assert COMPOSE.count('HSTREE_WELFORD_WINDOW: "${HSTREE_WELFORD_WINDOW:-2048}"') == 3


def test_p99_is_the_default_for_hstree_and_iforest_services() -> None:
    for service in ("hstree-analysis", "repeatability-v1", "repeatability-v2"):
        section = _compose_service_section(service)
        assert 'HSTREE_THRESHOLD_MODE: "${HSTREE_THRESHOLD_MODE:-p99}"' in section

    iforest_section = _compose_service_section("iforest-analysis")
    assert 'IFOREST_THRESHOLD_MODE: "${IFOREST_THRESHOLD_MODE:-native}"' in iforest_section

    for service, variable in (
        ("kitnet-analysis", "KITNET_THRESHOLD_MODE"),
        ("deepant-analysis", "DEEPANT_THRESHOLD_MODE"),
    ):
        section = _compose_service_section(service)
        assert f'{variable}: "${{{variable}:-welford-windowed}}"' in section


def test_repeatability_diff_service_shell() -> None:
    section = _compose_service_section("repeatability-diff")

    assert "dockerfile: services/kafka-tools/Dockerfile" in section
    assert 'image: dasma-kafka-kafka-tools:0.1.0' in section
    assert 'entrypoint: ["python", "/app/validation/repeatability/repeatability_diff.py"]' in section
    assert 'REPEATABILITY_VIZ_TOPIC_A: "${REPEATABILITY_VIZ_TOPIC_A:-repeatability-v1-viz}"' in section
    assert 'REPEATABILITY_VIZ_TOPIC_B: "${REPEATABILITY_VIZ_TOPIC_B:-repeatability-v2-viz}"' in section
    assert 'REPEATABILITY_DEST_TOPIC: "${REPEATABILITY_DEST_TOPIC:-repeatability-diff}"' in section
    assert 'REPEATABILITY_GROUP_ID: "${REPEATABILITY_GROUP_ID:-repeatability-diff-consumer}"' in section
    assert 'REPEATABILITY_MODEL_ID: "${REPEATABILITY_MODEL_ID:-hstree}"' in section
    assert 'REPEATABILITY_SCORE_FIELD: "${REPEATABILITY_SCORE_FIELD:-hstree_score}"' in section
    assert 'REPEATABILITY_STATUS_FIELD: "${REPEATABILITY_STATUS_FIELD:-hstree_status}"' in section
    assert 'REPEATABILITY_THRESHOLD_FIELD: "${REPEATABILITY_THRESHOLD_FIELD:-hstree_threshold}"' in section
    assert 'REPEATABILITY_ANOMALY_FIELD: "${REPEATABILITY_ANOMALY_FIELD:-hstree_anomaly}"' in section
    assert 'REPEATABILITY_EXTRA_FIELDS: "${REPEATABILITY_EXTRA_FIELDS:-}"' in section
    assert 'REPEATABILITY_TIMEOUT_SEC: "${REPEATABILITY_TIMEOUT_SEC:-30}"' in section
    assert 'REPEATABILITY_RETENTION_SEC: "${REPEATABILITY_RETENTION_SEC:-300}"' in section
    assert 'DASMA_ENABLED: "${DASMA_REPEATABILITY_ENABLED:-0}"' in section
    assert 'restart: "no"' in section


def test_repeatability_diff_entrypoint_script_exists() -> None:
    """Step 4: the compose entrypoint target must exist and import cleanly."""
    from validation.repeatability import repeatability_diff

    script = ROOT / "validation" / "repeatability" / "repeatability_diff.py"
    assert script.is_file()
    assert hasattr(repeatability_diff, "main")
    assert hasattr(repeatability_diff, "process_arrival")
    assert hasattr(repeatability_diff, "build_diff_record")


def test_repeatability_buckets_created_without_retention() -> None:
    for bucket in ("repeatability-v1", "repeatability-v2", "repeatability-diff"):
        lines = [
            line.strip()
            for line in COMPOSE.splitlines()
            if line.strip() == f'ensure_no_retention "{bucket}"'
        ]
        assert len(lines) == 1, f"bucket {bucket} must be ensured exactly once"


def test_telegraf_repeatability_inputs() -> None:
    expected = {
        "repeatability-v1-viz": (
            "telegraf-repeatability-v1",
            "repeatability-v1",
            ["Group"],
            ["timestamp"],
        ),
        "repeatability-v2-viz": (
            "telegraf-repeatability-v2",
            "repeatability-v2",
            ["Group"],
            ["timestamp"],
        ),
        "repeatability-diff": (
            "telegraf-repeatability-diff",
            "repeatability-diff",
            ["Group", "source_event_id"],
            ["timestamp", "source_event_id", "emit_reason"],
        ),
    }

    inputs = [
        block
        for block in _telegraf_blocks("[[inputs.kafka_consumer]]")
        if "repeatability" in block
    ]
    assert len(inputs) == 3, "exactly three repeatability telegraf inputs are expected"

    for block in inputs:
        topics = re.findall(r'topics = \["([^"]+)"\]', block)
        assert len(topics) == 1 and topics[0] in expected, f"unexpected topic {topics}"
        consumer_group, measurement, tag_keys, string_fields = expected[topics[0]]

        assert f'consumer_group = "{consumer_group}"' in block
        assert f'name_override = "{measurement}"' in block
        assert f"tag_keys = {tag_keys!r}".replace("'", '"') in block
        assert f"json_string_fields = {string_fields!r}".replace("'", '"') in block
        assert 'json_time_key = "timestamp"' in block
        assert 'json_time_format = "2006-01-02 15:04:05"' in block
        assert 'offset = "oldest"' in block


def test_telegraf_repeatability_outputs_use_distinct_buckets() -> None:
    outputs = [
        block
        for block in _telegraf_blocks("[[outputs.influxdb_v2]]")
        if "repeatability" in block
    ]
    assert len(outputs) == 3, "exactly three repeatability telegraf outputs are expected"

    buckets = set()
    for block in outputs:
        bucket = re.findall(r'bucket = "([^"]+)"', block)
        namepass = re.findall(r'namepass = \["([^"]+)"\]', block)
        assert len(bucket) == 1 and len(namepass) == 1
        assert bucket[0] == namepass[0], "measurement and bucket must stay aligned"
        buckets.add(bucket[0])

    assert buckets == {"repeatability-v1", "repeatability-v2", "repeatability-diff"}
    # v1 and v2 must never share a bucket: identical series would overwrite
    # each other inside InfluxDB.
    assert "repeatability-v1" != "repeatability-v2"
