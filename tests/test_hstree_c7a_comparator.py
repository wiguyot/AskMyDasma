import importlib.util
from pathlib import Path


_SCRIPT = Path(__file__).parents[1] / "experiments/hstree/compare-hstree-c7a.py"
_SPEC = importlib.util.spec_from_file_location("compare_hstree_c7a", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
compare_records = _MODULE.compare_records
replay_campaign_p99 = _MODULE.replay_campaign_p99


def _campaign(score: float = -4.0) -> dict:
    return {
        "Group": "A", "source_event_id": "event-1", "hstree_score": score,
        "hstree_status": "normal", "hstree_anomaly": False, "hstree_threshold": -3.0,
    }


def _replay(score: float = -4.0) -> dict:
    return {
        "Group": "A", "source_event_id": "event-1", "score": score,
        "p99_status": "normal", "p99_threshold": -3.0,
    }


def test_c7a_comparator_matches_exact_records_by_identity() -> None:
    report = compare_records([_campaign()], [_replay()])

    assert report["verdict"] == "match"
    assert report["shared_identities"] == 1
    assert report["mismatch_count"] == 0


def test_c7a_comparator_reports_first_mismatch_and_missing_identities() -> None:
    campaign = [_campaign(score=-2.0), {**_campaign(), "source_event_id": "event-extra"}]
    replay = [_replay()]

    report = compare_records(campaign, replay)

    assert report["verdict"] == "mismatch"
    assert report["campaign_only"] == [["A", "event-extra"]]
    assert report["first_mismatch"]["source_event_id"] == "event-1"
    assert "hstree_score" in report["first_mismatch"]["fields"]


def test_replay_p99_uses_per_group_warmup_and_archived_score_order() -> None:
    records = [
        {"Group": "A", "source_event_id": "event-1", "hstree_score": 0.0},
        {"Group": "A", "source_event_id": "event-2", "hstree_score": -1.0},
        {"Group": "A", "source_event_id": "event-3", "hstree_score": -2.0},
        {"Group": "A", "source_event_id": "event-4", "hstree_score": 10.0},
    ]

    replay = replay_campaign_p99(
        records, initial_window=1, threshold_window=10, min_samples=2
    )

    assert [record["p99_status"] for record in replay] == [
        "warming_up", "warming_up", "warming_up", "anomaly",
    ]
    assert replay[-1]["p99_threshold"] == -1.01
