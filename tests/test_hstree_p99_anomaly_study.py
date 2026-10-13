import importlib.util
from pathlib import Path


_SCRIPT = Path(__file__).parents[1] / "experiments/hstree/hstree-p99-anomaly-study.py"
_SPEC = importlib.util.spec_from_file_location("hstree_p99_anomaly_study", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
summarize_events = _MODULE.summarize_events


def test_summarize_events_counts_rates_windows_and_shared_event_time() -> None:
    events = [
        {
            "Group": "A", "score": -4.0, "p99_status": "normal",
            "welford_status": "normal", "reference_window": 0,
            "position_after_rollover": 1, "timestamp": "2020-01-01 12:00:01",
        },
        {
            "Group": "A", "score": 2.0, "p99_status": "anomaly",
            "welford_status": "normal", "reference_window": 1,
            "position_after_rollover": 2, "timestamp": "2020-01-01 12:00:42",
        },
        {
            "Group": "B", "score": 3.0, "p99_status": "anomaly",
            "welford_status": "anomaly", "reference_window": 1,
            "position_after_rollover": 2, "timestamp": "2020-01-01 12:00:50",
        },
        {
            "Group": "B", "score": -3.0, "p99_status": "warming_up",
            "welford_status": "warming_up", "reference_window": 1,
            "position_after_rollover": 3, "timestamp": "2020-01-01 12:01:00",
        },
    ]

    summary = summarize_events(events)

    assert summary["rows"] == 4
    assert summary["scored"] == 3
    assert summary["p99_anomalies"] == 2
    assert summary["p99_anomaly_rate"] == 2 / 3
    assert summary["welford_anomalies"] == 1
    assert summary["by_group"]["B"] == {
        "events": 2, "model_scores": 2, "threshold_warmup": 1,
        "scored": 1, "anomalies": 1, "welford_anomalies": 1,
    }
    assert summary["by_reference_window"]["1"]["anomalies"] == 2
    assert summary["anomaly_position_after_rollover"] == {"2": 2}
    assert summary["minutes_with_anomalies_in_multiple_groups"] == {
        "2020-01-01 12:00:00": ["A", "B"]
    }
