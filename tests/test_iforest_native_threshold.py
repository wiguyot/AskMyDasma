"""Native iForest decisions stay attached to the event's scoring forest."""

import logging
import json
from collections import deque
from types import SimpleNamespace

import numpy as np
import pytest
from pydantic import ValidationError

from dasma_kafka.common.explained_models import IForestModelManager
from dasma_kafka.common.threshold_modes import normalize_iforest_threshold_mode
from dasma_kafka.explainability.config import ExplainabilityConfig, validate_explainability_selection
from dasma_kafka.explainability_v2.config import ExplainabilityV2Config
from dasma_kafka.explainability_v2.config import validate_explainability_selection as validate_v2_selection
from dasma_kafka.iforest_analysis.helpers import classify_iforest_native
from dasma_kafka.iforest_analysis.model import IForestGlobalModel
from dasma_kafka.web.models import CampaignLaunchRequest
from scripts.web.campaign_config import build_env, load_campaign_dict


def make_model(contamination="auto"):
    return IForestGlobalModel(
        num_features=2, initial_window_size=16, window_size=32,
        max_samples=8, n_estimators=11, refit_every=3, contamination=contamination,
    )


@pytest.mark.parametrize("contamination", ["auto", "0.01", "0.10", "0.50"])
def test_native_verdict_matches_sklearn_across_groups_and_refits(contamination):
    rng = np.random.default_rng(9)
    rows = rng.normal(size=(90, 2))
    rows[::11] += 8
    models = {group: make_model(contamination) for group in ("A", "B")}
    refit_cutoff_changed = False
    for index, row in enumerate(rows):
        model = models["A" if index % 2 else "B"]
        scoring_forest = model.current_model
        score = model.partial_fit_score(row)
        verdict = classify_iforest_native(
            score, model.last_scoring_threshold, is_warming_up=model.is_warming_up,
        )
        if scoring_forest is None:
            assert verdict == ("warming_up", None, None)
            assert model.last_scoring_threshold is None
            continue
        expected_anomaly = bool(scoring_forest.predict([row])[0] == -1)
        assert model.last_scoring_model is scoring_forest
        assert verdict == (
            "anomaly" if expected_anomaly else "normal",
            expected_anomaly, float(-scoring_forest.offset_),
        )
        if contamination == "auto":
            assert model.last_scoring_threshold == 0.5
        elif model.current_model is not scoring_forest:
            refit_cutoff_changed |= model.current_model.offset_ != scoring_forest.offset_
    assert all(model.generation > 1 for model in models.values())
    if contamination != "auto":
        assert refit_cutoff_changed, "Exercise an event where the next forest has another cutoff"


def test_native_equality_is_normal_and_missing_score_has_no_verdict():
    assert classify_iforest_native(0.5, 0.5, is_warming_up=False) == ("normal", False, 0.5)
    assert classify_iforest_native(0.51, 0.5, is_warming_up=False) == ("anomaly", True, 0.5)
    assert classify_iforest_native(None, 0.5, is_warming_up=False) == ("warming_up", None, None)


def test_contamination_changes_cutoff_without_changing_streaming_scores():
    rows = np.random.default_rng(2).normal(size=(36, 2))
    auto = make_model()
    numeric = make_model("0.10")
    np.testing.assert_array_equal(
        [auto.partial_fit_score(row) for row in rows],
        [numeric.partial_fit_score(row) for row in rows],
    )
    assert auto.last_scoring_threshold != numeric.last_scoring_threshold


@pytest.mark.parametrize("percent", range(1, 51))
def test_campaign_accepts_each_percentage_and_propagates_it(percent):
    value = f"{percent / 100:.2f}"
    config = load_campaign_dict({
        "dataset_path": "data/source.csv", "selected_models": ["iforest"],
        "iforest_contamination": value,
    })
    assert config["iforest_estimator"] == "native"
    assert build_env(config)["IFOREST_CONTAMINATION"] == value
    assert build_env(config)["IFOREST_THRESHOLD_MODE"] == "native"


@pytest.mark.parametrize("value", ["0", "0.51", "1", "0.015", "nan", "inf", "bad"])
def test_campaign_rejects_invalid_contamination(value):
    with pytest.raises(ValueError, match="contamination doit être"):
        load_campaign_dict({
            "dataset_path": "data/source.csv", "selected_models": ["iforest"],
            "iforest_contamination": value,
        })


def test_native_is_iforest_only_and_normalizer_preserves_external_modes():
    request = CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch")
    assert request.iforest_estimator == "native"
    with pytest.raises(ValidationError):
        CampaignLaunchRequest(dataset_path="data/source.csv", mode="batch", hstree_estimator="native")
    for mode in ("native", "p99", "welford", "welford-windowed"):
        assert normalize_iforest_threshold_mode(mode) == mode
    for mode in (None, "", "unknown", " NATIVE "):
        assert normalize_iforest_threshold_mode(mode) == "native"


@pytest.mark.parametrize("config_type,validate", [
    (ExplainabilityConfig, validate_explainability_selection),
    (ExplainabilityV2Config, validate_v2_selection),
])
def test_explainability_receives_contamination_and_keeps_scores_in_sync(config_type, validate, monkeypatch):
    monkeypatch.setenv("IFOREST_CONTAMINATION", "0.10")
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    config = config_type.from_environment()
    config.explained_model = "iforest"
    config.explainability_mode = "simple"
    config.explanation_method = "kernel"
    config.hstree_seed = 42
    config.source_topic = "source-data"
    config.iforest_initial_window_size = 16
    config.iforest_window_size = 32
    config.iforest_max_samples = 8
    config.iforest_n_estimators = 11
    config.iforest_refit_every = 3
    config.shap_background_size = 2
    validate(config)
    assert config.iforest_contamination == "0.10"
    manager = IForestModelManager(config, logging.getLogger("native-test"))
    source = make_model("0.10")
    for row in np.random.default_rng(4).normal(size=(25, 2)):
        score = source.partial_fit_score(row)
        mirrored = manager.fit_score("A", row)
        if source.is_warming_up:
            assert mirrored is None
        else:
            assert mirrored == score
            assert manager.models["A"].last_scoring_threshold == source.last_scoring_threshold


@pytest.mark.parametrize("mode,contamination", [("native", "auto"), ("native", "0.10"), ("p99", "auto")])
def test_runtime_smoke_publishes_matching_avro_and_grafana_json(mode, contamination, monkeypatch):
    """Run the full consumer loop with real forests and an in-memory transport."""
    from runtime.detectors.iforest import iforest_analysis as runtime

    rows = np.random.default_rng(3).normal(size=(35, 2))
    rows[::9] += 7
    messages = deque([
        SimpleNamespace(
            error=lambda: None, key=lambda: "A", timestamp=lambda: (1, 1000),
            value=lambda row=row: {"Group": "A", "x": float(row[0]), "y": float(row[1])},
        ) for row in rows
    ])

    class Consumer:
        committed = 0

        def subscribe(self, _topics):
            pass

        def poll(self, _timeout):
            if not messages:
                raise KeyboardInterrupt
            return messages.popleft()

        def commit(self, **_kwargs):
            self.committed += 1

        def close(self):
            pass

    class Producer:
        def __init__(self):
            self.values = []
            self.pending = deque()

        def produce(self, **kwargs):
            self.values.append(kwargs["value"])
            self.pending.append(kwargs["on_delivery"])

        def poll(self, _timeout):
            while self.pending:
                self.pending.popleft()(None, None)

        def flush(self, _timeout):
            pass

    consumer, avro, viz = Consumer(), Producer(), Producer()
    schema = json.dumps({"type": "record", "name": "NativeSmoke", "fields": []})
    registry = SimpleNamespace(get_latest_version=lambda _subject: SimpleNamespace(
        schema=SimpleNamespace(schema_str=schema),
    ))
    for name, replacement in (
        ("make_sr_client", lambda: registry), ("AvroDeserializer", lambda **_kw: None),
        ("AvroSerializer", lambda **_kw: None), ("make_consumer", lambda *_args: consumer),
        ("make_producer", lambda *_args: avro), ("make_json_producer", lambda: viz),
    ):
        monkeypatch.setattr(runtime, name, replacement)
    monkeypatch.setattr(runtime.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(runtime.sys, "argv", ["iforest_analysis.py"])
    for name, value in {
        "DASMA_ENABLED": "1", "ANALYSE_SEED": "42", "IFOREST_THRESHOLD_MODE": mode,
        "IFOREST_CONTAMINATION": contamination, "IFOREST_INITIAL_WINDOW_SIZE": "16",
        "IFOREST_WINDOW_SIZE": "32", "IFOREST_MAX_SAMPLES": "8", "IFOREST_N_ESTIMATORS": "11",
        "IFOREST_REFIT_EVERY": "3", "IFOREST_ANOMALY_WARMUP_WINDOW": "2",
        "IFOREST_EXCLUDE_FIELDS": "Group", "IFOREST_THRESHOLD_WINDOW": "10",
    }.items():
        monkeypatch.setenv(name, value)
    runtime.main()

    assert consumer.committed == len(rows)
    assert avro.values == [json.loads(value) for value in viz.values]
    reference = make_model(contamination)
    for index, (row, record) in enumerate(zip(rows, avro.values)):
        score = reference.partial_fit_score(row)
        assert record["iforest_score"] == score
        if index < 16 or (mode == "p99" and index < 18):
            assert record["iforest_status"] == "warming_up"
            assert record["iforest_anomaly"] is None
            assert record["iforest_threshold"] is None
        elif mode == "native":
            assert record["iforest_threshold"] == reference.last_scoring_threshold
            assert record["iforest_anomaly"] == bool(reference.last_scoring_model.predict([row])[0] == -1)
        else:
            assert record["iforest_threshold"] is not None
            assert record["iforest_anomaly"] == (score > record["iforest_threshold"])
