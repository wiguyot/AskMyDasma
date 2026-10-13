"""Characterize streaming IForest scores before Phase B explainability changes."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import IsolationForest

from dasma_kafka.iforest_analysis.model import IForestGlobalModel
from dasma_kafka.common.explained_models import IForestModelManager
from dasma_kafka.explainability.features import IForestFeatureExtractor as V1FeatureExtractor
from dasma_kafka.explainability_v2.features import IForestFeatureExtractor as V2FeatureExtractor
from dasma_kafka.explainability.shap_engine import SHAPExplainer as V1SHAPExplainer
from dasma_kafka.explainability_v2.shap_engine import SHAPExplainer as V2SHAPExplainer
from dasma_kafka.iforest_analysis.helpers import build_feature_vector, infer_feature_names
from dasma_kafka.producer.helpers import sanitize_avro_field_name


INITIAL_WINDOW = 4
WINDOW_SIZE = 6
REFIT_EVERY = 2
NUM_FEATURES = 2


def _rows() -> list[np.ndarray]:
    return [
        np.array([0.0, 1.0]),
        np.array([0.2, 1.5]),
        np.array([0.4, 2.0]),
        np.array([0.6, 2.5]),
        np.array([2.0, 8.0]),
        np.array([-1.0, 4.0]),
        np.array([3.0, 9.0]),
        np.array([0.8, 3.0]),
    ]


def _reference_scores(rows: list[np.ndarray]) -> tuple[list[float], int]:
    window: list[list[float]] = []
    model: IsolationForest | None = None
    since_refit = 0
    generation = 0
    scores: list[float] = []

    for row in rows:
        vector = list(row)
        score: float | None = None
        if model is not None:
            score = float(-model.score_samples(np.asarray([vector], dtype=np.float64))[0])
            since_refit += 1

        window.append(vector)
        if len(window) > WINDOW_SIZE:
            window = window[-WINDOW_SIZE:]

        if len(window) >= INITIAL_WINDOW and (model is None or since_refit >= REFIT_EVERY):
            model = IsolationForest(
                n_estimators=7,
                max_samples=2,
                contamination="auto",
                random_state=19,
                n_jobs=1,
            )
            model.fit(np.asarray(window, dtype=np.float64))
            since_refit = 0
            generation += 1

        scores.append(0.0 if score is None else score)

    return scores, generation


def test_iforest_streaming_scores_match_reference_across_refits_and_evictions() -> None:
    rows = _rows()
    expected_scores, expected_generation = _reference_scores(rows)
    model = IForestGlobalModel(
        num_features=NUM_FEATURES,
        initial_window_size=INITIAL_WINDOW,
        window_size=WINDOW_SIZE,
        random_state=19,
        n_estimators=7,
        max_samples=2,
        refit_every=REFIT_EVERY,
    )

    actual_scores = [model.partial_fit_score(row) for row in rows]

    np.testing.assert_array_equal(actual_scores, expected_scores)
    assert model.generation == expected_generation
    assert model.samples_buffered == WINDOW_SIZE
    assert model.is_ready
    assert not model.is_warming_up


def test_iforest_score_batch_is_pure_and_uses_current_model() -> None:
    model = IForestGlobalModel(
        num_features=NUM_FEATURES,
        initial_window_size=INITIAL_WINDOW,
        window_size=WINDOW_SIZE,
        random_state=19,
        n_estimators=7,
        max_samples=2,
    )
    for row in _rows()[:INITIAL_WINDOW]:
        model.partial_fit_score(row)

    batch = np.asarray([[0.1, 1.2], [5.0, 10.0]], dtype=np.float64)
    samples_before = model.samples_buffered
    generation_before = model.generation

    np.testing.assert_array_equal(model.score_batch(batch), -model.current_model.score_samples(batch))
    assert model.samples_buffered == samples_before
    assert model.generation == generation_before


def _iforest_config() -> SimpleNamespace:
    return SimpleNamespace(
        iforest_initial_window_size=INITIAL_WINDOW,
        iforest_window_size=WINDOW_SIZE,
        iforest_n_estimators=7,
        iforest_max_samples="2",
        iforest_refit_every=REFIT_EVERY,
        hstree_seed=19,
        shap_background_size=4,
    )


def test_iforest_manager_mirrors_streaming_model_and_returns_none_during_warmup() -> None:
    manager = IForestModelManager(_iforest_config(), logging.getLogger("iforest-test"))
    reference = IForestGlobalModel(
        num_features=NUM_FEATURES,
        initial_window_size=INITIAL_WINDOW,
        window_size=WINDOW_SIZE,
        random_state=19,
        n_estimators=7,
        max_samples=2,
        refit_every=REFIT_EVERY,
    )

    actual = [manager.fit_score("A", row) for row in _rows()]
    expected = [reference.partial_fit_score(row) for row in _rows()]
    assert actual[: INITIAL_WINDOW - 1] == [None] * (INITIAL_WINDOW - 1)
    expected_after_warmup = expected[INITIAL_WINDOW - 1 :]
    expected_after_warmup[0] = None  # the event which completes warmup still has no scorer
    np.testing.assert_array_equal(actual[INITIAL_WINDOW - 1 :], expected_after_warmup)
    assert manager.get_background_data("A") is not None
    np.testing.assert_array_equal(
        manager.predict_fn("A")(np.asarray([[0.1, 1.2], [4.0, 10.0]])),
        reference.score_batch(np.asarray([[0.1, 1.2], [4.0, 10.0]])),
    )


def test_tree_conversion_cache_tracks_only_current_scoring_generation() -> None:
    manager = IForestModelManager(_iforest_config(), logging.getLogger("iforest-tree-cache"))
    observed: dict[int, object] = {}
    for row in _rows():
        manager.fit_score("A", row)
        generation = manager.get_scoring_generation("A")
        forest = manager.get_scoring_model("A")
        if generation is None or forest is None:
            continue
        converted = manager.get_tree_model("A")
        assert manager.get_tree_model("A") is converted
        if generation in observed:
            assert observed[generation] is converted
        else:
            observed[generation] = converted
    assert len(observed) >= 2
    assert len(manager.tree_cache) == 1
    assert manager.tree_cache["A"][0] == manager.get_scoring_generation("A")


@pytest.mark.parametrize("explainer_type", [V1SHAPExplainer, V2SHAPExplainer])
def test_both_methods_explain_pre_refit_scoring_forest_on_trigger_event(explainer_type) -> None:
    """The event that installs B is explained by A in both Kernel and Tree paths."""
    config = _iforest_config()
    config.shap_nsamples = 16
    manager = IForestModelManager(config, logging.getLogger("iforest-event-tree-contract"))
    explainer = explainer_type(SimpleNamespace(shap_nsamples=16), logging.getLogger("iforest-event-tree"))
    explainer.set_feature_names(["a", "b"])

    for event_index, row in enumerate(_rows(), start=1):
        previous = manager.get_scoring_model("A")
        score = manager.fit_score("A", row)
        if event_index != INITIAL_WINDOW + REFIT_EVERY:
            continue

        # This event scored on generation 1, then caused generation 2 to be fitted.
        assert previous is not None
        assert score is not None
        assert manager.models["A"].current_model is not previous
        assert manager.get_scoring_model("A") is previous
        assert manager.get_scoring_generation("A") == 1
        background = manager.get_background_data("A")
        assert background is not None

        kernel = explainer.compute_shap_values(
            predict_fn=manager.predict_fn_for_scoring_event("A"),
            features=row,
            background_data=background,
        )
        tree = explainer.compute_tree_values(manager, "A", row, background)
        assert kernel["shap_values"] is not None
        assert tree["shap_values"] is not None
        assert sum(kernel["shap_values"].values()) + kernel["shap_base_value"] == pytest.approx(
            score, abs=1e-8
        )
        assert sum(tree["shap_values"].values()) + tree["shap_base_value"] == pytest.approx(
            tree["explained_value"], abs=1e-6
        )
        converted = manager.get_tree_model("A")
        assert manager.get_tree_model("A") is converted
        assert manager.tree_cache["A"][0] == manager.get_scoring_generation("A") == 1


def test_iforest_extractors_delegate_numeric_and_exclusion_semantics() -> None:
    record = {
        "timestamp": "2026-01-01T00:00:00Z",
        "Group": "A",
        "a": 1,
        "b": True,
        "c": "not-numeric",
        "iforest_score": 0.4,
        "iforest_anomaly": False,
        "_offset": 12,
    }
    for extractor_type in (V1FeatureExtractor, V2FeatureExtractor):
        extractor = extractor_type(logging.getLogger("iforest-feature-test"))
        extractor.initialize_from_record(record)
        assert extractor.get_feature_names() == ["a", "b"]
        np.testing.assert_array_equal(extractor.extract(record), [1.0, 1.0])


METADATA_NAMES = (
    ("State (0=OK 1=Warning 2=Error)", "BackgroundSubtracted(0/1)"),
    ("State__0_OK_1_Warning_2_Error_", "BackgroundSubtracted_0_1_"),
)


@pytest.mark.parametrize("metadata_names", [*METADATA_NAMES, sum(METADATA_NAMES, ())])
@pytest.mark.parametrize("extractor_type, explainer_type", [
    (V1FeatureExtractor, V1SHAPExplainer),
    (V2FeatureExtractor, V2SHAPExplainer),
])
def test_metadata_changes_leave_vectors_scores_background_and_shap_unchanged(
    metadata_names, extractor_type, explainer_type,
) -> None:
    logger = logging.getLogger("iforest-metadata-test")
    config = _iforest_config()
    config.shap_nsamples = 8
    extractors = [extractor_type(logger), extractor_type(logger)]
    managers = [IForestModelManager(config, logger), IForestModelManager(config, logger)]
    models = {
        group: IForestGlobalModel(
            NUM_FEATURES, INITIAL_WINDOW, WINDOW_SIZE, 19, 7, max_samples=2,
            refit_every=REFIT_EVERY,
        )
        for group in ("A", "B")
    }
    last_vectors = {}
    last_scores = {}
    for i, row in enumerate(_rows()):
        for group in models:
            base = {"Group": group, "a": float(row[0]), "b": float(row[1])}
            records = [
                base | dict.fromkeys(metadata_names, 0),
                base | {name: (i + j) % 3 for j, name in enumerate(metadata_names)},
            ]
            names = infer_feature_names(records[0], set())
            assert names == infer_feature_names(records[1], set()) == ["a", "b"]
            vector = build_feature_vector(base, names)
            upstream = models[group].partial_fit_score(vector)
            for extractor, manager, record in zip(extractors, managers, records):
                if not extractor.is_initialized:
                    # An empty custom exclusion list must still enforce the business rule.
                    extractor.excluded_columns = set()
                    extractor.initialize_from_record(record)
                assert extractor.get_feature_names() == names
                actual_vector = extractor.extract(record)
                np.testing.assert_array_equal(actual_vector, vector)
                score = manager.fit_score(group, actual_vector)
                assert score == (None if models[group].is_warming_up else upstream)
            last_vectors[group] = np.asarray(vector)
            last_scores[group] = upstream

    for group, vector in last_vectors.items():
        backgrounds = [manager.get_background_data(group) for manager in managers]
        np.testing.assert_array_equal(backgrounds[0], backgrounds[1])
        assert backgrounds[0].shape == (config.shap_background_size, NUM_FEATURES)
        explanations = []
        for extractor, manager, background in zip(extractors, managers, backgrounds):
            explainer = explainer_type(config, logger)
            explainer.set_feature_names(extractor.get_feature_names())
            explanation = explainer.compute_shap_values(manager.predict_fn(group), vector, background)
            assert explanation["shap_values"] is not None, explanation
            assert set(explanation["shap_values"]) == {"a", "b"}
            reconstructed = explanation["shap_base_value"] + sum(explanation["shap_values"].values())
            np.testing.assert_allclose(reconstructed, last_scores[group], atol=1e-12, rtol=0)
            explanations.append(explanation)
        assert explanations[0]["shap_values"] == explanations[1]["shap_values"]
        assert explanations[0]["shap_base_value"] == explanations[1]["shap_base_value"]


def test_source_5000_has_79_features_in_detector_and_both_mirrors() -> None:
    source = Path(__file__).resolve().parents[1] / "data" / "source-5000.csv"
    frame = pd.read_csv(source, sep=None, engine="python")
    record = frame.iloc[0].to_dict()
    excluded = set("source_event_id,timestamp,Group,group,Cycle,LabviewTime,time,StepNumber,"
                   "State (0=OK 1=Warning 2=Error),BackgroundSubtracted(0/1),ActionNumber".split(","))
    used_names = set()
    mapping = {name: sanitize_avro_field_name(name, used_names) for name in frame.columns}
    serialized = {mapping[name]: value for name, value in record.items()}
    csv_names = infer_feature_names(record, excluded)
    avro_names = infer_feature_names(serialized, excluded)
    assert len(csv_names) == len(avro_names) == 79
    assert sorted(mapping[name] for name in csv_names) == avro_names
    assert not set(sum(METADATA_NAMES, ())) & set(csv_names + avro_names)
    for extractor_type in (V1FeatureExtractor, V2FeatureExtractor):
        for input_record, expected in ((record, csv_names), (serialized, avro_names)):
            extractor = extractor_type(logging.getLogger("iforest-source-test"))
            # Match the campaign exclusions, retaining the extractor's output-field exclusions.
            extractor.excluded_columns = extractor.excluded_columns | excluded
            extractor.initialize_from_record(input_record)
            assert extractor.get_feature_names() == expected
            np.testing.assert_array_equal(extractor.extract(input_record), build_feature_vector(input_record, expected))
