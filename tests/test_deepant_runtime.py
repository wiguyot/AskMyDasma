"""Unit tests for the DeepAnT worker runtime (torch-free parts)."""

from __future__ import annotations

import os
import unittest

from src.dasma_kafka.deepant_analysis.runtime import (
    DeepAntAck,
    DeepAntWorkerConfig,
    DeepAntWorkItem,
    _apply_cpu_thread_limits,
)


def _make_config(**overrides) -> DeepAntWorkerConfig:
    base = dict(
        bootstrap_servers="kafka1:9092",
        schema_registry_url="http://schema-registry:8081",
        schema_registry_basic_auth_user_info=None,
        schema_registry_basic_auth_source="USER_INFO",
        dest_topic="deepant-analysis",
        dest_viz_topic="deepant-analysis-viz",
        dest_avro_schema="{}",
        seed=42,
        log_level="INFO",
        window_size=0,
        exclude_fields=["timestamp", "Group"],
        threshold_mode="percentile",
        threshold_percentile=99.0,
        threshold_window=500,
        welford_window=2048,
        anomaly_k_sigma=3.0,
        anomaly_warmup_window=100,
        exclude_anomalies_from_refit=False,
        cpu_threads=1,
        lookback_size=100,
        initial_window_size=512,
        refit_every=64,
        deepant_window_size=2048,
        filters=32,
        kernel_size=2,
        dropout=0.25,
        lr=0.001,
        epochs_initial=10,
        epochs_refit=5,
        batch_size=32,
    )
    base.update(overrides)
    return DeepAntWorkerConfig(**base)


class DeepAntWorkItemTest(unittest.TestCase):
    def test_work_item_carries_kafka_coordinates(self) -> None:
        item = DeepAntWorkItem(
            key_str="grp", record={"a": 1.0}, topic="source-data", partition=0, offset=7, timestamp_ms=999
        )
        self.assertEqual((item.topic, item.partition, item.offset), ("source-data", 0, 7))
        self.assertEqual(item.timestamp_ms, 999)

    def test_ack_reports_outcome(self) -> None:
        self.assertTrue(DeepAntAck("source-data", 0, 7, True).ok)
        self.assertFalse(DeepAntAck("source-data", 0, 7, False).ok)


class DeepAntWorkerConfigTest(unittest.TestCase):
    def test_config_is_constructible_and_holds_hyperparameters(self) -> None:
        cfg = _make_config(threshold_mode="welford-windowed", cpu_threads=2)
        self.assertEqual(cfg.threshold_mode, "welford-windowed")
        self.assertEqual(cfg.cpu_threads, 2)
        self.assertEqual(cfg.lookback_size, 100)

    def test_config_is_picklable(self) -> None:
        import pickle

        cfg = _make_config()
        restored = pickle.loads(pickle.dumps(cfg))
        self.assertEqual(restored.dest_topic, "deepant-analysis")
        self.assertEqual(restored.seed, 42)


class CpuThreadLimitsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {
            var: os.environ.get(var)
            for var in (
                "OMP_NUM_THREADS",
                "MKL_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "NUMEXPR_NUM_THREADS",
                "TORCH_NUM_THREADS",
            )
        }
        for var in self._saved:
            os.environ.pop(var, None)

    def tearDown(self) -> None:
        for var, value in self._saved.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value

    def test_sets_all_thread_vars(self) -> None:
        _apply_cpu_thread_limits(1)
        for var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "TORCH_NUM_THREADS"):
            self.assertEqual(os.environ[var], "1")

    def test_floor_of_one_thread(self) -> None:
        _apply_cpu_thread_limits(0)
        self.assertEqual(os.environ["OMP_NUM_THREADS"], "1")

    def test_does_not_override_existing(self) -> None:
        os.environ["OMP_NUM_THREADS"] = "8"
        _apply_cpu_thread_limits(2)
        # setdefault must not clobber an explicit operator setting.
        self.assertEqual(os.environ["OMP_NUM_THREADS"], "8")
        self.assertEqual(os.environ["MKL_NUM_THREADS"], "2")


if __name__ == "__main__":
    unittest.main()
