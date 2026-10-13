"""Unit tests for the reproducibility CSV exporter (P2-3).

Fake Kafka module, controllable clock and temporary output roots only: no
broker, no Docker, no real `data/` writes. The fake consumer advances the
shared clock only when its message queue is empty, so every timeout path
terminates deterministically.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
import tempfile
import types
from collections import deque
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from validation.reproducibility import export_topic_to_csv as exporter
from validation.reproducibility.csv_contract import MODEL_COLUMNS, canonical_key

PARTITION_EOF = -191


class FakeKafkaError:
    _PARTITION_EOF = PARTITION_EOF

    def __init__(self, code=None, name="fake-error"):
        self._code = code
        self._name = name

    def code(self):
        return self._code

    def __str__(self):
        return f"FakeKafkaError({self._name})"


class FakeMessage:
    def __init__(self, value=None, error=None, offset=0):
        self._value = value
        self._error = error
        self._offset = offset

    def error(self):
        return self._error

    def value(self):
        return self._value

    def offset(self):
        return self._offset


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeConsumer:
    instances: list["FakeConsumer"] = []
    clock: FakeClock | None = None  # class attribute, set by the test base

    def __init__(self, conf):
        self.conf = conf
        self.subscribed: list[str] = []
        self.committed = []
        self.closed = False
        self.messages: deque = deque()
        type(self).instances.append(self)

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def poll(self, timeout):
        if self.messages:
            return self.messages.popleft()
        clock = type(self).clock
        if clock is not None:
            clock.advance(timeout)
        return None

    def commit(self, message=None, asynchronous=False):
        self.committed.append(message)

    def close(self):
        self.closed = True


def _fake_kafka_module(consumer_cls=FakeConsumer):
    module = types.ModuleType("confluent_kafka")
    module.Consumer = consumer_cls
    module.KafkaError = FakeKafkaError
    return module


def _viz_payload(model: str, group: str = "G1", event: str = "event-1", **overrides):
    record = {
        "source_event_id": event,
        "timestamp": "2026-09-15 10:00:00",
        "Group": group,
        f"{model}_score": 0.0123,
        f"{model}_status": "normal",
        f"{model}_threshold": 1.5,
        f"{model}_anomaly": False,
        "mdragpoint_internal_r": 0.75,
    }
    record.update(overrides)
    return record


def _paths(tmp_path: Path, model: str = "hstree", suffix: str = "1"):
    campaign = tmp_path / "out" / "campaign-42"
    return {
        "csv": campaign / f"{model}-source-{suffix}.csv",
        "metadata": campaign / f"{model}-source-{suffix}.metadata.json",
        "staging": campaign / f"{model}-source-{suffix}.staging.sqlite3",
    }


class ExporterTestBase(TestCase):
    def setUp(self) -> None:
        FakeConsumer.instances = []
        self.clock = FakeClock()
        FakeConsumer.clock = self.clock
        self.kafka = _fake_kafka_module()

    def run_main(
        self,
        tmp_path: Path,
        messages=(),
        model: str = "hstree",
        consumer_cls=None,
        **overrides,
    ):
        """Run main() with queued messages and the fake clock; return the consumer."""
        if consumer_cls is None:
            message_queue = deque(messages)

            class QueuedConsumer(FakeConsumer):
                def __init__(self, conf):
                    super().__init__(conf)
                    self.messages = message_queue

            consumer_cls = QueuedConsumer
        env = {
            "REPRODUCIBILITY_MODEL_ID": model,
            "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
            "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
            "REPRODUCIBILITY_OUTPUT_ROOT": str(tmp_path / "out"),
            "REPRODUCIBILITY_IDLE_EXIT_SEC": "1",
            "REPRODUCIBILITY_STARTUP_TIMEOUT_SEC": "1",
        }
        env.update(overrides)
        with patch.dict(sys.modules, {"confluent_kafka": _fake_kafka_module(consumer_cls)}), patch.dict(
            os.environ, env
        ), patch.object(exporter, "_monotonic", self.clock):
            exporter.main()
        return FakeConsumer.instances[-1]

    def assert_no_final(self, tmp_path: Path, model: str = "hstree") -> None:
        paths = _paths(tmp_path, model=model)
        self.assertFalse(paths["csv"].exists())
        self.assertFalse(paths["metadata"].exists())


class ConfigTest(TestCase):
    """Family 1: defaults, per-model topics, overrides, validation errors."""

    def test_defaults_for_every_model_topic(self) -> None:
        expected_topics = {
            "hstree": "hstree-analysis-viz",
            "iforest": "iforest-analysis-viz",
            "kitnet": "kitnet-analysis-viz",
            "deepant": "deepant-analysis-viz",
            "mdragpoint": "mdragstream-point-analysis-viz",
        }
        for model, topic in expected_topics.items():
            config = exporter.load_config({
                "REPRODUCIBILITY_MODEL_ID": model,
                "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
                "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
            })
            self.assertEqual(config.source_topic, topic, model)
            self.assertEqual(config.bootstrap_servers, "kafka1:9092,kafka2:9092,kafka3:9092")
            self.assertEqual(config.group_id, "reproducibility-export-campaign-42-source-1", model)
            self.assertEqual(config.output_root, "data/reproducibility")
            self.assertEqual(config.idle_exit_sec, 15)
            self.assertEqual(config.startup_timeout_sec, 300)
            self.assertEqual(config.image, "")
            self.assertEqual(config.campaign_parameters, {})
            self.assertEqual(config.csv_path.name, f"{model}-source-1.csv")
            self.assertEqual(config.metadata_path.name, f"{model}-source-1.metadata.json")

    def test_overrides_are_honored(self) -> None:
        config = exporter.load_config({
            "REPRODUCIBILITY_MODEL_ID": "iforest",
            "REPRODUCIBILITY_SOURCE_ROLE": "source-2",
            "REPRODUCIBILITY_CAMPAIGN_ID": "essai-cta",
            "REPRODUCIBILITY_SOURCE_TOPIC": "custom-topic",
            "REPRODUCIBILITY_GROUP_ID": "custom-group",
            "REPRODUCIBILITY_OUTPUT_ROOT": "/tmp/out",
            "REPRODUCIBILITY_IDLE_EXIT_SEC": "7",
            "REPRODUCIBILITY_STARTUP_TIMEOUT_SEC": "42",
            "REPRODUCIBILITY_IMAGE": "dasma-kafka-anomaly-sklearn:0.1.0",
            "REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": '{"num_trees": 75}',
        })
        self.assertEqual(config.source_topic, "custom-topic")
        self.assertEqual(config.group_id, "custom-group")
        self.assertEqual(config.output_root, "/tmp/out")
        self.assertEqual(config.idle_exit_sec, 7)
        self.assertEqual(config.startup_timeout_sec, 42)
        self.assertEqual(config.image, "dasma-kafka-anomaly-sklearn:0.1.0")
        self.assertEqual(config.campaign_parameters, {"num_trees": 75})
        self.assertEqual(config.csv_path.name, "iforest-source-2.csv")
        self.assertEqual(config.metadata_path.name, "iforest-source-2.metadata.json")

    def test_missing_required_values_rejected(self) -> None:
        base = {
            "REPRODUCIBILITY_MODEL_ID": "hstree",
            "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
            "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
        }
        for missing in (
            "REPRODUCIBILITY_MODEL_ID",
            "REPRODUCIBILITY_SOURCE_ROLE",
            "REPRODUCIBILITY_CAMPAIGN_ID",
        ):
            env = dict(base)
            env.pop(missing)
            with self.assertRaises(ValueError, msg=missing):
                exporter.load_config(env)

    def test_invalid_values_rejected(self) -> None:
        cases = [
            ({"REPRODUCIBILITY_MODEL_ID": "explainability"}, "model"),
            ({"REPRODUCIBILITY_MODEL_ID": "unknown"}, "model"),
            ({"REPRODUCIBILITY_SOURCE_ROLE": "source-3"}, "source role"),
            ({"REPRODUCIBILITY_SOURCE_ROLE": ""}, "source role"),
            ({"REPRODUCIBILITY_CAMPAIGN_ID": "../escape"}, "campaign"),
            ({"REPRODUCIBILITY_CAMPAIGN_ID": ""}, "campaign"),
            ({"REPRODUCIBILITY_IDLE_EXIT_SEC": "0"}, "IDLE_EXIT"),
            ({"REPRODUCIBILITY_IDLE_EXIT_SEC": "-1"}, "IDLE_EXIT"),
            ({"REPRODUCIBILITY_IDLE_EXIT_SEC": "abc"}, "IDLE_EXIT"),
            ({"REPRODUCIBILITY_IDLE_EXIT_SEC": "1.5"}, "IDLE_EXIT"),
            ({"REPRODUCIBILITY_STARTUP_TIMEOUT_SEC": "0"}, "STARTUP_TIMEOUT"),
            ({"REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": "["}, "JSON"),
            ({"REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": "[]"}, "object"),
            ({"REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": "42"}, "object"),
            ({"REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": '{"a": NaN}'}, "JSON"),
            ({"REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON": '{"a": Infinity}'}, "JSON"),
        ]
        for overrides, needle in cases:
            env = {
                "REPRODUCIBILITY_MODEL_ID": "hstree",
                "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
                "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
            }
            env.update(overrides)
            with self.assertRaises(ValueError, msg=overrides) as ctx:
                exporter.load_config(env)
            self.assertIn(needle, str(ctx.exception), overrides)

    def test_accepted_models_match_csv_contract(self) -> None:
        from validation.reproducibility.csv_contract import MODEL_COLUMNS

        self.assertEqual(set(exporter.MODEL_VIZ_TOPICS), set(MODEL_COLUMNS))


class ConsumerConfigTest(TestCase):
    """Family 2: exact consumer dictionary."""

    def test_consumer_config_exact(self) -> None:
        config = exporter.load_config({
            "REPRODUCIBILITY_MODEL_ID": "hstree",
            "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
            "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
        })
        self.assertEqual(
            exporter.build_consumer_config(config),
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
            },
        )


class SqliteStagingTest(TestCase):
    """The durable staging store must use a journal mode usable on Docker Desktop."""

    def test_staging_uses_wal_with_full_synchronous_durability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            path = Path(tmp_str) / "campaign" / "staging.sqlite3"
            path.parent.mkdir()
            conn = exporter.init_staging(path)
            self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(conn.execute("PRAGMA synchronous").fetchone()[0], 2)
            exporter.stage_record(conn, ("G1", "event-1"), '{"score":"1.0"}')
            conn.close()
            exporter.remove_staging(path)
            for suffix in ("", "-journal", "-wal", "-shm"):
                self.assertFalse(Path(f"{path}{suffix}").exists())


class ExportPipelineTest(ExporterTestBase):
    """Families 3+6: pipeline, canonical order, metadata, finalization."""

    def test_hstree_pipeline_writes_canonical_csv_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            consumer = self.run_main(
                tmp,
                [
                    FakeMessage(
                        value=json.dumps(_viz_payload("hstree", group="G1", event="event-2")).encode()
                    ),
                    FakeMessage(
                        value=json.dumps(_viz_payload("hstree", group="G1", event="event-10")).encode()
                    ),
                    FakeMessage(
                        value=json.dumps(_viz_payload("hstree", group="A2", event="event-1")).encode()
                    ),
                    FakeMessage(error=FakeKafkaError(code=PARTITION_EOF)),
                ],
            )
            paths = _paths(tmp)
            self.assertTrue(paths["csv"].exists())
            self.assertTrue(paths["metadata"].exists())

            self.assertEqual(consumer.subscribed, ["hstree-analysis-viz"])
            self.assertEqual(len(consumer.committed), 3)

            with open(paths["csv"], encoding="utf-8", newline="") as fh:
                rows = list(csv.reader(fh))
            self.assertEqual(
                rows[0],
                ["source_event_id", "timestamp", "Group", "score", "status", "threshold", "anomaly"],
            )
            self.assertEqual(
                [canonical_key(dict(zip(rows[0], row))) for row in rows[1:]],
                [("A2", "event-1"), ("G1", "event-10"), ("G1", "event-2")],
            )

            metadata = json.loads(paths["metadata"].read_text(encoding="utf-8"))
            self.assertEqual(metadata["format_version"], 1)
            self.assertEqual(metadata["model"], "hstree")
            self.assertEqual(metadata["source_role"], "source-1")
            self.assertEqual(metadata["campaign_id"], "campaign-42")
            self.assertEqual(metadata["columns"], list(MODEL_COLUMNS["hstree"]))
            self.assertEqual(metadata["campaign_parameters"], {})
            self.assertEqual(metadata["image"], "")

    def test_mdragpoint_pipeline_includes_internal_r_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            self.run_main(
                tmp,
                [
                    FakeMessage(value=json.dumps(_viz_payload("mdragpoint", event="event-1")).encode()),
                    FakeMessage(
                        value=json.dumps(
                            _viz_payload("mdragpoint", event="event-2", mdragpoint_internal_r=None)
                        ).encode()
                    ),
                ],
                model="mdragpoint",
            )
            paths = _paths(tmp, model="mdragpoint")
            with open(paths["csv"], encoding="utf-8", newline="") as fh:
                rows = list(csv.reader(fh))
            self.assertEqual(rows[0], list(MODEL_COLUMNS["mdragpoint"]))
            self.assertEqual(rows[1][-1], "0.75")
            self.assertEqual(rows[2][-1], "")
            metadata = json.loads(paths["metadata"].read_text(encoding="utf-8"))
            self.assertEqual(metadata["columns"], list(MODEL_COLUMNS["mdragpoint"]))

    def test_metadata_published_last(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            replace_calls = []
            real_replace = Path.replace

            def recording_replace(path, target):
                replace_calls.append(Path(target).name)
                return real_replace(path, target)

            with patch.object(Path, "replace", recording_replace):
                self.run_main(tmp, [FakeMessage(value=json.dumps(_viz_payload("hstree")).encode())])
            self.assertEqual(len(replace_calls), 2)
            self.assertEqual(replace_calls[0], "hstree-source-1.csv")
            self.assertEqual(replace_calls[1], "hstree-source-1.metadata.json")


class ExportErrorHandlingTest(ExporterTestBase):
    """Families 4+5: errors without loss, deduplication."""

    def test_malformed_json_is_fatal_without_final_or_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            with self.assertRaises(RuntimeError) as ctx:
                self.run_main(tmp, [FakeMessage(value=b"{not json")])
            self.assertIn("malformed JSON", str(ctx.exception))
            self.assert_no_final(tmp)
            self.assertEqual(FakeConsumer.instances[-1].committed, [])
            self.assertFalse(_paths(tmp)["staging"].exists())

    def test_contract_error_keeps_faulty_offset_uncommitted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            record = _viz_payload("hstree", event="event-1")
            del record["hstree_score"]
            with self.assertRaises(ValueError) as ctx:
                self.run_main(tmp, [FakeMessage(value=json.dumps(record).encode())])
            self.assertIn("hstree_score", str(ctx.exception))
            self.assert_no_final(tmp)
            self.assertEqual(FakeConsumer.instances[-1].committed, [])

    def test_missing_mdragpoint_extra_is_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            record = _viz_payload("mdragpoint")
            del record["mdragpoint_internal_r"]
            with self.assertRaises(ValueError) as ctx:
                self.run_main(
                    tmp, [FakeMessage(value=json.dumps(record).encode())], model="mdragpoint"
                )
            self.assertIn("mdragpoint_internal_r", str(ctx.exception))
            self.assert_no_final(tmp, model="mdragpoint")

    def test_kafka_error_message_is_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)

            class ErroringConsumer(FakeConsumer):
                def poll(self, timeout):
                    type(self).clock.advance(timeout)
                    return FakeMessage(error=FakeKafkaError(code=1, name="boom"))

            with self.assertRaises(RuntimeError) as ctx:
                self.run_main(tmp, consumer_cls=ErroringConsumer)
            self.assertIn("kafka error", str(ctx.exception))
            self.assertIn("boom", str(ctx.exception))
            self.assert_no_final(tmp)
            self.assertEqual(FakeConsumer.instances[-1].committed, [])

    def test_sqlite_storage_error_is_fatal_without_final(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            message = FakeMessage(value=json.dumps(_viz_payload("hstree")).encode())

            def failing_stage(conn, key, row_json):
                raise sqlite3.OperationalError("disk full")

            with patch.object(exporter, "stage_record", failing_stage):
                with self.assertRaises(sqlite3.OperationalError):
                    self.run_main(tmp, [message])
            self.assert_no_final(tmp)
            self.assertEqual(FakeConsumer.instances[-1].committed, [])

    def test_identical_redelivery_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            payload = json.dumps(_viz_payload("hstree", event="event-1")).encode()
            consumer = self.run_main(
                tmp,
                [
                    FakeMessage(value=payload, offset=0),
                    FakeMessage(value=payload, offset=1),
                ],
            )
            paths = _paths(tmp)
            with open(paths["csv"], encoding="utf-8", newline="") as fh:
                rows = list(csv.reader(fh))
            self.assertEqual(len(rows), 2)  # header + one data row
            self.assertEqual(len(consumer.committed), 2)

    def test_conflicting_row_for_same_key_is_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            with self.assertRaises(ValueError) as ctx:
                self.run_main(
                    tmp,
                    [
                        FakeMessage(
                            value=json.dumps(
                                _viz_payload("hstree", event="event-1", hstree_score=0.1)
                            ).encode()
                        ),
                        FakeMessage(
                            value=json.dumps(
                                _viz_payload("hstree", event="event-1", hstree_score=0.2)
                            ).encode()
                        ),
                    ],
                )
            self.assertIn("conflicting", str(ctx.exception))
            self.assert_no_final(tmp)


class FinalizationTest(ExporterTestBase):
    """Family 6: startup timeout, idle exit, existing exports."""

    def test_startup_timeout_fails_without_final(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            env = {
                "REPRODUCIBILITY_MODEL_ID": "hstree",
                "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
                "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
                "REPRODUCIBILITY_OUTPUT_ROOT": str(tmp / "out"),
                "REPRODUCIBILITY_IDLE_EXIT_SEC": "1",
                "REPRODUCIBILITY_STARTUP_TIMEOUT_SEC": "1",
            }
            with patch.dict(sys.modules, {"confluent_kafka": self.kafka}), patch.dict(
                os.environ, env
            ), patch.object(exporter, "_monotonic", self.clock):
                with self.assertRaises(RuntimeError) as ctx:
                    exporter.main()
            self.assertIn("no valid record", str(ctx.exception))
            paths = _paths(tmp)
            self.assertFalse(paths["csv"].exists())
            self.assertFalse(paths["metadata"].exists())
            self.assertFalse(paths["staging"].exists())

    def test_idle_exit_after_first_valid_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)

            class OneMessageConsumer(FakeConsumer):
                def __init__(self, conf):
                    super().__init__(conf)
                    self.messages.append(
                        FakeMessage(value=json.dumps(_viz_payload("hstree")).encode())
                    )

            self.run_main(tmp, consumer_cls=OneMessageConsumer)
            paths = _paths(tmp)
            self.assertTrue(paths["csv"].exists())
            self.assertTrue(paths["metadata"].exists())
            self.assertTrue(FakeConsumer.instances[-1].closed)

    def test_existing_exports_are_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            paths = _paths(tmp)
            paths["csv"].parent.mkdir(parents=True, exist_ok=True)
            paths["csv"].write_text("old export\n", encoding="utf-8")
            env = {
                "REPRODUCIBILITY_MODEL_ID": "hstree",
                "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
                "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
                "REPRODUCIBILITY_OUTPUT_ROOT": str(tmp / "out"),
            }
            with patch.dict(sys.modules, {"confluent_kafka": self.kafka}), patch.dict(
                os.environ, env
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    exporter.main()
            self.assertIn("refusing to overwrite", str(ctx.exception))
            self.assertEqual(paths["csv"].read_text(encoding="utf-8"), "old export\n")
            self.assertEqual(FakeConsumer.instances, [])

    def test_metadata_only_export_refused_too(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            paths = _paths(tmp)
            paths["metadata"].parent.mkdir(parents=True, exist_ok=True)
            paths["metadata"].write_text("{}\n", encoding="utf-8")
            env = {
                "REPRODUCIBILITY_MODEL_ID": "hstree",
                "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
                "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
                "REPRODUCIBILITY_OUTPUT_ROOT": str(tmp / "out"),
            }
            with patch.dict(sys.modules, {"confluent_kafka": self.kafka}), patch.dict(
                os.environ, env
            ):
                with self.assertRaises(RuntimeError):
                    exporter.main()
            self.assertEqual(FakeConsumer.instances, [])


class DisabledModeTest(ExporterTestBase):
    """Family 7: DASMA_ENABLED=0 idles before any Kafka/SQLite/file work."""

    def test_disabled_calls_signal_pause_without_kafka_or_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            created: list[str] = []

            class ExplodingConsumer(FakeConsumer):
                def __init__(self, conf):
                    created.append("consumer")
                    raise AssertionError("consumer must not be constructed")

            env = {
                "DASMA_ENABLED": "0",
                "REPRODUCIBILITY_MODEL_ID": "hstree",
                "REPRODUCIBILITY_SOURCE_ROLE": "source-1",
                "REPRODUCIBILITY_CAMPAIGN_ID": "campaign-42",
                "REPRODUCIBILITY_OUTPUT_ROOT": str(tmp / "out"),
            }
            with patch.dict(
                sys.modules, {"confluent_kafka": _fake_kafka_module(ExplodingConsumer)}
            ), patch.dict(os.environ, env), patch.object(
                exporter.signal, "pause", side_effect=KeyboardInterrupt
            ) as pause:
                with self.assertRaises(KeyboardInterrupt):
                    exporter.main()
            pause.assert_called_once()
            self.assertEqual(created, [])
            self.assertFalse((tmp / "out").exists())
