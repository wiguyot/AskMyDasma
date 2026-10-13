"""Unit tests for the reproducibility observability publisher (P2-7).

Temporary directories only: no broker, no Docker, no real ``data/`` writes.
A fake ``confluent_kafka`` module is injected into ``sys.modules`` so the
idempotent configuration, delivery handling, publication order and the
no-summary-after-error rule are all verified. The publisher module itself must
import without any Kafka dependency (deferred import).
"""

from __future__ import annotations

import csv
import json
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from types import ModuleType
from unittest import TestCase
from unittest.mock import patch

from validation.reproducibility.compare_csv import ComparisonConfig
from validation.reproducibility.compare_csv import run as compare_run
from validation.reproducibility.csv_contract import columns_for_model

import validation.reproducibility.publish_comparison_to_kafka as publish_module
from validation.reproducibility.publish_comparison_to_kafka import (
    PublishError,
    build_args,
    build_diff_event,
    build_summary_event,
    publish,
)


class FakeProducer:
    """Fake confluent_kafka.Producer recording everything it is asked."""

    instances: list["FakeProducer"] = []

    def __init__(self, config: dict):
        self.config = config
        self.produced: list[tuple[str, str, str]] = []
        self.flush_calls = 0
        self.fail_delivery_on: str | None = None
        FakeProducer.instances.append(self)

    def produce(self, topic, key=None, value=None, on_delivery=None):
        def as_text(item):
            return item.decode("utf-8") if isinstance(item, bytes) else item

        self.produced.append((topic, as_text(key), as_text(value)))
        if self.fail_delivery_on == topic and on_delivery is not None:
            on_delivery("simulated delivery error", None)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flush_calls += 1
        self.last_flush_timeout = timeout
        if getattr(self, "flush_returns_pending", False):
            return 1  # messages still in the local queue after the timeout
        return 0


# ---------------------------------------------------------------------------
# Fixtures: real reports produced by the P2-5 comparator
# ---------------------------------------------------------------------------


def _metadata(model: str, role: str, image: str) -> dict:
    return {
        "format_version": 1,
        "model": model,
        "source_role": role,
        "campaign_id": "campaign-42",
        "columns": list(columns_for_model(model)),
        "campaign_parameters": {"hstree_num_trees": 50},
        "image": image,
    }


def _row(model: str, group: str = "A", event: str = "e1", **overrides) -> dict[str, str]:
    """One viz-topic source row (the CSV v1 input of the comparator)."""
    row = {
        "source_event_id": event,
        "timestamp": "2026-09-15 10:00:00",
        "Group": group,
        "score": "0.1",
        "status": "normal",
        "threshold": "1.0",
        "anomaly": "false",
    }
    if model == "mdragpoint":
        row["mdragpoint_internal_r"] = "0.5"
    row.update(overrides)
    return row


def _write_source(directory: Path, model: str, suffix: str, rows: list[dict[str, str]], image: str) -> Path:
    csv_path = directory / f"{model}-source-{suffix}.csv"
    columns = list(columns_for_model(model))
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    role = f"source-{suffix}"
    (directory / f"{model}-source-{suffix}.metadata.json").write_text(
        json.dumps(_metadata(model, role, image)), encoding="utf-8"
    )
    return csv_path


def _produce_comparison(tmp: Path, model: str, rows_1: list[dict], rows_2: list[dict]) -> tuple[Path, Path]:
    directory = tmp / "campaign-42"
    directory.mkdir(parents=True, exist_ok=True)
    path_1 = _write_source(directory, model, "1", rows_1, "image-a:1")
    path_2 = _write_source(directory, model, "2", rows_2, "image-b:1")
    report = directory / f"{model}-comparison.csv"
    config = ComparisonConfig(
        source_1=path_1,
        source_2=path_2,
        report=report,
        summary=Path(str(report) + ".summary.json"),
        abs_tolerance=0.0,
        relative_tolerance=0.0,
    )
    compare_run(config)
    return report, Path(str(report) + ".summary.json")


def _make_factory(recorder: list, fail_on: str | None = None):
    def factory(config):
        producer = FakeProducer(config)
        if fail_on is not None:
            producer.fail_delivery_on = fail_on
        recorder.append(producer)
        return producer

    return factory


_TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


class CliAndDefaultsTest(TestCase):
    """Family: CLI defaults and path errors."""

    def test_defaults(self) -> None:
        args = build_args(["--report", "r.csv"])
        self.assertEqual(args.summary, None)
        self.assertEqual(args.bootstrap_servers, "kafka1:9092,kafka2:9092,kafka3:9092")
        self.assertEqual(args.diff_topic, "reproducibility-diff")
        self.assertEqual(args.summary_topic, "reproducibility-summary")

    def test_missing_report_exits_two(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            build_args([])
        self.assertEqual(ctx.exception.code, 2)

    def test_report_equals_summary_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            report = tmp / "r.csv"
            with self.assertRaises(PublishError):
                publish(
                    report, report,
                    bootstrap_servers="kafka1:9092",
                    diff_topic="reproducibility-diff",
                    summary_topic="reproducibility-summary",
                    producer_factory=_make_factory([]),
                )

    def test_module_imports_without_confluent_kafka(self) -> None:
        saved = sys.modules.pop("confluent_kafka", None)
        try:
            importlib = __import__("importlib")
            module = importlib.import_module(
                "validation.reproducibility.publish_comparison_to_kafka"
            )
            self.assertIsNotNone(module)
            self.assertNotIn("confluent_kafka", sys.modules)
        finally:
            if saved is not None:
                sys.modules["confluent_kafka"] = saved


class ValidationTest(TestCase):
    """Family: strict validation of the report and summary before publishing."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        # A factory that must never be reached by validation failures.
        self.guard_factory = _make_factory([])

    def _valid_pair(self, model: str) -> tuple[Path, Path]:
        row = _row(model)
        return _produce_comparison(self.tmp, model, [row], [row])

    def test_valid_pairs_hstree_and_mdragpoint(self) -> None:
        for model in ("hstree", "mdragpoint"):
            report, summary = self._valid_pair_for(model)
            produced = publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="reproducibility-diff",
                summary_topic="reproducibility-summary",
                producer_factory=_make_factory([]),
            )
            self.assertEqual(produced, 1, model)

    def _valid_pair_for(self, model: str) -> tuple[Path, Path]:
        row = _row(model)
        return _produce_comparison(self.tmp, model, [row], [row])

    def test_missing_files_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        with self.assertRaises(PublishError):
            publish(
                self.tmp / "absent.csv", summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )
        with self.assertRaises(PublishError):
            publish(
                report, self.tmp / "absent.summary.json",
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )

    def test_non_standard_summary_json_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        summary.write_text('{"format_version": 1, "x": NaN}', encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )

    def test_summary_key_mismatch_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        payload = json.loads(summary.read_text(encoding="utf-8"))
        payload["unexpected"] = 1
        summary.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )
        payload = json.loads(summary.read_text(encoding="utf-8"))
        del payload["max_abs_diff"]
        summary.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )

    def test_summary_format_version_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        payload = json.loads(summary.read_text(encoding="utf-8"))
        payload["format_version"] = 2
        summary.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )

    def test_summary_unknown_model_and_bad_campaign_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        for mutate in (
            lambda p: p.update(model="unknown"),
            lambda p: p.update(campaign_id="../escape"),
        ):
            payload = json.loads(summary.read_text(encoding="utf-8"))
            mutate(payload)
            summary.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(PublishError):
                publish(
                    report, summary,
                    bootstrap_servers="kafka1:9092",
                    diff_topic="t", summary_topic="s",
                    producer_factory=self.guard_factory,
                )

    def test_inconsistent_counters_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        payload = json.loads(summary.read_text(encoding="utf-8"))
        payload["compared"] = payload["compared"] + 5
        summary.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )

    def test_report_header_mismatch_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        lines = report.read_text(encoding="utf-8").splitlines()
        lines[0] = ",".join(columns_for_model("mdragpoint"))
        report.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )

    def test_row_count_mismatch_rejected(self) -> None:
        report, summary = self._valid_pair_for("hstree")
        lines = report.read_text(encoding="utf-8").splitlines()
        header = lines[0].split(",")
        extra = ",".join("x" for _ in header)
        report.write_text("\n".join(lines + [extra]) + "\n", encoding="utf-8")
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=self.guard_factory,
            )


class ProjectionTest(TestCase):
    """Family: flat JSON projection of every row kind."""

    def setUp(self) -> None:
        self.summary = {
            "campaign_id": "campaign-42",
            "source_1": {"csv": "/tmp/a.csv", "image": "image-a:1"},
            "source_2": {"csv": "/tmp/b.csv", "image": "image-b:1"},
            "abs_tolerance": 0.5,
            "relative_tolerance": 0.1,
            "compared": 0, "exact": 0, "within_tolerance": 0, "divergent": 0,
            "missing_source_1": 0, "missing_source_2": 0,
            "max_abs_diff": None, "max_relative_diff": None,
        }

    def _base_row(self, model: str, **overrides) -> dict[str, str]:
        row = {
            "Group": "A",
            "source_event_id": "e1",
            "comparison_status": "exact",
            "exact": "1",
            "within_tolerance": "1",
            "timestamp_equal": "0",
            "timestamp_offset_seconds": "1.0",
            "differing_fields": "",
            "max_abs_diff": "0.0",
            "max_relative_diff": "0.0",
            "timestamp_source_1": "2026-09-15 10:00:00",
            "timestamp_source_2": "2026-09-15 10:00:01",
            "status_source_1": "normal",
            "status_source_2": "normal",
            "anomaly_source_1": "false",
            "anomaly_source_2": "false",
            "score_source_1": "0.1",
            "score_source_2": "0.1",
            "score_abs_diff": "0.0",
            "score_relative_diff": "0.0",
            "threshold_source_1": "1.0",
            "threshold_source_2": "1.0",
            "threshold_abs_diff": "0.0",
            "threshold_relative_diff": "0.0",
        }
        if model == "mdragpoint":
            row.update({
                "mdragpoint_internal_r_source_1": "0.5",
                "mdragpoint_internal_r_source_2": "0.6",
                "mdragpoint_internal_r_abs_diff": "0.1",
                "mdragpoint_internal_r_relative_diff": "0.16666666666666666",
            })
        row.update(overrides)
        return row

    def test_exact_hstree_event(self) -> None:
        row = self._base_row("hstree")
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["comparison_status"], "exact")
        self.assertEqual(event["exact"], 1)
        self.assertEqual(event["within_tolerance"], 1)
        self.assertEqual(event["differing_fields"], "")
        self.assertEqual(event["timestamp_equal"], 0)  # copied from the report cell
        self.assertEqual(event["timestamp_offset_seconds"], 1.0)  # copied: source-2 - source-1
        self.assertEqual(event["score_source_1"], 0.1)
        self.assertEqual(event["anomaly_source_1"], 0)
        self.assertEqual(event["timestamp"], "2026-09-15 10:00:00")
        self.assertEqual(event["timestamp_origin"], "source-1")
        self.assertNotIn("mdragpoint_internal_r_source_1", event)

    def test_diagnostic_cells_are_copied_not_recomputed(self) -> None:
        # Equal timestamp cells but an offset cell of 5.0: the publisher must
        # copy the report value, never recompute it (0.0) nor invert its sign.
        row = self._base_row(
            "hstree",
            timestamp_equal="1",
            timestamp_offset_seconds="5.0",
            timestamp_source_1="2026-09-15 10:00:00",
            timestamp_source_2="2026-09-15 10:00:00",
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["timestamp_equal"], 1)
        self.assertEqual(event["timestamp_offset_seconds"], 5.0)

    def test_invalid_flag_cell_rejected(self) -> None:
        row = self._base_row("hstree", timestamp_equal="yes")
        with self.assertRaises(PublishError):
            build_diff_event(self.summary, row, "hstree")

    def test_mdragpoint_extra_columns(self) -> None:
        row = self._base_row("mdragpoint")
        event = build_diff_event(self.summary, row, "mdragpoint")
        self.assertEqual(event["mdragpoint_internal_r_source_1"], 0.5)
        self.assertEqual(event["mdragpoint_internal_r_source_2"], 0.6)
        self.assertEqual(event["mdragpoint_internal_r_abs_diff"], 0.1)

    def test_divergent_row_projection(self) -> None:
        row = self._base_row(
            "hstree",
            comparison_status="divergent", exact="0", within_tolerance="0",
            differing_fields="score", score_source_1="0.1", score_source_2="0.9",
            score_abs_diff="0.8", score_relative_diff="0.8888888888888888",
            max_abs_diff="0.8", max_relative_diff="0.8888888888888888",
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["comparison_status"], "divergent")
        self.assertEqual(event["exact"], 0)
        self.assertEqual(event["within_tolerance"], 0)
        self.assertEqual(event["differing_fields"], "score")
        self.assertEqual(event["score_abs_diff"], 0.8)

    def test_missing_row_has_null_values_and_null_timestamps(self) -> None:
        row = self._base_row(
            "hstree",
            comparison_status="missing_source_1", exact="0", within_tolerance="0",
            timestamp_equal="", timestamp_offset_seconds="",
            max_abs_diff="", max_relative_diff="",
            timestamp_source_2="", status_source_2="", anomaly_source_2="",
            score_source_2="", score_abs_diff="", score_relative_diff="",
            threshold_source_2="", threshold_abs_diff="", threshold_relative_diff="",
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["comparison_status"], "missing_source_1")
        self.assertIsNone(event["timestamp_source_2"])
        self.assertIsNone(event["status_source_2"])
        self.assertIsNone(event["anomaly_source_2"])
        self.assertIsNone(event["score_source_2"])
        self.assertIsNone(event["timestamp_equal"])
        self.assertIsNone(event["timestamp_offset_seconds"])

    def test_nan_and_infinities_become_null(self) -> None:
        row = self._base_row(
            "hstree",
            score_source_1="NaN", score_source_2="Infinity",
            max_abs_diff="-Infinity",
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertIsNone(event["score_source_1"])
        self.assertIsNone(event["score_source_2"])
        self.assertIsNone(event["max_abs_diff"])

    def test_anomaly_true_is_one(self) -> None:
        row = self._base_row("hstree", anomaly_source_1="true")
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["anomaly_source_1"], 1)

    def test_timestamp_fallback_to_publication(self) -> None:
        # Absent cells fall back to the publication instant...
        row = self._base_row(
            "hstree", timestamp_source_1="", timestamp_source_2=""
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["timestamp_origin"], "publication")
        self.assertRegex(event["timestamp"], _TIMESTAMP_PATTERN)
        # ...and so do uninterpretable cells.
        row = self._base_row(
            "hstree",
            timestamp_source_1="not-a-date",
            timestamp_source_2="17/09/2026",
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["timestamp_origin"], "publication")
        self.assertRegex(event["timestamp"], _TIMESTAMP_PATTERN)

    def test_uninterpretable_source_1_falls_back_to_interpretable_source_2(self) -> None:
        row = self._base_row(
            "hstree",
            timestamp_source_1="garbage",
            timestamp_source_2="2026-09-15 10:00:01",
        )
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["timestamp"], "2026-09-15 10:00:01")
        self.assertEqual(event["timestamp_origin"], "source-2")

    def test_strings_are_not_trimmed(self) -> None:
        row = self._base_row("hstree", status_source_2=" anomaly ")
        event = build_diff_event(self.summary, row, "hstree")
        self.assertEqual(event["status_source_2"], " anomaly ")


class KafkaOrderAndConfigTest(TestCase):
    """Family: idempotent config, deterministic keys, publication order."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_config_keys_order_and_summary_content(self) -> None:
        source_1 = [
            _row("hstree", group="A", event="e1", score="0.1"),
            _row("hstree", group="A", event="e2", score="0.1"),
            _row("hstree", group="B", event="e1", score="0.1"),
        ]
        source_2 = [
            _row("hstree", group="A", event="e1", score="0.1"),
            _row("hstree", group="A", event="e2", score="0.9"),
        ]
        report, summary = _produce_comparison(self.tmp, "hstree", source_1, source_2)
        recorder: list = []
        count = publish(
            report, summary,
            bootstrap_servers="kafka1:9092,kafka2:9092",
            diff_topic="reproducibility-diff",
            summary_topic="reproducibility-summary",
            producer_factory=_make_factory(recorder),
        )
        self.assertEqual(count, 3)  # every report row is published
        producer = recorder[0]
        self.assertEqual(
            producer.config,
            {
                "bootstrap.servers": "kafka1:9092,kafka2:9092",
                "acks": "all",
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 1,
            },
        )
        topics = [topic for topic, _, _ in producer.produced]
        self.assertEqual(
            topics,
            [
                "reproducibility-diff",
                "reproducibility-diff",
                "reproducibility-diff",
                "reproducibility-summary",
            ],
        )
        # Deterministic keys, in report row order.
        self.assertEqual(
            producer.produced[0][1], "campaign-42|hstree|A|e1"
        )
        self.assertEqual(
            producer.produced[1][1], "campaign-42|hstree|A|e2"
        )
        self.assertEqual(
            producer.produced[2][1], "campaign-42|hstree|B|e1"
        )
        # The summary is the final event with its own key.
        self.assertEqual(producer.produced[3][1], "campaign-42|hstree|summary")
        summary_event = json.loads(producer.produced[3][2])
        self.assertEqual(summary_event["campaign_id"], "campaign-42")
        self.assertEqual(summary_event["model"], "hstree")
        self.assertEqual(summary_event["compared"], 2)
        self.assertEqual(summary_event["exact"], 1)
        self.assertEqual(summary_event["within_tolerance"], 0)
        self.assertEqual(summary_event["divergent"], 1)
        self.assertEqual(summary_event["missing_source_2"], 1)
        self.assertEqual(summary_event["missing_source_1"], 0)
        self.assertEqual(summary_event["abs_tolerance"], 0.0)
        self.assertEqual(summary_event["source_1_image"], "image-a:1")
        self.assertEqual(summary_event["source_2_image"], "image-b:1")
        self.assertEqual(summary_event["first_divergence_group"], "A")
        self.assertEqual(summary_event["first_divergence_source_event_id"], "e2")
        self.assertEqual(summary_event["first_divergence_fields"], "score")
        self.assertIsNone(summary_event["first_missing_source_1_group"])
        self.assertIsNone(summary_event["first_missing_source_1_source_event_id"])
        self.assertEqual(summary_event["first_missing_source_2_group"], "B")
        self.assertEqual(summary_event["first_missing_source_2_source_event_id"], "e1")
        self.assertRegex(summary_event["timestamp"], _TIMESTAMP_PATTERN)
        self.assertEqual(producer.flush_calls, 2)
        # Local CSV paths are never published.
        for _, _, payload in producer.produced:
            self.assertNotIn("/tmp/", payload)
        # Every payload is strict JSON (no NaN/Infinity constants).
        for _, _, payload in producer.produced:
            json.loads(payload, parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)))

    def test_summary_published_only_after_all_diff_events(self) -> None:
        rows = [_row("hstree", group="A", event=f"e{i}") for i in range(1, 5)]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)
        recorder: list = []
        publish(
            report, summary,
            bootstrap_servers="kafka1:9092",
            diff_topic="reproducibility-diff",
            summary_topic="reproducibility-summary",
            producer_factory=_make_factory(recorder),
        )
        producer = recorder[0]
        self.assertEqual(len(producer.produced), 5)
        self.assertEqual(producer.produced[-1][1], "campaign-42|hstree|summary")


class DeliveryErrorTest(TestCase):
    """Family: no summary after a failed delivery; errors name the topic."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_failed_diff_delivery_prevents_summary(self) -> None:
        rows = [_row("hstree", group="A", event="e1")]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)
        recorder: list = []
        with self.assertRaises(PublishError) as ctx:
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="reproducibility-diff",
                summary_topic="reproducibility-summary",
                producer_factory=_make_factory(recorder, fail_on="reproducibility-diff"),
            )
        self.assertIn("reproducibility-diff", str(ctx.exception))
        producer = recorder[0]
        produced_topics = {topic for topic, _, _ in producer.produced}
        self.assertNotIn("reproducibility-summary", produced_topics)

    def test_failed_summary_delivery_is_fatal(self) -> None:
        rows = [_row("hstree", group="A", event="e1")]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)
        recorder: list = []
        with self.assertRaises(PublishError) as ctx:
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="reproducibility-diff",
                summary_topic="reproducibility-summary",
                producer_factory=_make_factory(recorder, fail_on="reproducibility-summary"),
            )
        self.assertIn("reproducibility-summary", str(ctx.exception))




class SummaryEventTest(TestCase):
    """Family: the event flattens the comparator's diagnostic markers."""

    @staticmethod
    def _summary(markers: dict) -> dict:
        return {
            "format_version": 1, "model": "hstree", "campaign_id": "c",
            "source_1": {"csv": "/tmp/a.csv", "image": "i1"},
            "source_2": {"csv": "/tmp/b.csv", "image": "i2"},
            "abs_tolerance": 0.5, "relative_tolerance": 0.1,
            "compared": 2, "exact": 1, "within_tolerance": 1, "divergent": 0,
            "missing_source_1": 0, "missing_source_2": 0,
            "first_divergence": markers.get("first_divergence"),
            "first_missing_source_1": markers.get("first_missing_source_1"),
            "first_missing_source_2": markers.get("first_missing_source_2"),
            "max_abs_diff": 0.5, "max_relative_diff": 0.25,
        }

    def test_markers_are_flattened(self) -> None:
        event = build_summary_event(self._summary({
            "first_divergence": {"Group": "A", "source_event_id": "e2", "differing_fields": ["score", "threshold"]},
            "first_missing_source_1": {"Group": "B", "source_event_id": "e1"},
            "first_missing_source_2": None,
        }))
        self.assertEqual(event["first_divergence_group"], "A")
        self.assertEqual(event["first_divergence_source_event_id"], "e2")
        self.assertEqual(event["first_divergence_fields"], "score,threshold")
        self.assertEqual(event["first_missing_source_1_group"], "B")
        self.assertEqual(event["first_missing_source_1_source_event_id"], "e1")
        self.assertIsNone(event["first_missing_source_2_group"])
        self.assertIsNone(event["first_missing_source_2_source_event_id"])
        self.assertEqual(event["source_1_image"], "i1")
        self.assertEqual(event["source_2_image"], "i2")
        self.assertEqual(event["max_abs_diff"], 0.5)
        self.assertEqual(
            set(event),
            {
                "timestamp", "campaign_id", "model",
                "abs_tolerance", "relative_tolerance",
                "compared", "exact", "within_tolerance", "divergent",
                "missing_source_1", "missing_source_2",
                "max_abs_diff", "max_relative_diff",
                "source_1_image", "source_2_image",
                "first_divergence_group", "first_divergence_source_event_id",
                "first_divergence_fields",
                "first_missing_source_1_group", "first_missing_source_1_source_event_id",
                "first_missing_source_2_group", "first_missing_source_2_source_event_id",
            },
        )

    def test_absent_markers_are_null(self) -> None:
        event = build_summary_event(self._summary({}))
        for key in (
            "first_divergence_group", "first_divergence_source_event_id",
            "first_divergence_fields", "first_missing_source_1_group",
            "first_missing_source_1_source_event_id", "first_missing_source_2_group",
            "first_missing_source_2_source_event_id",
        ):
            self.assertIsNone(event[key], key)
        self.assertRegex(event["timestamp"], _TIMESTAMP_PATTERN)


class StreamingReportTest(TestCase):
    """Family: the report is streamed, never fully materialized."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_iter_report_rows_is_a_generator(self) -> None:
        import inspect

        self.assertTrue(inspect.isgeneratorfunction(publish_module._iter_report_rows))

    def test_first_pull_reads_only_the_header_and_one_row(self) -> None:
        rows = [_row("hstree", group="A", event=f"e{i:02d}") for i in range(1, 6)]
        report, _ = _produce_comparison(self.tmp, "hstree", rows, rows)
        lines_read: list[int] = []
        real_open = open

        class CountingHandle:
            def __init__(self, handle):
                self._handle = handle

            def readline(self, *args):
                lines_read.append(1)
                return self._handle.readline(*args)

            def __next__(self):
                line = self._handle.__next__()
                lines_read.append(1)
                return line

            def __iter__(self):
                return self

            def __enter__(self):
                self._handle.__enter__()
                return self

            def __exit__(self, *args):
                return self._handle.__exit__(*args)

            def __getattr__(self, name):
                return getattr(self._handle, name)

        def counting_open(*args, **kwargs):
            return CountingHandle(real_open(*args, **kwargs))

        with patch.object(publish_module, "open", counting_open, create=True):
            stream = publish_module._iter_report_rows(report, "hstree")
            first = next(stream)
        self.assertEqual(first["Group"], "A")
        self.assertLessEqual(len(lines_read), 3)  # header + first row at most

    def test_publish_never_materializes_the_report(self) -> None:
        rows = [_row("hstree", group="A", event=f"e{i:02d}") for i in range(1, 51)]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)
        recorder: list = []
        count = publish(
            report, summary,
            bootstrap_servers="kafka1:9092",
            diff_topic="reproducibility-diff",
            summary_topic="reproducibility-summary",
            producer_factory=_make_factory(recorder),
        )
        self.assertEqual(count, 50)


class BoundedFlushTest(TestCase):
    """Family: a flush that cannot complete in time is a fatal error."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_undelivered_diff_messages_prevent_the_summary(self) -> None:
        rows = [_row("hstree", group="A", event="e1")]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)
        recorder: list = []
        factory = _make_factory(recorder)

        def factory_with_stuck_flush(config):
            producer = factory(config)
            producer.flush_returns_pending = True
            return producer

        with self.assertRaises(PublishError) as ctx:
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="reproducibility-diff",
                summary_topic="reproducibility-summary",
                producer_factory=factory_with_stuck_flush,
            )
        self.assertIn("still undelivered", str(ctx.exception))
        self.assertIn(str(publish_module.FLUSH_TIMEOUT_SECONDS), str(ctx.exception))
        producer = recorder[0]
        produced_topics = {topic for topic, _, _ in producer.produced}
        self.assertNotIn("reproducibility-summary", produced_topics)


class ReportCountersAndMarkersValidationTest(TestCase):
    """Family: report counters and first markers must match the summary."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def _pair(self) -> tuple[Path, Path]:
        source_1 = [
            _row("hstree", group="A", event="e1", score="0.1"),
            _row("hstree", group="A", event="e2", score="0.1"),
        ]
        source_2 = [
            _row("hstree", group="A", event="e1", score="0.1"),
            _row("hstree", group="A", event="e2", score="0.9"),
        ]
        return _produce_comparison(self.tmp, "hstree", source_1, source_2)

    def _rewrite_summary(self, summary_path: Path, mutate) -> None:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        mutate(payload)
        summary_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_per_status_counter_mismatch_rejected(self) -> None:
        report, summary = self._pair()
        # Keep the counter identity, but move one exact row to within_tolerance
        # and divergent: total and identity hold, per-status counts do not.
        def mutate(payload):
            payload["exact"] = 0
            payload["within_tolerance"] = 1
            payload["divergent"] = 1
            payload["compared"] = 2

        self._rewrite_summary(summary, mutate)
        with self.assertRaises(PublishError) as ctx:
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=_make_factory([]),
            )
        self.assertIn(
            "reports 1 exact row(s) but the summary counts 0", str(ctx.exception)
        )

    def test_first_divergence_marker_mismatch_rejected(self) -> None:
        report, summary = self._pair()

        def mutate(payload):
            payload["first_divergence"] = {
                "Group": "Z", "source_event_id": "e9", "differing_fields": ["score"],
            }

        self._rewrite_summary(summary, mutate)
        with self.assertRaises(PublishError) as ctx:
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=_make_factory([]),
            )
        self.assertIn("first_divergence", str(ctx.exception))

    def test_null_marker_with_divergent_rows_rejected(self) -> None:
        report, summary = self._pair()

        def mutate(payload):
            payload["first_divergence"] = None

        self._rewrite_summary(summary, mutate)
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=_make_factory([]),
            )

    def test_marker_without_matching_rows_rejected(self) -> None:
        rows = [_row("hstree", group="A", event="e1")]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)

        def mutate(payload):
            payload["first_divergence"] = {
                "Group": "A", "source_event_id": "e1", "differing_fields": ["score"],
            }

        self._rewrite_summary(summary, mutate)
        with self.assertRaises(PublishError):
            publish(
                report, summary,
                bootstrap_servers="kafka1:9092",
                diff_topic="t", summary_topic="s",
                producer_factory=_make_factory([]),
            )

    def test_coherent_markers_accepted(self) -> None:
        report, summary = self._pair()
        recorder: list = []
        count = publish(
            report, summary,
            bootstrap_servers="kafka1:9092",
            diff_topic="reproducibility-diff",
            summary_topic="reproducibility-summary",
            producer_factory=_make_factory(recorder),
        )
        self.assertEqual(count, 2)


class MainCliTest(TestCase):
    """Family: main() end-to-end with the fake confluent_kafka, no broker."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_main_publishes_and_returns_zero(self) -> None:
        rows = [_row("hstree", group="A", event="e1")]
        report, summary = _produce_comparison(self.tmp, "hstree", rows, rows)
        fake_module = ModuleType("confluent_kafka")
        fake_module.Producer = FakeProducer
        saved = sys.modules.get("confluent_kafka")
        sys.modules["confluent_kafka"] = fake_module
        try:
            code = publish_module.main([
                "--report", str(report),
                "--summary", str(summary),
                "--bootstrap-servers", "kafka1:9092",
            ])
        finally:
            if saved is not None:
                sys.modules["confluent_kafka"] = saved
            else:
                sys.modules.pop("confluent_kafka", None)
        self.assertEqual(code, 0)

    def test_main_returns_one_on_validation_error(self) -> None:
        code = publish_module.main([
            "--report", str(self.tmp / "absent.csv"),
        ])
        self.assertEqual(code, 1)
