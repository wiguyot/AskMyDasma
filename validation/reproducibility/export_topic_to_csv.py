#!/usr/bin/env python3
"""
export_topic_to_csv.py

Autonomous exporter of one JSON viz topic (``<model>-analysis-viz``) to the
phase-2 reproducibility CSV contract. For a normal reproducibility campaign it
produces:

    <REPRODUCIBILITY_OUTPUT_ROOT>/<campaign-id>/<model>-source-<1|2>.csv
    <REPRODUCIBILITY_OUTPUT_ROOT>/<campaign-id>/<model>-source-<1|2>.metadata.json

Design points
-------------
* Consumes with ``auto.offset.reset=earliest`` and manual per-message commits:
  a record is durably staged (sqlite3) *before* its offset is committed, so a
  crash never loses or duplicates data (same key + same row is idempotent).
* External sort via sqlite staging: the full campaign is never held in memory.
  The final CSV is written ``ORDER BY group_key, event_key``.
* Fatal contract/storage/Kafka errors leave the faulty offset uncommitted and
  publish no final file.
* The metadata file is published last: its presence is the success marker; a
  lone CSV after a failure is invalid. Existing exports are never overwritten.
* Idle exit only after a first valid record; EOF is not data. This allows P2-4
  to start the exporter before the producer without a premature exit.
* ``DASMA_ENABLED=0`` idles before any Kafka/SQLite/file construction.

Direct execution from the repository root (with confluent_kafka available):

    REPRODUCIBILITY_MODEL_ID=hstree \\
    REPRODUCIBILITY_SOURCE_ROLE=source-1 \\
    REPRODUCIBILITY_CAMPAIGN_ID=campaign-42 \\
    BOOTSTRAP_SERVERS=localhost:9092 \\
    python validation/reproducibility/export_topic_to_csv.py
"""

from __future__ import annotations

import csv
import json
import os
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from validation.reproducibility.csv_contract import (
        build_metadata,
        canonical_key,
        columns_for_model,
        normalize_viz_record,
    )
else:
    from .csv_contract import (
        build_metadata,
        canonical_key,
        columns_for_model,
        normalize_viz_record,
    )

DEFAULT_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
DEFAULT_OUTPUT_ROOT = "data/reproducibility"
DEFAULT_IDLE_EXIT_SEC = 15
DEFAULT_STARTUP_TIMEOUT_SEC = 300

# The accepted models are exactly those of reproducibility.csv_contract; the
# mapping adds the unique JSON viz topic of each detection model. There is no
# Avro topic and no explainability export.
MODEL_VIZ_TOPICS = {
    "hstree": "hstree-analysis-viz",
    "iforest": "iforest-analysis-viz",
    "kitnet": "kitnet-analysis-viz",
    "deepant": "deepant-analysis-viz",
    "mdragpoint": "mdragstream-point-analysis-viz",
}


@dataclass(frozen=True)
class ExportConfig:
    """Immutable configuration of one CSV export run."""

    bootstrap_servers: str
    model_id: str
    source_role: str
    campaign_id: str
    source_topic: str
    group_id: str
    output_root: str
    idle_exit_sec: int
    startup_timeout_sec: int
    image: str
    campaign_parameters: dict[str, Any]

    @property
    def campaign_dir(self) -> Path:
        return Path(self.output_root) / self.campaign_id

    @property
    def role_suffix(self) -> str:
        return "1" if self.source_role == "source-1" else "2"

    @property
    def csv_path(self) -> Path:
        return self.campaign_dir / f"{self.model_id}-source-{self.role_suffix}.csv"

    @property
    def metadata_path(self) -> Path:
        return self.campaign_dir / f"{self.model_id}-source-{self.role_suffix}.metadata.json"

    @property
    def staging_path(self) -> Path:
        return self.campaign_dir / f"{self.model_id}-source-{self.role_suffix}.staging.sqlite3"


def _env_str(env: Mapping[str, str], key: str, default: str = "") -> str:
    value = env.get(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()


def load_config(environ: Mapping[str, str] | None = None) -> ExportConfig:
    """Build and fully validate the export configuration (pure, testable)."""
    env = os.environ if environ is None else environ

    model_id = _env_str(env, "REPRODUCIBILITY_MODEL_ID")
    if not model_id or model_id not in MODEL_VIZ_TOPICS:
        raise ValueError(
            f"reproducibility model is invalid or missing: expected one of "
            f"{', '.join(sorted(MODEL_VIZ_TOPICS))}, got {model_id!r}"
        )
    source_role = _env_str(env, "REPRODUCIBILITY_SOURCE_ROLE")
    if source_role not in ("source-1", "source-2"):
        raise ValueError(
            "reproducibility source role is invalid or missing: expected exactly "
            f"'source-1' or 'source-2', got {source_role!r}"
        )
    campaign_id = _env_str(env, "REPRODUCIBILITY_CAMPAIGN_ID")
    # Same portable directory-name rule as P2-1/P2-2.
    import re

    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", campaign_id):
        raise ValueError(
            "reproducibility campaign-id is invalid or missing: it must be a "
            "portable directory name (alphanumeric, dot, underscore or hyphen; "
            "starting with an alphanumeric character; at most 128 characters); got "
            f"{campaign_id!r}"
        )

    source_topic = _env_str(env, "REPRODUCIBILITY_SOURCE_TOPIC", MODEL_VIZ_TOPICS[model_id])
    if not source_topic:
        raise ValueError("REPRODUCIBILITY_SOURCE_TOPIC resolved to an empty topic")
    group_id = _env_str(
        env,
        "REPRODUCIBILITY_GROUP_ID",
        f"reproducibility-export-{campaign_id}-{source_role}",
    )
    if not group_id:
        raise ValueError("REPRODUCIBILITY_GROUP_ID resolved to an empty consumer group")

    idle_exit_sec = _env_positive_int(env, "REPRODUCIBILITY_IDLE_EXIT_SEC", DEFAULT_IDLE_EXIT_SEC)
    startup_timeout_sec = _env_positive_int(
        env, "REPRODUCIBILITY_STARTUP_TIMEOUT_SEC", DEFAULT_STARTUP_TIMEOUT_SEC
    )

    parameters_json = _env_str(env, "REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON", "{}")
    try:
        campaign_parameters = json.loads(parameters_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON must be valid JSON: {exc}"
        ) from exc
    if not isinstance(campaign_parameters, dict):
        raise ValueError(
            "REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON must be a JSON object, got "
            f"{type(campaign_parameters).__name__}"
        )
    try:
        json.dumps(campaign_parameters, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "REPRODUCIBILITY_CAMPAIGN_PARAMETERS_JSON must be standard JSON "
            f"(allow_nan=False): {exc}"
        ) from exc

    return ExportConfig(
        bootstrap_servers=_env_str(env, "BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        model_id=model_id,
        source_role=source_role,
        campaign_id=campaign_id,
        source_topic=source_topic,
        group_id=group_id,
        output_root=_env_str(env, "REPRODUCIBILITY_OUTPUT_ROOT", DEFAULT_OUTPUT_ROOT),
        idle_exit_sec=idle_exit_sec,
        startup_timeout_sec=startup_timeout_sec,
        image=_env_str(env, "REPRODUCIBILITY_IMAGE", ""),
        campaign_parameters=campaign_parameters,
    )


def _env_positive_int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise ValueError(f"{key} must be a strictly positive integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{key} must be a strictly positive integer, got {value}")
    return value


def build_consumer_config(config: ExportConfig) -> dict[str, Any]:
    """Exact consumer configuration for the single viz topic (testable)."""
    return {
        "bootstrap.servers": config.bootstrap_servers,
        "group.id": config.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    }


# ---------------------------------------------------------------------------
# sqlite staging (external sort)
# ---------------------------------------------------------------------------


def remove_staging(path: Path) -> None:
    """Remove the SQLite database and every journal file belonging to it."""
    for suffix in ("", "-journal", "-wal", "-shm"):
        Path(f"{path}{suffix}").unlink(missing_ok=True)


def init_staging(path: Path) -> sqlite3.Connection:
    """Create a fresh, durable staging database.

    Docker Desktop's shared folders reject SQLite rollback journals despite a
    writable bind mount. WAL works there and retains transactional durability;
    ``synchronous=FULL`` makes the durability boundary explicit because Kafka
    offsets are committed only after the SQLite transaction.
    """
    remove_staging(path)
    conn = sqlite3.connect(path)
    try:
        journal_mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            raise RuntimeError(
                f"SQLite staging requires WAL journal mode, got {journal_mode!r}"
            )
        conn.execute("PRAGMA synchronous=FULL")
    except Exception:
        conn.close()
        remove_staging(path)
        raise
    conn.execute(
        "CREATE TABLE rows ("
        "group_key TEXT NOT NULL, "
        "event_key TEXT NOT NULL, "
        "row_json TEXT NOT NULL, "
        "PRIMARY KEY (group_key, event_key))"
    )
    conn.commit()
    return conn


def stage_record(conn: sqlite3.Connection, key: tuple[str, str], row_json: str) -> None:
    """Durably stage one normalized row; committed before the Kafka commit.

    Same key with the same row is idempotent (one final line); the same key
    with a different row is a fatal contract error.
    """
    existing = conn.execute(
        "SELECT row_json FROM rows WHERE group_key = ? AND event_key = ?",
        (key[0], key[1]),
    ).fetchone()
    if existing is not None:
        if existing[0] != row_json:
            raise ValueError(
                f"conflicting row for canonical key {key}: staged export would "
                "be ambiguous"
            )
        return
    conn.execute(
        "INSERT INTO rows (group_key, event_key, row_json) VALUES (?, ?, ?)",
        (key[0], key[1], row_json),
    )
    conn.commit()


def write_exports(conn: sqlite3.Connection, config: ExportConfig) -> None:
    """Write the final CSV and metadata under temp names, CSV first.

    ``Path.replace()`` publishes each file; the metadata lands last so its
    presence is the success marker.
    """
    columns = columns_for_model(config.model_id)
    csv_tmp = config.csv_path.with_name(config.csv_path.name + ".tmp")
    metadata_tmp = config.metadata_path.with_name(config.metadata_path.name + ".tmp")

    with open(csv_tmp, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="raise")
        writer.writeheader()
        cursor = conn.execute(
            "SELECT row_json FROM rows ORDER BY group_key, event_key"
        )
        for (row_json,) in cursor:
            writer.writerow(json.loads(row_json))

    metadata = build_metadata(
        model=config.model_id,
        source_role=config.source_role,
        campaign_id=config.campaign_id,
        campaign_parameters=config.campaign_parameters,
        image=config.image,
    )
    metadata_tmp.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    csv_tmp.replace(config.csv_path)
    metadata_tmp.replace(config.metadata_path)


def _ensure_no_existing_export(config: ExportConfig) -> None:
    for path in (config.csv_path, config.metadata_path):
        if path.exists():
            raise RuntimeError(
                f"refusing to overwrite an existing export: {path} — remove it "
                "explicitly before exporting again"
            )


def _monotonic() -> float:
    """Indirection kept for testability of the timeout logic."""
    return time.monotonic()


def main() -> None:
    if os.getenv("DASMA_ENABLED", "1") == "0":
        print("[reproducibility-export] DASMA_ENABLED=0 — service disabled. Idling.", flush=True)
        signal.pause()
        return

    config = load_config()
    _ensure_no_existing_export(config)
    config.campaign_dir.mkdir(parents=True, exist_ok=True)

    # Kafka imports stay deferred after the kill switch, as required.
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(build_consumer_config(config))
    consumer.subscribe([config.source_topic])
    conn = init_staging(config.staging_path)
    print(
        f"[reproducibility-export] model={config.model_id} role={config.source_role} "
        f"campaign={config.campaign_id} topic={config.source_topic} "
        f"group={config.group_id}",
        flush=True,
    )

    running = True
    interrupted = False

    def _handle_stop(sig, frame):  # noqa: ANN001
        nonlocal running, interrupted
        running = False
        interrupted = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    stored_any = False
    started_at = _monotonic()
    last_data_at = started_at

    try:
        while running:
            msg = consumer.poll(1.0)
            now = _monotonic()

            if not stored_any and now - started_at > config.startup_timeout_sec:
                raise RuntimeError(
                    f"no valid record received within {config.startup_timeout_sec}s "
                    f"on topic '{config.source_topic}' — failing without publishing "
                    "any export"
                )
            if stored_any and now - last_data_at > config.idle_exit_sec:
                break  # idle exit: end of stream after a first valid record

            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                code = err.code() if hasattr(err, "code") and callable(err.code) else None
                if code is not None and code == KafkaError._PARTITION_EOF:
                    continue  # EOF is normal and never counts as data
                raise RuntimeError(
                    f"kafka error on topic '{config.source_topic}': {err}"
                )

            raw = msg.value()
            try:
                record = json.loads(raw)
            except (json.JSONDecodeError, TypeError) as exc:
                raise RuntimeError(
                    f"malformed JSON on topic '{config.source_topic}' at offset "
                    f"{msg.offset()}: {exc}"
                ) from exc
            if not isinstance(record, dict):
                raise RuntimeError(
                    f"viz record on topic '{config.source_topic}' at offset "
                    f"{msg.offset()} must be a JSON object, got "
                    f"{type(record).__name__}"
                )

            row = normalize_viz_record(config.model_id, record)
            key = canonical_key(row)
            row_json = json.dumps(row, ensure_ascii=False, sort_keys=True)
            stage_record(conn, key, row_json)
            # Durable first: only a committed staging row may be committed Kafka-side.
            consumer.commit(message=msg, asynchronous=False)
            stored_any = True
            last_data_at = _monotonic()
    except Exception:
        # Fatal error: no final file, faulty offset left uncommitted. The
        # staging temp file is ours alone and is dropped.
        conn.close()
        remove_staging(config.staging_path)
        consumer.close()
        raise

    if interrupted:
        conn.close()
        consumer.close()
        print(
            "[reproducibility-export] interrupted — resources closed, no export "
            "published (the faulty-or-partial stream is not finalized)",
            file=sys.stderr,
            flush=True,
        )
        return

    write_exports(conn, config)
    conn.close()
    consumer.close()
    print(
        f"[reproducibility-export] published {config.csv_path} and "
        f"{config.metadata_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
