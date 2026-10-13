"""Reusable helpers for the CSV -> Kafka producer."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dasma_kafka.config.env import get_env_bool

if TYPE_CHECKING:
    from confluent_kafka import SerializingProducer
    import pandas as pd


DEFAULT_DATA_SOURCE_FILE = "data/source.csv"
DEFAULT_TOPIC = "source-data"
DEFAULT_TOPIC_VIZ = "source-data-viz"
SOURCE_EVENT_ID_FIELD = "source_event_id"
PROTECTED_SOURCE_COLUMNS = {
    SOURCE_EVENT_ID_FIELD,
    "timestamp",
    "Timestamp",
    "time",
    "Time",
    "Group",
    "group",
}


def _import_pandas():
    import pandas as pd

    return pd


def read_csv_auto(path: str):
    """Read CSV with automatic separator and tolerant encodings."""
    pd = _import_pandas()
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception as exc:  # pragma: no cover - defensive
            last_err = exc
    raise last_err  # type: ignore[misc]


def parse_excluded_fields(raw: str) -> list[str]:
    """Parse the comma-separated excluded field list from the environment."""
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def parse_selected_groups(raw: str) -> list[str]:
    """Parse the comma-separated selected group list from the environment."""
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def apply_group_filter(df: Any, selected_groups: list[str]) -> tuple[Any, str | None, int]:
    """Filter the dataframe to the selected DASMA groups.

    An empty selection means "all groups". A non-empty selection requires a
    source metadata column named `Group` or `group`; otherwise the campaign
    would silently publish unfiltered source data.
    """
    if not selected_groups:
        return df, None, len(df)

    group_column = None
    for candidate in ("Group", "group"):
        if candidate in df.columns:
            group_column = candidate
            break

    if group_column is None:
        raise ValueError("DASMA_SELECTED_GROUPS is set but the CSV has no Group/group column")

    selected = {str(group) for group in selected_groups}
    filtered = df[df[group_column].astype(str).isin(selected)].copy()
    if filtered.empty:
        raise ValueError(
            "DASMA_SELECTED_GROUPS matched no rows in the CSV: "
            + ", ".join(selected_groups)
        )

    return filtered, group_column, len(filtered)


def apply_dataframe_exclusions(
    df: Any,
    excluded_fields: list[str],
    protected_fields: set[str] | None = None,
) -> tuple[Any, list[str]]:
    """Drop excluded columns from the source dataframe, except protected metadata fields."""
    protected = PROTECTED_SOURCE_COLUMNS | set(protected_fields or set())
    columns_to_drop = [
        column
        for column in df.columns
        if str(column) in excluded_fields and str(column) not in protected
    ]
    if not columns_to_drop:
        return df, []
    return df.drop(columns=columns_to_drop), [str(column) for column in columns_to_drop]


def resolve_data_source_path(data_source: str, repo_root: Path, script_dir: Path) -> tuple[Path, list[Path]]:
    """Resolve the CSV path from env/user input using project-specific lookup rules."""
    if os.path.isabs(data_source):
        path = Path(data_source)
        return path, [path]

    candidates = [
        Path(data_source).resolve(),
        (repo_root / data_source).resolve(),
        (script_dir / data_source).resolve(),
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate, candidates

    return candidates[0], candidates


def sanitize_avro_field_name(name: str, used_names: set[str]) -> str:
    """Convert an arbitrary column name into a valid, unique Avro field name."""
    cleaned = str(name).strip()
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", cleaned)

    if not cleaned:
        cleaned = "field"

    if not re.match(r"[A-Za-z_]", cleaned[0]):
        cleaned = f"f_{cleaned}"

    base = cleaned
    idx = 1
    while cleaned in used_names:
        cleaned = f"{base}_{idx}"
        idx += 1

    used_names.add(cleaned)
    return cleaned


def infer_avro_type(series: Any) -> str:
    """Infer a primitive Avro type from a pandas Series."""
    pd = _import_pandas()
    dtype = series.dtype

    if pd.api.types.is_integer_dtype(dtype):
        return "long"
    if pd.api.types.is_float_dtype(dtype):
        return "double"
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "string"
    return "string"


def build_avro_schema_from_dataframe(df: Any) -> tuple[str, dict[str, str]]:
    """Build an Avro record schema and original-column to Avro-name mapping."""
    fields: list[dict[str, Any]] = []
    name_mapping: dict[str, str] = {}
    used_names: set[str] = set()

    for col_name in df.columns:
        avro_type = infer_avro_type(df[col_name])
        avro_field_name = sanitize_avro_field_name(str(col_name), used_names)
        name_mapping[str(col_name)] = avro_field_name

        field_def: dict[str, Any] = {"name": avro_field_name, "type": avro_type}
        if str(col_name) != avro_field_name:
            field_def["doc"] = f"original column name: {col_name}"

        fields.append(field_def)

    schema_dict: dict[str, Any] = {
        "type": "record",
        "name": "Measurement",
        "namespace": "dasma",
        "fields": fields,
    }
    return json.dumps(schema_dict, ensure_ascii=False), name_mapping


def make_avro_producer(schema_str: str) -> SerializingProducer:
    """Create a SerializingProducer with an AvroSerializer for the value."""
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import StringSerializer

    from dasma_kafka.kafka.schema_registry import make_schema_registry_client_from_env

    sr_client = make_schema_registry_client_from_env()

    def to_dict(value: dict[str, Any], _) -> dict[str, Any]:
        return value

    avro_conf = {
        "auto.register.schemas": get_env_bool("SCHEMA_AUTO_REGISTER", True),
        "normalize.schemas": True,
        "use.latest.version": get_env_bool("SCHEMA_USE_LATEST", False),
    }

    value_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        to_dict=to_dict,
        conf=avro_conf,
    )

    conf: dict[str, Any] = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092"),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "delivery.timeout.ms": 300000,
        "enable.idempotence": True,
    }
    return SerializingProducer(conf)


def make_json_producer() -> SerializingProducer:
    """Create a SerializingProducer that publishes JSON strings."""
    from confluent_kafka import SerializingProducer
    from confluent_kafka.serialization import StringSerializer

    conf: dict[str, Any] = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092"),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": StringSerializer("utf_8"),
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "delivery.timeout.ms": 300000,
        "enable.idempotence": True,
    }
    return SerializingProducer(conf)


def cast_value_to_avro(value: Any, avro_type: str) -> Any:
    """Cast a Python value to a type compatible with the given Avro primitive."""
    if value is None:
        return None
    if avro_type == "long":
        return int(value)
    if avro_type == "double":
        return float(value)
    if avro_type == "boolean":
        return bool(value)
    return str(value)


def ensure_timestamp_column(df: Any, env_ts_name: str):
    """Ensure the dataframe exposes a usable timestamp column."""
    if env_ts_name:
        if env_ts_name not in df.columns:
            available = ", ".join(map(str, df.columns))
            raise ValueError(
                f"DATA_TIMESTAMP_NAME='{env_ts_name}' but this column is missing in the CSV.\n"
                f"Available columns: {available}"
            )
        return df

    if "timestamp" in df.columns:
        ts_col = df.pop("timestamp")
        df.insert(0, "timestamp", ts_col)
        print(
            "[timestamp] DATA_TIMESTAMP_NAME not set: using existing 'timestamp' column from CSV "
            "(moved to first column)."
        )
        return df

    base = datetime.now()
    timestamps = [
        (base + timedelta(microseconds=i)).strftime("%Y-%m-%d %H:%M:%S.%f")
        for i in range(len(df))
    ]
    df.insert(0, "timestamp", timestamps)
    print(
        "[timestamp] DATA_TIMESTAMP_NAME not set: added synthetic 'timestamp' column "
        f"starting at {timestamps[0]} with +1us increments."
    )
    return df


def ensure_source_event_id_column(df: Any, field_name: str = SOURCE_EVENT_ID_FIELD):
    """Ensure every emitted source row has a stable event-level join key.

    The realtime producer may restamp many rows to the same second.  Consensus
    therefore needs a separate key that remains unique even when display
    timestamps collide.  Existing CSV values are preserved; otherwise IDs are
    generated in emission order after filtering.
    """
    if field_name in df.columns:
        event_ids = df.pop(field_name).astype(str)
        insert_at = 1 if "timestamp" in df.columns else 0
        df.insert(insert_at, field_name, event_ids)
        print(
            f"[source-event] using existing '{field_name}' column from CSV "
            "(moved next to timestamp)."
        )
        return df

    event_ids = [f"event-{i:012d}" for i in range(1, len(df) + 1)]
    insert_at = 1 if "timestamp" in df.columns else 0
    df.insert(insert_at, field_name, event_ids)
    if event_ids:
        print(
            f"[source-event] added '{field_name}' column "
            f"from {event_ids[0]} to {event_ids[-1]}."
        )
    else:
        print(f"[source-event] added empty '{field_name}' column.")
    return df
