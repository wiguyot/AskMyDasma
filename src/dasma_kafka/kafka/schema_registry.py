"""Schema Registry client helpers."""

from __future__ import annotations

from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient

from dasma_kafka.config.env import get_env_str


def make_schema_registry_client_from_env() -> SchemaRegistryClient:
    """Build a Schema Registry client from environment variables."""
    url = get_env_str("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    conf: dict[str, Any] = {"url": url}

    user_info = get_env_str("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "")
    if user_info:
        conf["basic.auth.user.info"] = user_info
        conf["basic.auth.credentials.source"] = get_env_str(
            "SCHEMA_REGISTRY_BASIC_AUTH_SOURCE",
            "USER_INFO",
        )

    return SchemaRegistryClient(conf)

