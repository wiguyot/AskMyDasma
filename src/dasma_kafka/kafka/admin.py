"""Admin helpers shared across Kafka-oriented scripts."""

from __future__ import annotations

import logging
import re
import socket
import time

from confluent_kafka.admin import AdminClient


def make_admin(bootstrap: str) -> AdminClient:
    """Create an AdminClient from bootstrap servers."""
    if not bootstrap:
        raise ValueError("No bootstrap provided (e.g., kafka1:9092)")
    return AdminClient({"bootstrap.servers": bootstrap})


def count_brokers(admin: AdminClient, timeout: float = 10.0) -> int:
    """Return the number of brokers visible through metadata."""
    metadata = admin.list_topics(timeout=timeout)
    return len(metadata.brokers)


def wait_for_cluster(admin: AdminClient, target_brokers: int, sleep_seconds: float) -> None:
    """Wait until the cluster exposes at least target_brokers brokers."""
    logging.info("Waiting for Kafka cluster to be ready (need >= %d broker(s))...", target_brokers)
    while True:
        try:
            brokers = count_brokers(admin)
            if brokers >= target_brokers:
                logging.info("Kafka cluster ready (brokers=%d)", brokers)
                return
            logging.info("Kafka not ready yet (brokers=%d), sleeping %.1fs", brokers, sleep_seconds)
        except Exception as exc:
            logging.warning("Kafka metadata fetch failed (%s), sleeping %.1fs", exc, sleep_seconds)
        time.sleep(sleep_seconds)


def parse_bootstrap_hosts(bootstrap: str) -> list[str]:
    """Extract hostnames from a comma-separated bootstrap string."""
    hosts: list[str] = []
    for part in bootstrap.split(","):
        part = part.strip()
        if not part:
            continue
        host = part.split(":", 1)[0]
        hosts.append(host)
    return hosts


def preflight_hostname_resolution(bootstrap: str) -> None:
    """Log unresolved hosts and common naming suggestions."""
    unresolved: list[str] = []
    suggestions: list[str] = []

    for host in parse_bootstrap_hosts(bootstrap):
        try:
            socket.getaddrinfo(host, None)
        except Exception:
            unresolved.append(host)
            match = re.match(r"^(.*?)-(\d+)$", host)
            if match:
                suggestions.append(f"{match.group(1)}{match.group(2)}")

    if unresolved:
        logging.error("The following hosts cannot be resolved on this machine: %s", ", ".join(unresolved))
        if suggestions:
            logging.error(
                "Tip: in this project, the Docker Compose broker service name is typically 'kafka1'. "
                "Try for example: %s (with port :9092).",
                ", ".join(suggestions),
            )
        logging.error(
            "If you run this script on the host (not inside the Docker network), "
            "use the published port, e.g., localhost:9092",
        )
