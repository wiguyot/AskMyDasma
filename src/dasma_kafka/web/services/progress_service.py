"""Progress calculation service for AskMyDasma."""

from __future__ import annotations

import logging
from pathlib import Path

from ..models import (
    CampaignProgressView,
    CampaignView,
    ExplainabilityV1ProgressView,
    ExplainabilityV1State,
    ExplainabilityV2ProgressView,
    ExplainabilityV2State,
    ProgressItemView,
)
from ..orchestration.compose_runner import ComposeRunner, ComposeError
from ..settings import WebSettings
from .docker_service import DockerService


PROGRESS_TOPICS: list[tuple[str, str]] = [
    ("HSTree Analysis", "hstree-analysis"),
    ("IForest Analysis", "iforest-analysis"),
    ("KitNet Analysis", "kitnet-analysis"),
    ("DeepAnT Analysis", "deepant-analysis"),
    ("mDragStream Point-based Analysis", "mdragstream-point-analysis"),
    ("Explainability", "explainability"),
    ("Explainability v2", "explainability-v2"),
    ("Source Data Producer", "source-data"),
]

MODEL_PROGRESS_TOPICS: dict[str, tuple[str, str]] = {
    "hstree": ("HSTree Analysis", "hstree-analysis"),
    "iforest": ("IForest Analysis", "iforest-analysis"),
    "kitnet": ("KitNet Analysis", "kitnet-analysis"),
    "deepant": ("DeepAnT Analysis", "deepant-analysis"),
    "mdragpoint": ("mDragStream Point-based Analysis", "mdragstream-point-analysis"),
    "explainability": ("Explainability", "explainability"),
    "explainability-v2": ("Explainability v2", "explainability-v2"),
}

SOURCE_PROGRESS_TOPIC: tuple[str, str] = ("Source Data Producer", "source-data")

# Explainability v2 completion inputs (OC-11).
EXPLAINABILITY_V2_MODEL = "explainability-v2"
EXPLAINABILITY_V2_CONSUMER_GROUP = "explainability-v2-consumer"
EXPLAINABILITY_V2_UPSTREAM_TOPIC = "hstree-analysis"
EXPLAINABILITY_V2_OUTPUT_TOPIC = "explainability-v2"
EXPLAINABILITY_V2_SERVICE = "explainability-v2"
# Docker raw states that mean the v2 worker can no longer make progress.
EXPLAINABILITY_V2_DEAD_STATES = frozenset({"exited", "dead", "error"})

# Explainability V1 completion inputs (V1-04c). The output topic counted for
# completeness is the Avro one, like V2 counts its own Avro output.
EXPLAINABILITY_V1_MODEL = "explainability"
EXPLAINABILITY_V1_CONSUMER_GROUP = "explainability-consumer"
EXPLAINABILITY_V1_UPSTREAM_TOPIC = "hstree-analysis"
EXPLAINABILITY_V1_OUTPUT_TOPIC = "explainability"
EXPLAINABILITY_V1_SERVICE = "explainability"


def _explainability_upstream_topic(campaign: CampaignView, service: str) -> str:
    prefix = "explainability_v2" if service == EXPLAINABILITY_V2_MODEL else "explainability"
    model = getattr(campaign, f"{prefix}_model", None)
    mode = getattr(campaign, f"{prefix}_mode", None)
    if mode == "simple":
        return "source-data"
    if mode == "reference" and model:
        return f"{model}-analysis"
    return "hstree-analysis"


class ProgressService:
    """Compute an approximate progress value from the dataset and Kafka offsets."""

    def __init__(self, settings: WebSettings, docker_service: DockerService | None = None):
        self.settings = settings
        self._runner = ComposeRunner(settings.project_root, host_project_root=settings.host_project_root)
        self._docker_service = docker_service or DockerService(settings)

    def _get_topic_offsets(self, topics: list[str]) -> dict[str, int | None]:
        if not topics:
            return {}

        script = "; ".join(
            f'/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka1:9092 --topic "{t}" 2>/dev/null | while IFS= read -r line; do printf "%s\\t%s\\n" "{t}" "$line"; done'
            for t in topics
        )
        result = self._runner.exec("kafka1", "bash", "-lc", script)

        offsets: dict[str, int | None] = {t: None for t in topics}
        for raw_line in result.stdout.splitlines():
            line = raw_line.rstrip("\n")
            if not line:
                continue
            topic, _, payload = line.partition("\t")
            if not topic:
                continue
            parts = payload.split(":")
            if len(parts) >= 3:
                try:
                    offsets[topic] = int(parts[-1])
                except ValueError:
                    pass
        return offsets

    def _resolve_dataset_path(self, dataset_path: str) -> Path:
        path = Path(dataset_path)
        if path.is_absolute():
            return path
        return self.settings.project_root / dataset_path

    def _count_dataset_rows(self, dataset_path: str) -> int:
        csv_path = self._resolve_dataset_path(dataset_path)
        if not csv_path.exists():
            return 0

        with csv_path.open("r", encoding="utf-8", errors="replace") as handle:
            row_count = sum(1 for _ in handle)

        return max(row_count - 1, 0)

    @staticmethod
    def _progress_percent(produced_records: int, total_records: int) -> float:
        if total_records <= 0:
            return 0.0
        return round(min((produced_records / total_records) * 100.0, 100.0), 1)

    def _get_consumer_group_lag(self, group: str, topic: str) -> int | None:
        """Sum the LAG values of a Kafka consumer group on one topic.

        Kafka 3.9 refuses `--topic` together with `--describe` ("Option
        [topic] can't be used with option [describe]"), so the whole group
        description is read and rows are filtered on the TOPIC column.
        Returns None whenever the output is ambiguous (missing header, no row
        for the requested topic, absent offset, unparseable value, command
        failure): missing information is never converted into a zero lag, and
        zero is only returned when the topic explicitly shows a numeric zero.
        """
        script = (
            "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 "
            f'--describe --group "{group}" 2>/dev/null'
        )
        try:
            result = self._runner.exec("kafka1", "bash", "-lc", script)
        except Exception as exc:
            logging.warning("Unable to read consumer group %s lag: %s", group, exc)
            return None

        lines = result.stdout.splitlines()
        header_index = None
        topic_index = None
        lag_index = None
        for index, line in enumerate(lines):
            tokens = line.split()
            upper_tokens = [token.upper() for token in tokens]
            # Both columns are required: a header carrying only LAG (or only
            # TOPIC) is ambiguous and must never be indexed positionally.
            if "LAG" in upper_tokens and "TOPIC" in upper_tokens:
                header_index = index
                lag_index = upper_tokens.index("LAG")
                topic_index = upper_tokens.index("TOPIC")
                break
        if header_index is None:
            return None

        total_lag = 0
        topic_seen = False
        for line in lines[header_index + 1 :]:
            tokens = line.split()
            if not tokens:
                continue
            if len(tokens) <= lag_index or len(tokens) <= topic_index:
                return None
            if tokens[topic_index] != topic:
                continue
            topic_seen = True
            if not tokens[lag_index].isdigit():
                return None
            total_lag += int(tokens[lag_index])
        if not topic_seen:
            return None
        return total_lag

    def _service_dead(self, service_name: str) -> bool | None:
        """Report whether a worker Docker service is unable to make progress.

        True: service exited/dead/error or absent from `docker compose ps`.
        False: service is present and alive.
        None: Docker itself could not be observed (observation failure).
        """
        try:
            services = self._docker_service.get_services_status()
        except Exception as exc:
            logging.warning("Unable to observe %s service state: %s", service_name, exc)
            return None
        for service in services:
            if service.name == service_name or service.container_name == service_name:
                return service.state in EXPLAINABILITY_V2_DEAD_STATES
        return True

    def _explainability_v2_service_dead(self) -> bool | None:
        """Report whether the v2 Docker service is unable to make progress."""
        return self._service_dead(EXPLAINABILITY_V2_SERVICE)

    @staticmethod
    def _produced_from_offset(offset: int | None) -> int | None:
        if offset is None or offset < 0:
            return None
        return offset + 1

    def _build_explainability_v2_progress(
        self,
        campaign: CampaignView,
        offsets: dict[str, int | None],
    ) -> ExplainabilityV2ProgressView:
        if EXPLAINABILITY_V2_MODEL not in campaign.selected_models:
            return ExplainabilityV2ProgressView()

        expected_records = self._count_dataset_rows(campaign.dataset_path)
        upstream_topic = _explainability_upstream_topic(campaign, EXPLAINABILITY_V2_MODEL)
        upstream_records = self._produced_from_offset(offsets.get(upstream_topic))
        published_records = self._produced_from_offset(offsets.get(EXPLAINABILITY_V2_OUTPUT_TOPIC))
        source_records = self._produced_from_offset(offsets.get(SOURCE_PROGRESS_TOPIC[1]))

        def view(state: ExplainabilityV2State, reason: str, consumer_lag: int | None) -> ExplainabilityV2ProgressView:
            return ExplainabilityV2ProgressView(
                selected=True,
                state=state,
                reason=reason,
                upstream_records=upstream_records,
                published_records=published_records,
                consumer_lag=consumer_lag,
            )

        service_dead = self._explainability_v2_service_dead()
        if campaign.status in {"starting", "running"}:
            if service_dead is True:
                return view("failed", "v2 worker service stopped", None)
            if service_dead is None:
                # Docker could not be observed: completeness cannot be
                # confirmed, so the state is processing, never complete.
                return view("processing", "unable to confirm v2 worker service state", None)

        if campaign.mode != "batch":
            return view("processing", "realtime campaign active: v2 keeps processing", None)

        consumer_lag = self._get_consumer_group_lag(
            EXPLAINABILITY_V2_CONSUMER_GROUP, upstream_topic
        )
        if consumer_lag is None:
            return view("processing", "unable to confirm v2 consumer lag", None)

        if upstream_records is None or source_records is None:
            return view("processing", "unable to confirm v2 upstream records", consumer_lag)
        if upstream_records < expected_records or source_records < expected_records:
            return view("processing", "waiting for HSTree upstream", consumer_lag)

        if consumer_lag > 0:
            return view("processing", f"v2 consumer lag: {consumer_lag}", consumer_lag)

        if published_records is None:
            return view("processing", "unable to confirm v2 published results", consumer_lag)
        if published_records < expected_records:
            return view("processing", f"v2 published results: {published_records}/{expected_records}", consumer_lag)

        return view("complete", "v2 delivery and commit confirmed for all expected records", consumer_lag)

    def get_explainability_v2_progress(self, campaign: CampaignView | None) -> ExplainabilityV2ProgressView:
        if campaign is None or EXPLAINABILITY_V2_MODEL not in campaign.selected_models:
            return ExplainabilityV2ProgressView()
        try:
            offsets = self._get_topic_offsets(
                [_explainability_upstream_topic(campaign, EXPLAINABILITY_V2_MODEL), EXPLAINABILITY_V2_OUTPUT_TOPIC, SOURCE_PROGRESS_TOPIC[1]]
            )
        except Exception as exc:
            logging.warning("Unable to compute explainability-v2 progress: %s", exc)
            offsets = {}
        return self._build_explainability_v2_progress(campaign, offsets)

    def _build_explainability_v1_progress(
        self,
        campaign: CampaignView,
        offsets: dict[str, int | None],
    ) -> ExplainabilityV1ProgressView:
        if EXPLAINABILITY_V1_MODEL not in campaign.selected_models:
            return ExplainabilityV1ProgressView()

        expected_records = self._count_dataset_rows(campaign.dataset_path)
        upstream_topic = _explainability_upstream_topic(campaign, EXPLAINABILITY_V1_MODEL)
        upstream_records = self._produced_from_offset(offsets.get(upstream_topic))
        published_records = self._produced_from_offset(offsets.get(EXPLAINABILITY_V1_OUTPUT_TOPIC))
        source_records = self._produced_from_offset(offsets.get(SOURCE_PROGRESS_TOPIC[1]))

        def view(state: ExplainabilityV1State, reason: str, consumer_lag: int | None) -> ExplainabilityV1ProgressView:
            return ExplainabilityV1ProgressView(
                selected=True,
                state=state,
                reason=reason,
                upstream_records=upstream_records,
                published_records=published_records,
                consumer_lag=consumer_lag,
            )

        service_dead = self._service_dead(EXPLAINABILITY_V1_SERVICE)
        if campaign.status in {"starting", "running"}:
            if service_dead is True:
                return view("failed", "v1 worker service stopped", None)
            if service_dead is None:
                # Docker could not be observed: completeness cannot be
                # confirmed, so the state is processing, never complete.
                return view("processing", "unable to confirm v1 worker service state", None)

        if campaign.mode != "batch":
            return view("processing", "realtime campaign active: v1 keeps processing", None)

        consumer_lag = self._get_consumer_group_lag(
            EXPLAINABILITY_V1_CONSUMER_GROUP, upstream_topic
        )
        if consumer_lag is None:
            return view("processing", "unable to confirm v1 consumer lag", None)

        if upstream_records is None or source_records is None:
            return view("processing", "unable to confirm v1 upstream records", consumer_lag)
        if upstream_records < expected_records or source_records < expected_records:
            return view("processing", "waiting for HSTree upstream", consumer_lag)

        if consumer_lag > 0:
            return view("processing", f"v1 consumer lag: {consumer_lag}", consumer_lag)

        if published_records is None:
            return view("processing", "unable to confirm v1 published results", consumer_lag)
        if published_records < expected_records:
            return view("processing", f"v1 published results: {published_records}/{expected_records}", consumer_lag)

        # V1-03 commits an offset only after both Avro and JSON deliveries
        # are confirmed: lag 0 plus complete published outputs implies the
        # contiguous commits happened. Operational completeness is at least
        # once; a replay after a crash may duplicate outputs (detectable via
        # source_event_id), never silently lose them.
        return view("complete", "v1 delivery and commit confirmed for all expected records", consumer_lag)

    def get_explainability_v1_progress(self, campaign: CampaignView | None) -> ExplainabilityV1ProgressView:
        if campaign is None or EXPLAINABILITY_V1_MODEL not in campaign.selected_models:
            return ExplainabilityV1ProgressView()
        try:
            offsets = self._get_topic_offsets(
                [_explainability_upstream_topic(campaign, EXPLAINABILITY_V1_MODEL), EXPLAINABILITY_V1_OUTPUT_TOPIC, SOURCE_PROGRESS_TOPIC[1]]
            )
        except Exception as exc:
            logging.warning("Unable to compute explainability-v1 progress: %s", exc)
            offsets = {}
        return self._build_explainability_v1_progress(campaign, offsets)

    @staticmethod
    def _active_progress_topics(campaign: CampaignView) -> list[tuple[str, str]]:
        topics: list[tuple[str, str]] = []
        seen: set[str] = set()

        for model in campaign.selected_models:
            topic = MODEL_PROGRESS_TOPICS.get(model)
            if topic is None:
                continue
            _, topic_name = topic
            if topic_name in seen:
                continue
            topics.append(topic)
            seen.add(topic_name)

        topics.append(SOURCE_PROGRESS_TOPIC)
        return topics

    def get_campaign_progress(self, campaign: CampaignView | None) -> CampaignProgressView:
        if campaign is None:
            return CampaignProgressView(
                explainability_v2=ExplainabilityV2ProgressView(),
                explainability_v1=ExplainabilityV1ProgressView(),
            )

        total_records = self._count_dataset_rows(campaign.dataset_path)
        progress_topics = self._active_progress_topics(campaign)
        items: list[ProgressItemView] = []
        offsets: dict[str, int | None] = {}
        try:
            offsets = self._get_topic_offsets([topic for _, topic in progress_topics])
            for label, topic in progress_topics:
                last_offset = offsets.get(topic)
                produced_records = 0 if last_offset is None or last_offset < 0 else last_offset + 1
                items.append(
                    ProgressItemView(
                        label=label,
                        topic=topic,
                        produced_records=produced_records,
                        total_records=total_records,
                        progress_percent=self._progress_percent(produced_records, total_records),
                    )
                )
        except Exception as exc:
            logging.warning("Unable to compute Kafka progress through docker compose: %s", exc)
            for label, topic in progress_topics:
                items.append(
                    ProgressItemView(
                        label=label,
                        topic=topic,
                        produced_records=0,
                        total_records=total_records,
                        progress_percent=0.0,
                    )
                )

        return CampaignProgressView(
            dataset_path=campaign.dataset_path,
            total_records=total_records,
            items=items,
            explainability_v2=self._build_explainability_v2_progress(campaign, offsets),
            explainability_v1=self._build_explainability_v1_progress(campaign, offsets),
        )
