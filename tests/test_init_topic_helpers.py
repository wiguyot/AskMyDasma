from unittest import TestCase
from unittest.mock import patch

from dasma_kafka.init_topic.helpers import load_topics_from_env


class InitTopicHelpersTest(TestCase):
    def test_load_topics_from_env_deduplicates_by_name(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TOPIC_NAME1": "shared",
                "TOPIC_PARTITIONS1": "1",
                "TOPIC_RF1": "3",
                "TOPIC_NAME9": "shared",
                "TOPIC_PARTITIONS9": "2",
                "TOPIC_RF9": "1",
            },
            clear=False,
        ):
            topics = load_topics_from_env()

        shared = [topic for topic in topics if topic.name == "shared"]
        self.assertEqual(len(shared), 1)
        self.assertEqual(shared[0].partitions, 1)
        self.assertEqual(shared[0].rf, 3)

    def test_load_topics_from_env_includes_deepant_topics(self) -> None:
        """TOPIC_NAME13/14 must be picked up so init-topic creates the DeepAnT topics."""
        topics = load_topics_from_env()
        names = {topic.name for topic in topics}
        self.assertIn("deepant-analysis", names)
        self.assertIn("deepant-analysis-viz", names)

    def test_load_topics_from_env_includes_mdragpoint_topics(self) -> None:
        """TOPIC_NAME15/16 must be picked up, while mDragSubseq stays absent."""
        topics = load_topics_from_env()
        names = {topic.name for topic in topics}
        self.assertIn("mdragstream-point-analysis", names)
        self.assertIn("mdragstream-point-analysis-viz", names)
        self.assertNotIn("mdragstream-subseq-analysis", names)
        self.assertNotIn("mdragstream-subseq-analysis-viz", names)

    def test_load_topics_from_env_includes_repeatability_defaults(self) -> None:
        """TOPIC_NAME19-23 must default to the repeatability topic names."""
        with patch.dict("os.environ", {}, clear=True):
            topics = load_topics_from_env()

        names = {topic.name for topic in topics}
        expected = {
            "source-data",
            "source-data-viz",
            "hstree-analysis",
            "hstree-analysis-viz",
            "iforest-analysis",
            "iforest-analysis-viz",
            "kitnet-analysis",
            "kitnet-analysis-viz",
            "consensus-analysis",
            "explainability",
            "explainability-viz",
            "topic-metrics",
            "deepant-analysis",
            "deepant-analysis-viz",
            "mdragstream-point-analysis",
            "mdragstream-point-analysis-viz",
            "explainability-v2",
            "explainability-v2-viz",
            "repeatability-v1",
            "repeatability-v1-viz",
            "repeatability-v2",
            "repeatability-v2-viz",
            "repeatability-diff",
            "reproducibility-diff",
            "reproducibility-summary",
            "explainability-shap-diff",
        }
        self.assertEqual(names, expected)

    def test_load_topics_from_env_override_propagates(self) -> None:
        """Custom TOPIC_NAME19/PARTITIONS19/RF19 must override the defaults."""
        with patch.dict(
            "os.environ",
            {
                "TOPIC_NAME19": "custom-repeat-topic",
                "TOPIC_PARTITIONS19": "2",
                "TOPIC_RF19": "1",
            },
            clear=True,
        ):
            topics = load_topics_from_env()

        names = {topic.name for topic in topics}
        self.assertNotIn("repeatability-v1", names)
        matches = [topic for topic in topics if topic.name == "custom-repeat-topic"]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].partitions, 2)
        self.assertEqual(matches[0].rf, 1)

    def test_load_topics_from_env_excludes_empty_name(self) -> None:
        """An empty topic name disables the topic (existing invariant)."""
        with patch.dict("os.environ", {"TOPIC_NAME19": ""}, clear=True):
            topics = load_topics_from_env()

        names = {topic.name for topic in topics}
        self.assertNotIn("repeatability-v1", names)
        self.assertIn("repeatability-v1-viz", names)
        self.assertIn("repeatability-v2", names)
        self.assertIn("repeatability-v2-viz", names)
        self.assertIn("repeatability-diff", names)
