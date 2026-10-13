from __future__ import annotations

import logging
import math
import unittest

import numpy as np

from dasma_kafka.explainability.config import KITNET_EXCLUDE_FIELDS
from dasma_kafka.explainability.features import KitNetFeatureExtractor as V1Extractor
from dasma_kafka.explainability_v2.config import KITNET_EXCLUDE_FIELDS as V2_EXCLUDES
from dasma_kafka.explainability_v2.features import KitNetFeatureExtractor as V2Extractor


class KitNetFeatureExtractorTest(unittest.TestCase):
    def test_v1_and_v2_keep_record_order_and_detector_conversions(self) -> None:
        record = {
            "second": "2.5",
            "source_event_id": "evt-1",
            "Group": "G1",
            "kitnet_score": 0.8,
            "first": 1,
            "nullable": None,
            "status": "not-numeric",
            "kitnet_status": "normal",
            "_partition": 3,
        }
        for extractor_type in (V1Extractor, V2Extractor):
            extractor = extractor_type(logging.getLogger("kitnet-features-test"))
            extractor.initialize_from_record(record)
            self.assertEqual(extractor.get_feature_names(), ["second", "first", "nullable", "status"])
            values = extractor.extract(record)
            self.assertEqual(values.dtype, np.float64)
            self.assertEqual(values[:2].tolist(), [2.5, 1.0])
            self.assertTrue(math.isnan(values[2]))
            self.assertTrue(math.isnan(values[3]))

    def test_excluded_fields_are_forced_for_both_versions(self) -> None:
        required = {
            "source_event_id", "timestamp", "Group", "group", "name", "time",
            "kitnet_score", "kitnet_status", "kitnet_anomaly", "kitnet_threshold",
            "_topic", "_partition", "_offset", "_key",
        }
        self.assertTrue(required.issubset(KITNET_EXCLUDE_FIELDS))
        self.assertTrue(required.issubset(V2_EXCLUDES))

    def test_feature_names_must_match_initialization(self) -> None:
        for extractor_type in (V1Extractor, V2Extractor):
            extractor = extractor_type(logging.getLogger("kitnet-features-test"))
            extractor.initialize_from_record({"a": 1.0, "b": 2.0})
            with self.assertRaisesRegex(ValueError, "feature names changed"):
                extractor.extract({"a": 1.0, "b": 2.0, "new": 3.0})
            with self.assertRaisesRegex(ValueError, "feature names changed"):
                extractor.extract({"a": 1.0})


if __name__ == "__main__":
    unittest.main()
