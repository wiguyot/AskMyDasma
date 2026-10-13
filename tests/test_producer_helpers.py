import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from dasma_kafka.producer.helpers import (
    apply_group_filter,
    cast_value_to_avro,
    ensure_source_event_id_column,
    parse_selected_groups,
    resolve_data_source_path,
    sanitize_avro_field_name,
)


class ProducerHelpersTest(TestCase):
    def test_sanitize_avro_field_name_handles_invalid_and_duplicate_names(self) -> None:
        used: set[str] = set()
        self.assertEqual(sanitize_avro_field_name(" 123 a+b ", used), "f_123_a_b")
        self.assertEqual(sanitize_avro_field_name("123 a+b", used), "f_123_a_b_1")

    def test_cast_value_to_avro(self) -> None:
        self.assertEqual(cast_value_to_avro("12", "long"), 12)
        self.assertEqual(cast_value_to_avro("1.5", "double"), 1.5)
        self.assertEqual(cast_value_to_avro("", "string"), "")

    def test_parse_selected_groups_splits_comma_separated_values(self) -> None:
        self.assertEqual(parse_selected_groups("1A, Sortie CTA,,NETBK "), ["1A", "Sortie CTA", "NETBK"])

    def test_ensure_source_event_id_column_adds_stable_ids_after_timestamp(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"timestamp": ["t1", "t2"], "Group": ["G1", "G1"], "value": [1, 2]})

        result = ensure_source_event_id_column(df)

        self.assertEqual(list(result.columns), ["timestamp", "source_event_id", "Group", "value"])
        self.assertEqual(result["source_event_id"].tolist(), ["event-000000000001", "event-000000000002"])

    def test_ensure_source_event_id_column_preserves_existing_values_as_strings(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"Group": ["G1", "G1"], "source_event_id": [10, 11], "value": [1, 2]})

        result = ensure_source_event_id_column(df)

        self.assertEqual(list(result.columns), ["source_event_id", "Group", "value"])
        self.assertEqual(result["source_event_id"].tolist(), ["10", "11"])

    def test_apply_group_filter_keeps_only_selected_groups(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"Group": ["Sortie CTA", "1A", "Sortie CTA"], "value": [1, 2, 3]})

        filtered, group_column, row_count = apply_group_filter(df, ["Sortie CTA"])

        self.assertEqual(group_column, "Group")
        self.assertEqual(row_count, 2)
        self.assertEqual(filtered["Group"].tolist(), ["Sortie CTA", "Sortie CTA"])
        self.assertEqual(filtered["value"].tolist(), [1, 3])

    def test_apply_group_filter_empty_selection_keeps_all_rows(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"Group": ["Sortie CTA", "1A"], "value": [1, 2]})

        filtered, group_column, row_count = apply_group_filter(df, [])

        self.assertIs(filtered, df)
        self.assertIsNone(group_column)
        self.assertEqual(row_count, 2)

    def test_apply_group_filter_accepts_lowercase_group_column(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"group": ["Sortie CTA", "1A"], "value": [1, 2]})

        filtered, group_column, row_count = apply_group_filter(df, ["1A"])

        self.assertEqual(group_column, "group")
        self.assertEqual(row_count, 1)
        self.assertEqual(filtered["value"].tolist(), [2])

    def test_apply_group_filter_requires_group_column_when_selection_is_set(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"value": [1, 2]})

        with self.assertRaisesRegex(ValueError, "no Group/group column"):
            apply_group_filter(df, ["Sortie CTA"])

    def test_apply_group_filter_rejects_selection_without_matching_rows(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"Group": ["1A", "2B"], "value": [1, 2]})

        with self.assertRaisesRegex(ValueError, "matched no rows"):
            apply_group_filter(df, ["Sortie CTA"])

    def test_resolve_data_source_path_prefers_existing_candidate(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_root = root / "repo"
            script_dir = repo_root / "producer"
            repo_root.mkdir()
            script_dir.mkdir()
            csv_path = repo_root / "data.csv"
            csv_path.write_text("a\n1\n")

            resolved, candidates = resolve_data_source_path("data.csv", repo_root, script_dir)

            self.assertEqual(resolved, csv_path.resolve())
            self.assertEqual(len(candidates), 3)
