from unittest import TestCase
from unittest.mock import patch

from dasma_kafka.config.env import get_env_bool, get_env_float, get_env_int, get_env_str


class EnvHelpersTest(TestCase):
    def test_get_env_str_strip(self) -> None:
        with patch.dict("os.environ", {"X_VALUE": "  hello  "}, clear=False):
            self.assertEqual(get_env_str("X_VALUE", strip=True), "hello")

    def test_get_env_bool(self) -> None:
        with patch.dict("os.environ", {"X_BOOL": "yes"}, clear=False):
            self.assertTrue(get_env_bool("X_BOOL", False))

    def test_get_env_int_invalid_returns_default(self) -> None:
        with patch.dict("os.environ", {"X_INT": "abc"}, clear=False):
            self.assertEqual(get_env_int("X_INT", 7), 7)

    def test_get_env_float_invalid_returns_default(self) -> None:
        with patch.dict("os.environ", {"X_FLOAT": "abc"}, clear=False):
            self.assertEqual(get_env_float("X_FLOAT", 1.5), 1.5)

