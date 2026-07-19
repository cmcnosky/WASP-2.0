from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from alpaca_autotrader_research.cli import _strict_object


class CliStrictJsonTests(unittest.TestCase):
    def test_rejects_top_level_and_nested_duplicate_keys(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            for payload in (
                '{"run_id":"one","run_id":"two"}',
                '{"outer":{"value":1,"value":2}}',
            ):
                path.write_text(payload, encoding="utf-8")
                with self.assertRaises(ValueError):
                    _strict_object(path)

    def test_enforces_serialized_byte_ceiling_before_decode(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_text('{"value":"oversized"}', encoding="utf-8")
            with self.assertRaises(ValueError):
                _strict_object(path, max_bytes=8)


if __name__ == "__main__":
    unittest.main()
