from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from alpaca_autotrader_research.hashing import (
    CanonicalizationError,
    canonical_json_text,
    sha256_digest,
    sha256_file,
)
from alpaca_autotrader_research.models import ProvenanceRecord


class HashingTests(unittest.TestCase):
    def test_mapping_order_does_not_change_hash(self) -> None:
        digest = sha256_digest({"a": 1, "b": 2})
        self.assertEqual(digest, sha256_digest({"b": 2, "a": 1}))
        self.assertEqual(64, len(digest))
        self.assertTrue(all(character in "0123456789abcdef" for character in digest))

    def test_non_finite_and_naive_time_are_rejected(self) -> None:
        with self.assertRaises(CanonicalizationError):
            canonical_json_text({"value": float("nan")})
        with self.assertRaises(CanonicalizationError):
            canonical_json_text({"at": datetime(2026, 1, 1)})

    def test_file_hash_and_availability_assertion(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "artifact"
            path.write_bytes(b"immutable")
            content_hash = sha256_file(path)
        available = datetime(2026, 1, 2, tzinfo=timezone.utc)
        record = ProvenanceRecord(
            artifact_id="artifact-v1",
            content_hash=content_hash,
            source="fixture",
            observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            available_at=available,
            feed="test",
            adjustment="raw",
        )
        record.assert_available_by(available)
        with self.assertRaises(ValueError):
            record.assert_available_by(available - timedelta(seconds=1))


if __name__ == "__main__":
    unittest.main()
