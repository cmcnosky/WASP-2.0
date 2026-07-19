from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from alpaca_autotrader_research.hashing import (
    JSON_HASH_PROFILE,
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
        with self.assertRaises(CanonicalizationError):
            canonical_json_text({"value": 0.5})

    def test_datetime_uses_rust_rfc3339_autosi_precision(self) -> None:
        self.assertEqual(
            '{"at":"2026-01-01T00:00:00.500Z"}',
            canonical_json_text(
                {
                    "at": datetime(
                        2026, 1, 1, 0, 0, 0, 500_000, tzinfo=timezone.utc
                    )
                }
            ),
        )
        self.assertEqual(
            '{"at":"2026-01-01T00:00:00.000500Z"}',
            canonical_json_text(
                {
                    "at": datetime(
                        2026, 1, 1, 0, 0, 0, 500, tzinfo=timezone.utc
                    )
                }
            ),
        )

    def test_v1_golden_vector_matches_rust(self) -> None:
        self.assertEqual("wasp-json-sha256-v1", JSON_HASH_PROFILE)
        vector = {
            "array": [3, 2, 1],
            "fixed_scaled": -1_234_567,
            "integer_max": 170141183460469231731687303715884105727,
            "integer_min": -170141183460469231731687303715884105728,
            "nested": {"a": "é", "z": True},
            "timestamp_micro": "2026-01-01T00:00:00.000500Z",
            "timestamp_milli": "2026-01-01T00:00:00.500Z",
            "timestamp_zero": "2026-01-01T00:00:00Z",
        }
        self.assertEqual(
            "0bb9dbc312710da164d2837c9c00edb4067cb6e57df7fafefd19e3f74723f198",
            sha256_digest(vector),
        )
        self.assertIsInstance(canonical_json_text({"value": True}), str)
        self.assertIsInstance(canonical_json_text({"value": -(1 << 127)}), str)
        self.assertIsInstance(canonical_json_text({"value": (1 << 128) - 1}), str)
        with self.assertRaises(CanonicalizationError):
            canonical_json_text({"value": -(1 << 127) - 1})
        with self.assertRaises(CanonicalizationError):
            canonical_json_text({"value": 1 << 128})

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
