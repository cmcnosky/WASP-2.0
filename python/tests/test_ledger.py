from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from alpaca_autotrader_research.ledger import (
    GENESIS_HASH,
    ExperimentLedger,
    LedgerEntry,
    LedgerIntegrityError,
    verify_entries,
)
from alpaca_autotrader_research.hashing import (
    CanonicalizationError,
    JSON_HASH_PROFILE,
    sha256_digest,
)


class LedgerTests(unittest.TestCase):
    def test_append_and_verify_hash_chain(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = ExperimentLedger(Path(directory) / "experiments.jsonl")
            first = ledger.append(
                "attempt_started",
                {"attempt": "one"},
                recorded_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            second = ledger.append(
                "attempt_failed",
                {"attempt": "one"},
                recorded_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            )
            entries = ledger.read_verified()
            self.assertEqual(2, len(entries))
            self.assertEqual(first.entry_hash, second.previous_hash)
            self.assertEqual(JSON_HASH_PROFILE, first.hash_profile)

    def test_timestamp_profile_and_float_payload_are_fail_closed(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "experiments.jsonl"
            ledger = ExperimentLedger(path)
            entry = ledger.append(
                "attempt",
                {"scaled_return": 125},
                recorded_at=datetime(
                    2026, 1, 1, 0, 0, 0, 500_000, tzinfo=timezone.utc
                ),
            )
            self.assertEqual("2026-01-01T00:00:00.500Z", entry.recorded_at)
            self.assertIn(f'"hash_profile":"{JSON_HASH_PROFILE}"', path.read_text())
            with self.assertRaises(CanonicalizationError):
                ledger.append("invalid_float", {"return": 0.1})

            original = path.read_text()
            path.write_text(original.replace(JSON_HASH_PROFILE, "wrong-profile"))
            with self.assertRaises(LedgerIntegrityError):
                ledger.read_verified()

            path.write_text(original.replace(f'"hash_profile":"{JSON_HASH_PROFILE}",', ""))
            with self.assertRaises(LedgerIntegrityError):
                ledger.read_verified()

            material = {
                "sequence": 1,
                "hash_profile": JSON_HASH_PROFILE,
                "recorded_at": "2026-01-01T00:00:00+00:00",
                "event_type": "attempt",
                "payload": {},
                "previous_hash": GENESIS_HASH,
            }
            forged = LedgerEntry(
                sequence=1,
                hash_profile=JSON_HASH_PROFILE,
                recorded_at="2026-01-01T00:00:00+00:00",
                event_type="attempt",
                payload={},
                previous_hash=GENESIS_HASH,
                entry_hash=sha256_digest(material),
            )
            with self.assertRaises(LedgerIntegrityError):
                verify_entries([forged])

    def test_detects_payload_tampering(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "experiments.jsonl"
            ledger = ExperimentLedger(path)
            ledger.append("attempt", {"result": "original"})
            path.write_text(path.read_text().replace("original", "tampered"))
            with self.assertRaises(LedgerIntegrityError):
                ledger.read_verified()

    def test_rejects_noncanonical_or_extra_content(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "experiments.jsonl"
            ledger = ExperimentLedger(path)
            ledger.append("attempt", {"result": "original"})
            original = path.read_text()
            path.write_text("\n" + original)
            with self.assertRaises(LedgerIntegrityError):
                ledger.read_verified()


if __name__ == "__main__":
    unittest.main()
