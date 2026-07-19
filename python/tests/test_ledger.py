from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from alpaca_autotrader_research.ledger import ExperimentLedger, LedgerIntegrityError


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
