"""Hash-chained append-only experiment ledger for local research runs."""

from __future__ import annotations

import fcntl
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, TextIO, Tuple

from .hashing import (
    JSON_HASH_PROFILE,
    CanonicalizationError,
    canonical_datetime_text,
    canonical_json_text,
    reject_duplicate_object_pairs,
    reject_json_constant,
    sha256_digest,
)


GENESIS_HASH = "0" * 64


class LedgerIntegrityError(RuntimeError):
    """The ledger cannot be trusted as an intact append-only sequence."""


def _require_canonical_recorded_at(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError as error:
        raise ValueError("recorded_at is not a valid timestamp") from error
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or canonical_datetime_text(parsed) != value
    ):
        raise ValueError("recorded_at is not a canonical UTC timestamp")


@dataclass(frozen=True)
class LedgerEntry:
    sequence: int
    hash_profile: str
    recorded_at: str
    event_type: str
    payload: Mapping[str, Any]
    previous_hash: str
    entry_hash: str

    def hash_material(self) -> Mapping[str, Any]:
        return {
            "sequence": self.sequence,
            "hash_profile": self.hash_profile,
            "recorded_at": self.recorded_at,
            "event_type": self.event_type,
            "payload": self.payload,
            "previous_hash": self.previous_hash,
        }

    @classmethod
    def create(
        cls,
        *,
        sequence: int,
        recorded_at: datetime,
        event_type: str,
        payload: Mapping[str, Any],
        previous_hash: str,
    ) -> "LedgerEntry":
        if not event_type:
            raise ValueError("event_type must be non-empty")
        if recorded_at.tzinfo is None or recorded_at.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")
        recorded_text = canonical_datetime_text(recorded_at)
        material: Mapping[str, Any] = {
            "sequence": sequence,
            "hash_profile": JSON_HASH_PROFILE,
            "recorded_at": recorded_text,
            "event_type": event_type,
            "payload": payload,
            "previous_hash": previous_hash,
        }
        return cls(
            sequence=sequence,
            hash_profile=JSON_HASH_PROFILE,
            recorded_at=recorded_text,
            event_type=event_type,
            payload=payload,
            previous_hash=previous_hash,
            entry_hash=sha256_digest(material),
        )


def _decode_entry(line: str, line_number: int) -> LedgerEntry:
    expected_keys = {
        "sequence",
        "hash_profile",
        "recorded_at",
        "event_type",
        "payload",
        "previous_hash",
        "entry_hash",
    }
    try:
        value = json.loads(
            line,
            object_pairs_hook=reject_duplicate_object_pairs,
            parse_constant=reject_json_constant,
        )
        if not isinstance(value, dict):
            raise TypeError("entry is not an object")
        if set(value) != expected_keys:
            raise ValueError("entry fields do not match the ledger schema")
        if (
            not isinstance(value["sequence"], int)
            or isinstance(value["sequence"], bool)
            or value["hash_profile"] != JSON_HASH_PROFILE
            or not isinstance(value["recorded_at"], str)
            or not isinstance(value["event_type"], str)
            or not isinstance(value["payload"], dict)
            or not isinstance(value["previous_hash"], str)
            or not isinstance(value["entry_hash"], str)
        ):
            raise TypeError("entry fields have invalid types")
        entry = LedgerEntry(
            sequence=value["sequence"],
            hash_profile=value["hash_profile"],
            recorded_at=value["recorded_at"],
            event_type=value["event_type"],
            payload=value["payload"],
            previous_hash=value["previous_hash"],
            entry_hash=value["entry_hash"],
        )
        _require_canonical_recorded_at(entry.recorded_at)
        if canonical_json_text(entry) != line:
            raise ValueError("entry is not in canonical serialized form")
        return entry
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise LedgerIntegrityError(f"invalid ledger entry at line {line_number}") from error


def _read_entries(ledger: TextIO) -> Tuple[LedgerEntry, ...]:
    entries: list[LedgerEntry] = []
    for number, raw_line in enumerate(ledger, start=1):
        if not raw_line.endswith("\n"):
            raise LedgerIntegrityError(f"truncated ledger entry at line {number}")
        line = raw_line[:-1]
        if not line:
            raise LedgerIntegrityError(f"blank ledger entry at line {number}")
        entries.append(_decode_entry(line, number))
    return verify_entries(entries)


def verify_entries(entries: Iterable[LedgerEntry]) -> Tuple[LedgerEntry, ...]:
    verified = tuple(entries)
    previous_hash = GENESIS_HASH
    for expected_sequence, entry in enumerate(verified, start=1):
        if entry.hash_profile != JSON_HASH_PROFILE:
            raise LedgerIntegrityError(
                f"unsupported hash profile at sequence {entry.sequence}"
            )
        if entry.sequence != expected_sequence:
            raise LedgerIntegrityError(
                f"expected sequence {expected_sequence}, found {entry.sequence}"
            )
        if entry.previous_hash != previous_hash:
            raise LedgerIntegrityError(f"broken hash chain at sequence {entry.sequence}")
        try:
            _require_canonical_recorded_at(entry.recorded_at)
            expected_hash = sha256_digest(entry.hash_material())
        except (CanonicalizationError, ValueError) as error:
            raise LedgerIntegrityError(
                f"non-canonical value at sequence {entry.sequence}"
            ) from error
        if entry.entry_hash != expected_hash:
            raise LedgerIntegrityError(f"entry hash mismatch at sequence {entry.sequence}")
        previous_hash = entry.entry_hash
    return verified


class ExperimentLedger:
    """Serialize all writers and expose append, never update or delete."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def read_verified(self) -> Tuple[LedgerEntry, ...]:
        if not self.path.exists():
            return ()
        with self.path.open("r", encoding="utf-8") as ledger:
            fcntl.flock(ledger.fileno(), fcntl.LOCK_SH)
            return _read_entries(ledger)

    def append(
        self,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        recorded_at: datetime | None = None,
    ) -> LedgerEntry:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = recorded_at or datetime.now(timezone.utc)
        descriptor = os.open(self.path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            with os.fdopen(descriptor, "r+", encoding="utf-8", closefd=False) as ledger:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                ledger.seek(0)
                entries = _read_entries(ledger)
                entry = LedgerEntry.create(
                    sequence=len(entries) + 1,
                    recorded_at=timestamp,
                    event_type=event_type,
                    payload=payload,
                    previous_hash=entries[-1].entry_hash if entries else GENESIS_HASH,
                )
                ledger.seek(0, os.SEEK_END)
                ledger.write(canonical_json_text(entry) + "\n")
                ledger.flush()
                os.fsync(descriptor)
                return entry
        finally:
            os.close(descriptor)
