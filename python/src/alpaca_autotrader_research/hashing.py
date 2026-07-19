"""Canonical serialization and immutable artifact provenance helpers."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping


JSON_HASH_PROFILE = "wasp-json-sha256-v1"
I128_MIN = -(1 << 127)
U128_MAX = (1 << 128) - 1


class CanonicalizationError(ValueError):
    """Raised when a value cannot be represented deterministically."""


def canonical_datetime_text(value: datetime) -> str:
    """Match Rust/Chrono RFC 3339 AutoSi precision for Python datetimes."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise CanonicalizationError("datetime values must be timezone-aware")
    utc = value.astimezone(timezone.utc)
    base = utc.strftime("%Y-%m-%dT%H:%M:%S")
    if utc.microsecond == 0:
        return base + "Z"
    if utc.microsecond % 1_000 == 0:
        return f"{base}.{utc.microsecond // 1_000:03d}Z"
    return f"{base}.{utc.microsecond:06d}Z"


def reject_duplicate_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value!r} is forbidden")


def _normalize(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        # Runtime narrowing is stronger than the public typing stub for asdict.
        return _normalize(dataclasses.asdict(value))  # type: ignore[arg-type]
    if isinstance(value, Enum):
        return _normalize(value.value)
    if isinstance(value, datetime):
        return canonical_datetime_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise CanonicalizationError("Decimal values must be finite")
        return format(value, "f")
    if isinstance(value, Mapping):
        normalized: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError("mapping keys must be strings")
            normalized[key] = _normalize(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, float):
        raise CanonicalizationError(
            "floating-point values are forbidden in canonical evidence; "
            "use a versioned integer or decimal string encoding"
        )
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        if value < I128_MIN or value > U128_MAX:
            raise CanonicalizationError(
                "integer values must fit the canonical i128/u128 domain"
            )
        return value
    raise CanonicalizationError(f"unsupported canonical value type: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Return deterministic UTF-8 JSON bytes suitable for content hashing."""

    return json.dumps(
        _normalize(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def sha256_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash an artifact without loading it into memory."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        while True:
            chunk = artifact.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def require_sha256(value: str, *, field: str = "digest") -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a lowercase 64-hex SHA-256 digest")
    return value
