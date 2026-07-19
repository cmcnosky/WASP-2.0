"""Immutable protocol and provenance models for research orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping, Tuple

from .hashing import require_sha256, sha256_digest


class RebalanceCadence(str, Enum):
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class ResearchStage(str, Enum):
    DEVELOPMENT = "development"
    VALIDATION = "validation"
    HOLDOUT = "holdout"
    PROSPECTIVE_SHADOW = "prospective_shadow"


class AttemptStatus(str, Enum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    ABANDONED = "abandoned"


@dataclass(frozen=True)
class DateWindow:
    name: ResearchStage
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"{self.name.value} start must not follow its end")

    def contains(self, value: date) -> bool:
        return self.start <= value <= self.end


@dataclass(frozen=True)
class ChronologicalSplits:
    development: DateWindow
    validation: DateWindow
    holdout: DateWindow

    def __post_init__(self) -> None:
        if self.development.name is not ResearchStage.DEVELOPMENT:
            raise ValueError("development window has the wrong stage")
        if self.validation.name is not ResearchStage.VALIDATION:
            raise ValueError("validation window has the wrong stage")
        if self.holdout.name is not ResearchStage.HOLDOUT:
            raise ValueError("holdout window has the wrong stage")
        if not (self.development.end < self.validation.start <= self.validation.end):
            raise ValueError("development and validation windows overlap or are unordered")
        if not (self.validation.end < self.holdout.start <= self.holdout.end):
            raise ValueError("validation and holdout windows overlap or are unordered")

    def classify(self, value: date) -> ResearchStage:
        for window in (self.development, self.validation, self.holdout):
            if window.contains(value):
                return window.name
        raise ValueError(f"date {value.isoformat()} is outside the locked research windows")


@dataclass(frozen=True)
class StrategyConfiguration:
    momentum_lookback_sessions: int
    trend_filter_sessions: int
    rebalance: RebalanceCadence

    def __post_init__(self) -> None:
        if self.momentum_lookback_sessions not in {63, 126, 252}:
            raise ValueError("momentum lookback is outside the preregistered family")
        if self.trend_filter_sessions not in {126, 252}:
            raise ValueError("trend filter is outside the preregistered family")

    @property
    def configuration_id(self) -> str:
        return (
            f"mom-{self.momentum_lookback_sessions:03d}_"
            f"trend-{self.trend_filter_sessions:03d}_{self.rebalance.value}"
        )


@dataclass(frozen=True)
class ProvenanceRecord:
    artifact_id: str
    content_hash: str
    source: str
    observed_at: datetime
    available_at: datetime
    feed: str
    adjustment: str
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_sha256(self.content_hash, field="content_hash")
        for name, value in (("observed_at", self.observed_at), ("available_at", self.available_at)):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if not all((self.artifact_id, self.source, self.feed, self.adjustment)):
            raise ValueError("provenance identity fields must be non-empty")

    @property
    def record_hash(self) -> str:
        return sha256_digest(self)

    def assert_available_by(self, decision_at: datetime) -> None:
        """Prove the artifact was published before a decision could consume it."""

        if decision_at.tzinfo is None or decision_at.utcoffset() is None:
            raise ValueError("decision_at must be timezone-aware")
        if self.available_at > decision_at:
            raise ValueError("artifact was not available by the decision timestamp")


@dataclass(frozen=True)
class Preregistration:
    protocol_version: str
    family_id: str
    created_at: datetime
    universe_manifest_hash: str
    data_snapshot_hash: str
    splits: ChronologicalSplits
    configurations: Tuple[StrategyConfiguration, ...]

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        require_sha256(self.universe_manifest_hash, field="universe_manifest_hash")
        require_sha256(self.data_snapshot_hash, field="data_snapshot_hash")
        identifiers = tuple(item.configuration_id for item in self.configurations)
        if len(identifiers) != 12 or len(set(identifiers)) != 12:
            raise ValueError("the preregistration must contain exactly 12 unique configurations")

    @property
    def registration_hash(self) -> str:
        return sha256_digest(self)


@dataclass(frozen=True)
class ExperimentAttempt:
    attempt_id: str
    family_id: str
    configuration_id: str
    stage: ResearchStage
    status: AttemptStatus
    recorded_at: datetime
    core_output_hash: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not all((self.attempt_id, self.family_id, self.configuration_id)):
            raise ValueError("attempt identity fields must be non-empty")
        if self.recorded_at.tzinfo is None or self.recorded_at.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")
        if self.core_output_hash is not None:
            require_sha256(self.core_output_hash, field="core_output_hash")
