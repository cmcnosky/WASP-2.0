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


class DependenceMethod(str, Enum):
    STATIONARY_BLOCK_BOOTSTRAP = "stationary_block_bootstrap"


class MissingDataAction(str, Enum):
    QUARANTINE_SYMBOL = "quarantine_symbol"


@dataclass(frozen=True)
class CostModelAssumptions:
    """Frozen conservative execution-cost inputs, expressed without floats."""

    decision_to_arrival_latency_ms: int
    half_spread_bps: int
    adverse_slippage_bps: int
    opportunity_cost_bps: int
    non_fill_probability_bps: int
    partial_fill_probability_bps: int
    minimum_fee_cents: int
    stress_variable_cost_multiplier_bps: int
    stress_empirical_percentile_bps: int

    def __post_init__(self) -> None:
        if not 1 <= self.decision_to_arrival_latency_ms <= 60_000:
            raise ValueError("decision-to-arrival latency must be 1 through 60000 ms")
        for name in (
            "half_spread_bps",
            "adverse_slippage_bps",
            "opportunity_cost_bps",
            "non_fill_probability_bps",
            "partial_fill_probability_bps",
            "minimum_fee_cents",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.non_fill_probability_bps + self.partial_fill_probability_bps > 10_000:
            raise ValueError("non-fill and partial-fill probabilities exceed one")
        if self.stress_variable_cost_multiplier_bps < 20_000:
            raise ValueError("stress cost must be at least twice modeled variable cost")
        if not 9_500 <= self.stress_empirical_percentile_bps <= 10_000:
            raise ValueError("stress empirical percentile must be at least the 95th percentile")


@dataclass(frozen=True)
class DependenceAndValidationDesign:
    dependence_method: DependenceMethod
    purge_sessions: int
    embargo_sessions: int
    bootstrap_block_sessions: int
    bootstrap_resamples: int
    one_sided_confidence_bps: int
    deflated_sharpe_probability_bps: int
    maximum_pbo_bps: int
    familywise_alpha_bps: int

    def __post_init__(self) -> None:
        if self.purge_sessions < 252:
            raise ValueError("purge must cover the longest preregistered lookback")
        if self.embargo_sessions < 5:
            raise ValueError("embargo must contain at least five sessions")
        if not 2 <= self.bootstrap_block_sessions <= self.purge_sessions:
            raise ValueError("bootstrap block length is outside the locked dependence design")
        if self.bootstrap_resamples < 10_000:
            raise ValueError("bootstrap requires at least 10000 resamples")
        if self.one_sided_confidence_bps < 9_500:
            raise ValueError("one-sided confidence must be at least 95 percent")
        if self.deflated_sharpe_probability_bps < 9_500:
            raise ValueError("deflated Sharpe probability gate must be at least 0.95")
        if not 0 <= self.maximum_pbo_bps <= 1_000:
            raise ValueError("PBO gate must be no more than 0.10")
        if not 0 < self.familywise_alpha_bps <= 500:
            raise ValueError("familywise alpha must be no more than 0.05")


@dataclass(frozen=True)
class MissingDataPolicy:
    action: MissingDataAction
    maximum_missing_sessions_per_symbol: int
    reject_duplicate_timestamps: bool
    reject_out_of_order_timestamps: bool
    require_correction_versioning: bool

    def __post_init__(self) -> None:
        if self.maximum_missing_sessions_per_symbol != 0:
            raise ValueError("v1 permits no unresolved missing strategy sessions")
        if not all(
            (
                self.reject_duplicate_timestamps,
                self.reject_out_of_order_timestamps,
                self.require_correction_versioning,
            )
        ):
            raise ValueError("v1 missing-data policy must fail closed and version corrections")


@dataclass(frozen=True)
class EconomicPowerDesign:
    minimum_worthwhile_edge_bps: int
    target_power_bps: int
    one_sided_alpha_bps: int
    minimum_effective_observations: int
    method: str

    def __post_init__(self) -> None:
        if self.minimum_worthwhile_edge_bps <= 0:
            raise ValueError("minimum worthwhile edge must be positive")
        if self.target_power_bps < 8_000:
            raise ValueError("declared power must be at least 80 percent")
        if not 0 < self.one_sided_alpha_bps <= 500:
            raise ValueError("one-sided alpha must be no more than 0.05")
        if self.minimum_effective_observations < 30:
            raise ValueError("effective sample requirement must be at least 30")
        if not self.method.strip():
            raise ValueError("power method must be explicit")


@dataclass(frozen=True)
class ConcentrationCriteria:
    maximum_single_asset_contribution_bps: int
    maximum_single_year_contribution_bps: int
    maximum_top_five_trades_contribution_bps: int
    minimum_completed_round_trips: int

    def __post_init__(self) -> None:
        for name in (
            "maximum_single_asset_contribution_bps",
            "maximum_single_year_contribution_bps",
            "maximum_top_five_trades_contribution_bps",
        ):
            if not 0 < getattr(self, name) < 10_000:
                raise ValueError(f"{name} must be strictly between zero and one")
        if self.minimum_completed_round_trips < 30:
            raise ValueError("concentration analysis requires at least 30 round trips")


@dataclass(frozen=True)
class HoldoutUnsealingPolicy:
    authority: str
    one_shot: bool
    require_registration_hash_match: bool
    reject_family_on_failure: bool
    prohibit_retuning: bool
    require_fresh_evidence_for_new_family: bool

    def __post_init__(self) -> None:
        if self.authority != "operator_only":
            raise ValueError("holdout unsealing authority must remain operator-only")
        if not all(
            (
                self.one_shot,
                self.require_registration_hash_match,
                self.reject_family_on_failure,
                self.prohibit_retuning,
                self.require_fresh_evidence_for_new_family,
            )
        ):
            raise ValueError("holdout policy must be one-shot, hash-bound, and non-retunable")


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
    cost_model: CostModelAssumptions
    validation_design: DependenceAndValidationDesign
    missing_data_policy: MissingDataPolicy
    power_design: EconomicPowerDesign
    concentration_criteria: ConcentrationCriteria
    holdout_policy: HoldoutUnsealingPolicy
    every_attempt_counts: bool

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        if not self.protocol_version.strip() or not self.family_id.strip():
            raise ValueError("preregistration identity fields must be non-empty")
        require_sha256(self.universe_manifest_hash, field="universe_manifest_hash")
        require_sha256(self.data_snapshot_hash, field="data_snapshot_hash")
        identifiers = tuple(item.configuration_id for item in self.configurations)
        if len(identifiers) != 12 or len(set(identifiers)) != 12:
            raise ValueError("the preregistration must contain exactly 12 unique configurations")
        if not self.every_attempt_counts:
            raise ValueError("every attempted, failed, abandoned, or completed variation must count")

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
