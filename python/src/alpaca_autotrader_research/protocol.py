"""Locked first-family research protocol and chronological access checks."""

from __future__ import annotations

from datetime import date, datetime
from itertools import product
from typing import Iterable, Tuple

from .models import (
    ChronologicalSplits,
    ConcentrationCriteria,
    CostModelAssumptions,
    DateWindow,
    DependenceAndValidationDesign,
    DependenceMethod,
    EconomicPowerDesign,
    HoldoutUnsealingPolicy,
    MissingDataAction,
    MissingDataPolicy,
    Preregistration,
    RebalanceCadence,
    ResearchStage,
    StrategyConfiguration,
)


LOCKED_SPLITS = ChronologicalSplits(
    development=DateWindow(ResearchStage.DEVELOPMENT, date(2016, 1, 1), date(2022, 12, 31)),
    validation=DateWindow(ResearchStage.VALIDATION, date(2023, 1, 1), date(2024, 12, 31)),
    holdout=DateWindow(ResearchStage.HOLDOUT, date(2025, 1, 1), date(2026, 6, 30)),
)

LOCKED_COST_MODEL = CostModelAssumptions(
    decision_to_arrival_latency_ms=500,
    half_spread_bps=5,
    adverse_slippage_bps=5,
    opportunity_cost_bps=5,
    non_fill_probability_bps=500,
    partial_fill_probability_bps=1_000,
    minimum_fee_cents=0,
    stress_variable_cost_multiplier_bps=20_000,
    stress_empirical_percentile_bps=9_500,
)

LOCKED_VALIDATION_DESIGN = DependenceAndValidationDesign(
    dependence_method=DependenceMethod.STATIONARY_BLOCK_BOOTSTRAP,
    purge_sessions=252,
    embargo_sessions=5,
    bootstrap_block_sessions=20,
    bootstrap_resamples=10_000,
    one_sided_confidence_bps=9_500,
    deflated_sharpe_probability_bps=9_500,
    maximum_pbo_bps=1_000,
    familywise_alpha_bps=500,
)

LOCKED_MISSING_DATA_POLICY = MissingDataPolicy(
    action=MissingDataAction.QUARANTINE_SYMBOL,
    maximum_missing_sessions_per_symbol=0,
    reject_duplicate_timestamps=True,
    reject_out_of_order_timestamps=True,
    require_correction_versioning=True,
)

LOCKED_POWER_DESIGN = EconomicPowerDesign(
    minimum_worthwhile_edge_bps=200,
    target_power_bps=8_000,
    one_sided_alpha_bps=500,
    minimum_effective_observations=60,
    method="one_sample_hac_power_v1",
)

LOCKED_CONCENTRATION_CRITERIA = ConcentrationCriteria(
    maximum_single_asset_contribution_bps=5_000,
    maximum_single_year_contribution_bps=4_000,
    maximum_top_five_trades_contribution_bps=5_000,
    minimum_completed_round_trips=30,
)

LOCKED_HOLDOUT_POLICY = HoldoutUnsealingPolicy(
    authority="operator_only",
    one_shot=True,
    require_registration_hash_match=True,
    reject_family_on_failure=True,
    prohibit_retuning=True,
    require_fresh_evidence_for_new_family=True,
)


def locked_configurations() -> Tuple[StrategyConfiguration, ...]:
    values = (
        StrategyConfiguration(momentum, trend, cadence)
        for momentum, trend, cadence in product(
            (63, 126, 252),
            (126, 252),
            (RebalanceCadence.WEEKLY, RebalanceCadence.MONTHLY),
        )
    )
    return tuple(values)


def generate_preregistration(
    *,
    family_id: str,
    created_at: datetime,
    universe_manifest_hash: str,
    data_snapshot_hash: str,
) -> Preregistration:
    """Generate the complete, fixed 12-configuration experiment family."""

    return Preregistration(
        protocol_version="etf-momentum-family-v1",
        family_id=family_id,
        created_at=created_at,
        universe_manifest_hash=universe_manifest_hash,
        data_snapshot_hash=data_snapshot_hash,
        splits=LOCKED_SPLITS,
        configurations=locked_configurations(),
        cost_model=LOCKED_COST_MODEL,
        validation_design=LOCKED_VALIDATION_DESIGN,
        missing_data_policy=LOCKED_MISSING_DATA_POLICY,
        power_design=LOCKED_POWER_DESIGN,
        concentration_criteria=LOCKED_CONCENTRATION_CRITERIA,
        holdout_policy=LOCKED_HOLDOUT_POLICY,
        every_attempt_counts=True,
    )


def enforce_locked_splits(candidate: ChronologicalSplits) -> None:
    if candidate != LOCKED_SPLITS:
        raise ValueError("research windows differ from the locked v1 protocol")


def assert_dates_belong_to_stage(values: Iterable[date], stage: ResearchStage) -> None:
    """Reject leakage across development, validation, and sealed holdout boundaries."""

    if stage is ResearchStage.PROSPECTIVE_SHADOW:
        raise ValueError("prospective shadow dates require a separately timestamped manifest")
    for value in values:
        actual = LOCKED_SPLITS.classify(value)
        if actual is not stage:
            raise ValueError(
                f"date {value.isoformat()} belongs to {actual.value}, not requested {stage.value}"
            )
