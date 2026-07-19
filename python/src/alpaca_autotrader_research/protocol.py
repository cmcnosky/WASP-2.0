"""Locked first-family research protocol and chronological access checks."""

from __future__ import annotations

from datetime import date, datetime
from itertools import product
from typing import Iterable, Tuple

from .models import (
    ChronologicalSplits,
    DateWindow,
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
