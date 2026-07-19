"""Fail-closed statistical and economic certification gate reports."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence, Tuple

from .certification import DerivedCertificationStatistics, derive_certification_statistics


@dataclass(frozen=True)
class GateThresholds:
    economic_buffer_rate: Decimal = Decimal("0.02")
    confidence: float = 0.95
    minimum_deflated_sharpe_probability: float = 0.95
    maximum_probability_backtest_overfit: float = 0.10
    maximum_familywise_p_value: float = 0.05
    minimum_power: float = 0.80

    def __post_init__(self) -> None:
        probabilities = (
            self.confidence,
            self.minimum_deflated_sharpe_probability,
            self.maximum_probability_backtest_overfit,
            self.maximum_familywise_p_value,
            self.minimum_power,
        )
        if self.economic_buffer_rate < 0:
            raise ValueError("economic_buffer_rate cannot be negative")
        if not all(math.isfinite(value) and 0 <= value <= 1 for value in probabilities):
            raise ValueError("gate probability thresholds must lie in [0, 1]")
        if self.confidence <= 0.5:
            raise ValueError("confidence must exceed 0.5")


@dataclass(frozen=True)
class GateEvidence:
    annual_recurring_cost: Decimal
    planned_live_capital: Decimal
    annualized_oos_return_lcb: float
    deflated_sharpe_probability: float | None
    probability_backtest_overfit: float | None
    familywise_p_value: float | None
    statistical_power: float | None
    stressed_drawdown: float
    certified_max_drawdown: float
    minimum_track_record_passed: bool
    concentration_passed: bool
    independent_reproduction_passed: bool
    data_quality_passed: bool
    sealed_holdout_passed: bool

    def __post_init__(self) -> None:
        if self.annual_recurring_cost < 0:
            raise ValueError("annual_recurring_cost cannot be negative")
        if self.planned_live_capital <= 0:
            raise ValueError("planned_live_capital must be positive")
        numeric = (
            self.annualized_oos_return_lcb,
            self.stressed_drawdown,
            self.certified_max_drawdown,
        )
        optional = (
            self.deflated_sharpe_probability,
            self.probability_backtest_overfit,
            self.familywise_p_value,
            self.statistical_power,
        )
        if not all(math.isfinite(value) for value in numeric):
            raise ValueError("gate evidence values must be finite")
        if any(value is not None and not math.isfinite(value) for value in optional):
            raise ValueError("gate evidence probabilities must be finite when present")
        if any(value is not None and not 0 <= value <= 1 for value in optional):
            raise ValueError("gate evidence probabilities must lie in [0, 1]")
        if not 0 <= self.stressed_drawdown <= 1 or not 0 <= self.certified_max_drawdown <= 1:
            raise ValueError("drawdown magnitudes must lie in [0, 1]")


@dataclass(frozen=True)
class GateCheck:
    name: str
    passed: bool
    actual: str
    requirement: str


@dataclass(frozen=True)
class GateReport:
    eligible: bool
    economic_hurdle: Decimal
    checks: Tuple[GateCheck, ...]

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "eligible": self.eligible,
            "economic_hurdle": format(self.economic_hurdle, "f"),
            "checks": [
                {
                    "name": check.name,
                    "passed": check.passed,
                    "actual": check.actual,
                    "requirement": check.requirement,
                }
                for check in self.checks
            ],
        }


@dataclass(frozen=True)
class DerivedGateReport:
    report: GateReport
    statistics: DerivedCertificationStatistics


def economic_hurdle(
    annual_recurring_cost: Decimal,
    planned_live_capital: Decimal,
    *,
    buffer_rate: Decimal = Decimal("0.02"),
) -> Decimal:
    if annual_recurring_cost < 0 or planned_live_capital <= 0 or buffer_rate < 0:
        raise ValueError("cost/buffer must be non-negative and capital must be positive")
    return annual_recurring_cost / planned_live_capital + buffer_rate


def _probability_check(
    name: str,
    actual: float | None,
    threshold: float,
    *,
    minimum: bool,
) -> GateCheck:
    operator = ">=" if minimum else "<="
    passed = actual is not None and (actual >= threshold if minimum else actual <= threshold)
    return GateCheck(
        name=name,
        passed=passed,
        actual="not_estimated" if actual is None else f"{actual:.8g}",
        requirement=f"{operator} {threshold:.8g}",
    )


def evaluate_gates(
    evidence: GateEvidence,
    thresholds: GateThresholds = GateThresholds(),
) -> GateReport:
    hurdle = economic_hurdle(
        evidence.annual_recurring_cost,
        evidence.planned_live_capital,
        buffer_rate=thresholds.economic_buffer_rate,
    )
    checks = (
        GateCheck(
            "economic_lower_bound",
            Decimal(str(evidence.annualized_oos_return_lcb)) > hurdle,
            f"{evidence.annualized_oos_return_lcb:.8g}",
            f"> {format(hurdle, 'f')}",
        ),
        _probability_check(
            "deflated_sharpe_probability",
            evidence.deflated_sharpe_probability,
            thresholds.minimum_deflated_sharpe_probability,
            minimum=True,
        ),
        _probability_check(
            "probability_backtest_overfit",
            evidence.probability_backtest_overfit,
            thresholds.maximum_probability_backtest_overfit,
            minimum=False,
        ),
        _probability_check(
            "familywise_test_p_value",
            evidence.familywise_p_value,
            thresholds.maximum_familywise_p_value,
            minimum=False,
        ),
        _probability_check(
            "statistical_power",
            evidence.statistical_power,
            thresholds.minimum_power,
            minimum=True,
        ),
        GateCheck(
            "stressed_drawdown",
            evidence.stressed_drawdown <= evidence.certified_max_drawdown,
            f"{evidence.stressed_drawdown:.8g}",
            f"<= {evidence.certified_max_drawdown:.8g}",
        ),
        GateCheck(
            "minimum_track_record",
            evidence.minimum_track_record_passed,
            str(evidence.minimum_track_record_passed),
            "true",
        ),
        GateCheck(
            "concentration",
            evidence.concentration_passed,
            str(evidence.concentration_passed),
            "true",
        ),
        GateCheck(
            "independent_reproduction",
            evidence.independent_reproduction_passed,
            str(evidence.independent_reproduction_passed),
            "true",
        ),
        GateCheck(
            "data_quality",
            evidence.data_quality_passed,
            str(evidence.data_quality_passed),
            "true",
        ),
        GateCheck(
            "sealed_holdout",
            evidence.sealed_holdout_passed,
            str(evidence.sealed_holdout_passed),
            "true",
        ),
    )
    return GateReport(
        eligible=all(check.passed for check in checks),
        economic_hurdle=hurdle,
        checks=checks,
    )


def evaluate_gates_from_core_outputs(
    *,
    candidate_net_returns: Iterable[float],
    candidate_excess_returns: Iterable[float],
    pbo_performance_matrix: Sequence[Sequence[float]],
    annual_recurring_cost: Decimal,
    planned_live_capital: Decimal,
    familywise_p_value: float | None,
    statistical_power: float | None,
    stressed_drawdown: float,
    certified_max_drawdown: float,
    concentration_passed: bool,
    independent_reproduction_passed: bool,
    data_quality_passed: bool,
    sealed_holdout_passed: bool,
    expected_trial_count: int = 12,
    benchmark_sharpe_per_period: float = 0,
    periods_per_year: int = 252,
    bootstrap_block_length: int | None = None,
    bootstrap_resamples: int = 5_000,
    bootstrap_seed: int = 0,
    effective_sample_max_lag: int | None = None,
    pbo_partitions: int = 8,
    thresholds: GateThresholds = GateThresholds(),
) -> DerivedGateReport:
    """Calculate return-derived evidence, then apply every promotion threshold."""

    statistics_result = derive_certification_statistics(
        candidate_net_returns=candidate_net_returns,
        candidate_excess_returns=candidate_excess_returns,
        pbo_performance_matrix=pbo_performance_matrix,
        expected_trial_count=expected_trial_count,
        benchmark_sharpe_per_period=benchmark_sharpe_per_period,
        periods_per_year=periods_per_year,
        confidence=thresholds.confidence,
        bootstrap_block_length=bootstrap_block_length,
        bootstrap_resamples=bootstrap_resamples,
        bootstrap_seed=bootstrap_seed,
        effective_sample_max_lag=effective_sample_max_lag,
        pbo_partitions=pbo_partitions,
    )
    evidence = GateEvidence(
        annual_recurring_cost=annual_recurring_cost,
        planned_live_capital=planned_live_capital,
        annualized_oos_return_lcb=(
            statistics_result.bootstrap_lcb.annualized_return_lcb
        ),
        deflated_sharpe_probability=statistics_result.deflated_sharpe.probability,
        probability_backtest_overfit=statistics_result.pbo.probability,
        familywise_p_value=familywise_p_value,
        statistical_power=statistical_power,
        stressed_drawdown=stressed_drawdown,
        certified_max_drawdown=certified_max_drawdown,
        minimum_track_record_passed=statistics_result.track_record.passes,
        concentration_passed=concentration_passed,
        independent_reproduction_passed=independent_reproduction_passed,
        data_quality_passed=data_quality_passed,
        sealed_holdout_passed=sealed_holdout_passed,
    )
    return DerivedGateReport(
        report=evaluate_gates(evidence, thresholds),
        statistics=statistics_result,
    )


def annualized_normal_mean_lcb(
    periodic_returns: Iterable[float],
    *,
    periods_per_year: int = 252,
    confidence: float = 0.95,
) -> float:
    """Diagnostic normal-approximation LCB; certification must state its assumptions.

    This utility analyzes supplied return evidence. It does not generate targets or prove
    that evidence came from Rust. Fewer than 30 observations fail closed because the
    approximation is too weak.
    """

    values = tuple(periodic_returns)
    if len(values) < 30:
        raise ValueError("at least 30 periodic returns are required")
    if periods_per_year <= 0 or not (0.5 < confidence < 1):
        raise ValueError("invalid annualization or confidence setting")
    if not all(math.isfinite(value) and value > -1 for value in values):
        raise ValueError("returns must be finite and greater than -1")
    mean = statistics.fmean(values)
    standard_error = statistics.stdev(values) / math.sqrt(len(values))
    critical = statistics.NormalDist().inv_cdf(confidence)
    return (mean - critical * standard_error) * periods_per_year
