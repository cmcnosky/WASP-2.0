"""Stdlib-only certification statistics over candidate return evidence.

None of these functions creates signals, portfolios, risk decisions, or orders. They
analyze supplied values and fail closed when the sample is too small or degenerate for
the requested calculation. A later promotion path must prove that those values came from
immutable Rust outputs; the current decision replay produces no performance evidence.
Sharpe-family inputs must be net periodic excess returns. Economic-return inputs must be
net after-cost portfolio returns.
"""

from __future__ import annotations

import itertools
import math
import random
import statistics
from dataclasses import dataclass
from typing import Iterable, Sequence


MINIMUM_RETURN_OBSERVATIONS = 30
MINIMUM_BOOTSTRAP_OBSERVATIONS = 60
EULER_MASCHERONI = 0.5772156649015329


class InsufficientEvidenceError(ValueError):
    """The supplied research evidence cannot support the requested statistic."""


@dataclass(frozen=True)
class ReturnMoments:
    observations: int
    mean: float
    standard_deviation: float
    sharpe_per_period: float
    skewness: float
    kurtosis: float


@dataclass(frozen=True)
class DeflatedSharpeResult:
    probability: float
    observed_sharpe_per_period: float
    trial_hurdle_sharpe_per_period: float
    skewness: float
    kurtosis: float
    observations: int
    trials: int


@dataclass(frozen=True)
class TrackRecordResult:
    passes: bool
    observations: int
    effective_observations: float
    required_effective_observations: int
    estimated_required_observations: int
    max_lag: int


@dataclass(frozen=True)
class BootstrapLcbResult:
    annualized_return_lcb: float
    confidence: float
    resamples: int
    block_length: int
    seed: int
    observations: int


@dataclass(frozen=True)
class PboResult:
    probability: float
    logits: tuple[float, ...]
    combinations_evaluated: int
    partitions: int
    strategies: int


@dataclass(frozen=True)
class DerivedCertificationStatistics:
    deflated_sharpe: DeflatedSharpeResult
    track_record: TrackRecordResult
    bootstrap_lcb: BootstrapLcbResult
    pbo: PboResult
    trial_sharpes_per_period: tuple[float, ...]


def _returns(values: Iterable[float], *, minimum: int) -> tuple[float, ...]:
    result = tuple(float(value) for value in values)
    if len(result) < minimum:
        raise InsufficientEvidenceError(
            f"at least {minimum} return observations are required; found {len(result)}"
        )
    if not all(math.isfinite(value) and value >= -1 for value in result):
        raise ValueError("returns must be finite and no less than -1")
    return result


def return_moments(values: Iterable[float]) -> ReturnMoments:
    """Estimate Sharpe, adjusted sample skewness, and Pearson kurtosis."""

    returns = _returns(values, minimum=MINIMUM_RETURN_OBSERVATIONS)
    count = len(returns)
    mean = statistics.fmean(returns)
    standard_deviation = statistics.stdev(returns)
    if standard_deviation <= 0:
        raise InsufficientEvidenceError("return variance must be positive")

    centered = tuple(value - mean for value in returns)
    second_moment = statistics.fmean(value**2 for value in centered)
    third_moment = statistics.fmean(value**3 for value in centered)
    fourth_moment = statistics.fmean(value**4 for value in centered)
    if second_moment <= 0:
        raise InsufficientEvidenceError("return variance must be positive")

    biased_skew = third_moment / second_moment**1.5
    skewness = math.sqrt(count * (count - 1)) / (count - 2) * biased_skew
    excess_kurtosis = fourth_moment / second_moment**2 - 3
    adjusted_excess = (
        (count - 1)
        / ((count - 2) * (count - 3))
        * ((count + 1) * excess_kurtosis + 6)
    )
    kurtosis = adjusted_excess + 3
    return ReturnMoments(
        observations=count,
        mean=mean,
        standard_deviation=standard_deviation,
        sharpe_per_period=mean / standard_deviation,
        skewness=skewness,
        kurtosis=kurtosis,
    )


def probabilistic_sharpe_ratio(
    values: Iterable[float],
    *,
    benchmark_sharpe_per_period: float = 0,
) -> float:
    """Probability that observed Sharpe exceeds a same-frequency benchmark.

    Implements the skewness/kurtosis-adjusted Probabilistic Sharpe Ratio. The
    benchmark must use the same observation frequency as the supplied net excess
    returns. This function does not fetch or subtract a risk-free rate.
    """

    if not math.isfinite(benchmark_sharpe_per_period):
        raise ValueError("benchmark Sharpe must be finite")
    moments = return_moments(values)
    variance_term = (
        1
        - moments.skewness * moments.sharpe_per_period
        + (moments.kurtosis - 1) / 4 * moments.sharpe_per_period**2
    )
    if not math.isfinite(variance_term) or variance_term <= 0:
        raise InsufficientEvidenceError("Sharpe sampling variance is not positive")
    statistic = (
        (moments.sharpe_per_period - benchmark_sharpe_per_period)
        * math.sqrt(moments.observations - 1)
        / math.sqrt(variance_term)
    )
    return statistics.NormalDist().cdf(statistic)


def expected_maximum_sharpe_hurdle(trial_sharpes_per_period: Iterable[float]) -> float:
    """Expected maximum Sharpe under multiple independent selection attempts."""

    sharpes = tuple(float(value) for value in trial_sharpes_per_period)
    if len(sharpes) < 2:
        raise InsufficientEvidenceError("at least two recorded trial Sharpes are required")
    if not all(math.isfinite(value) for value in sharpes):
        raise ValueError("trial Sharpes must be finite")
    trial_deviation = statistics.stdev(sharpes)
    if trial_deviation == 0:
        return 0.0
    trial_count = len(sharpes)
    normal = statistics.NormalDist()
    first_quantile = normal.inv_cdf(1 - 1 / trial_count)
    second_quantile = normal.inv_cdf(1 - 1 / (trial_count * math.e))
    return trial_deviation * (
        (1 - EULER_MASCHERONI) * first_quantile
        + EULER_MASCHERONI * second_quantile
    )


def deflated_sharpe_probability(
    candidate_returns: Iterable[float],
    trial_sharpes_per_period: Iterable[float],
) -> DeflatedSharpeResult:
    """Deflate candidate Sharpe for non-normal returns and recorded trial count."""

    returns = _returns(candidate_returns, minimum=MINIMUM_RETURN_OBSERVATIONS)
    trial_sharpes = tuple(trial_sharpes_per_period)
    hurdle = expected_maximum_sharpe_hurdle(trial_sharpes)
    moments = return_moments(returns)
    probability = probabilistic_sharpe_ratio(
        returns,
        benchmark_sharpe_per_period=hurdle,
    )
    return DeflatedSharpeResult(
        probability=probability,
        observed_sharpe_per_period=moments.sharpe_per_period,
        trial_hurdle_sharpe_per_period=hurdle,
        skewness=moments.skewness,
        kurtosis=moments.kurtosis,
        observations=moments.observations,
        trials=len(trial_sharpes),
    )


def effective_sample_size(
    values: Iterable[float],
    *,
    max_lag: int | None = None,
) -> tuple[float, int]:
    """Conservative Bartlett-weighted effective sample size for serial returns."""

    returns = _returns(values, minimum=MINIMUM_RETURN_OBSERVATIONS)
    observations = len(returns)
    selected_lag = (
        max(1, min(observations // 4, round(observations ** (1 / 3))))
        if max_lag is None
        else max_lag
    )
    if selected_lag < 1 or selected_lag >= observations:
        raise ValueError("max_lag must be between 1 and observations - 1")
    mean = statistics.fmean(returns)
    deviations = tuple(value - mean for value in returns)
    total_variation = sum(value**2 for value in deviations)
    if total_variation <= 0:
        raise InsufficientEvidenceError("return variance must be positive")

    inflation = 1.0
    for lag in range(1, selected_lag + 1):
        covariance = sum(
            deviations[index] * deviations[index - lag]
            for index in range(lag, observations)
        )
        autocorrelation = covariance / total_variation
        bartlett_weight = 1 - lag / (selected_lag + 1)
        inflation += 2 * bartlett_weight * autocorrelation
    # Never grant more evidence than the actual number of observations merely
    # because estimated autocorrelation is negative.
    conservative_inflation = max(1.0, inflation)
    return observations / conservative_inflation, selected_lag


def minimum_track_record_length(
    *,
    observed_sharpe_per_period: float,
    benchmark_sharpe_per_period: float,
    skewness: float,
    kurtosis: float,
    confidence: float = 0.95,
) -> int:
    """Minimum independent observations needed for probabilistic Sharpe confidence."""

    inputs = (
        observed_sharpe_per_period,
        benchmark_sharpe_per_period,
        skewness,
        kurtosis,
        confidence,
    )
    if not all(math.isfinite(value) for value in inputs):
        raise ValueError("track-record inputs must be finite")
    if not 0.5 < confidence < 1:
        raise ValueError("confidence must lie strictly between 0.5 and 1")
    excess_sharpe = observed_sharpe_per_period - benchmark_sharpe_per_period
    if excess_sharpe <= 0:
        raise InsufficientEvidenceError("observed Sharpe does not exceed its benchmark")
    variance_term = (
        1
        - skewness * observed_sharpe_per_period
        + (kurtosis - 1) / 4 * observed_sharpe_per_period**2
    )
    if variance_term <= 0:
        raise InsufficientEvidenceError("Sharpe sampling variance is not positive")
    critical = statistics.NormalDist().inv_cdf(confidence)
    return math.ceil(1 + variance_term * (critical / excess_sharpe) ** 2)


def track_record_diagnostics(
    values: Iterable[float],
    *,
    benchmark_sharpe_per_period: float,
    confidence: float = 0.95,
    max_lag: int | None = None,
) -> TrackRecordResult:
    returns = _returns(values, minimum=MINIMUM_RETURN_OBSERVATIONS)
    moments = return_moments(returns)
    effective, selected_lag = effective_sample_size(returns, max_lag=max_lag)
    required = minimum_track_record_length(
        observed_sharpe_per_period=moments.sharpe_per_period,
        benchmark_sharpe_per_period=benchmark_sharpe_per_period,
        skewness=moments.skewness,
        kurtosis=moments.kurtosis,
        confidence=confidence,
    )
    estimated_required = math.ceil(required * len(returns) / effective)
    return TrackRecordResult(
        passes=effective >= required,
        observations=len(returns),
        effective_observations=effective,
        required_effective_observations=required,
        estimated_required_observations=estimated_required,
        max_lag=selected_lag,
    )


def moving_block_bootstrap_annualized_lcb(
    values: Iterable[float],
    *,
    periods_per_year: int = 252,
    confidence: float = 0.95,
    block_length: int | None = None,
    resamples: int = 5_000,
    seed: int = 0,
) -> BootstrapLcbResult:
    """Deterministic moving-block percentile LCB for annualized arithmetic return."""

    returns = _returns(values, minimum=MINIMUM_BOOTSTRAP_OBSERVATIONS)
    observations = len(returns)
    selected_block = (
        max(2, round(observations ** (1 / 3)))
        if block_length is None
        else block_length
    )
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    if not 0.5 < confidence < 1:
        raise ValueError("confidence must lie strictly between 0.5 and 1")
    if selected_block < 2 or selected_block > observations // 2:
        raise ValueError("block_length must be between 2 and half the sample")
    if resamples < 1_000:
        raise InsufficientEvidenceError("at least 1,000 bootstrap resamples are required")

    generator = random.Random(seed)
    maximum_start = observations - selected_block
    annualized_means: list[float] = []
    for _ in range(resamples):
        sample: list[float] = []
        while len(sample) < observations:
            start = generator.randint(0, maximum_start)
            sample.extend(returns[start : start + selected_block])
        annualized_means.append(statistics.fmean(sample[:observations]) * periods_per_year)
    annualized_means.sort()
    lower_tail = 1 - confidence
    index = max(0, math.ceil(lower_tail * resamples) - 1)
    return BootstrapLcbResult(
        annualized_return_lcb=annualized_means[index],
        confidence=confidence,
        resamples=resamples,
        block_length=selected_block,
        seed=seed,
        observations=observations,
    )


def _column_sharpes(
    matrix: Sequence[Sequence[float]],
    row_indices: Sequence[int],
) -> tuple[float, ...]:
    strategy_count = len(matrix[0])
    result: list[float] = []
    for column in range(strategy_count):
        values = [matrix[row][column] for row in row_indices]
        deviation = statistics.stdev(values)
        if deviation <= 0:
            raise InsufficientEvidenceError(
                "each strategy must have positive variance in every PBO fold"
            )
        result.append(statistics.fmean(values) / deviation)
    return tuple(result)


def _average_ranks(values: Sequence[float]) -> tuple[float, ...]:
    indexed = sorted((value, index) for index, value in enumerate(values))
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(indexed):
        end = cursor + 1
        while end < len(indexed) and indexed[end][0] == indexed[cursor][0]:
            end += 1
        average_rank = ((cursor + 1) + end) / 2
        for _, original_index in indexed[cursor:end]:
            ranks[original_index] = average_rank
        cursor = end
    return tuple(ranks)


def probability_of_backtest_overfitting(
    performance_matrix: Sequence[Sequence[float]],
    *,
    partitions: int = 8,
) -> PboResult:
    """Estimate PBO using combinatorially symmetric cross-validation (CSCV).

    Rows are chronological net excess return observations and columns are every recorded
    strategy configuration. The matrix must come from development/validation evidence,
    never the sealed holdout. Each fold selects the best in-sample Sharpe and ranks that
    same configuration out of sample. Tied in-sample winners use the worst out-of-sample
    rank, a deliberately conservative choice.
    """

    if partitions < 4 or partitions > 16 or partitions % 2:
        raise ValueError("partitions must be an even integer between 4 and 16")
    rows = tuple(tuple(float(value) for value in row) for row in performance_matrix)
    if len(rows) < max(MINIMUM_BOOTSTRAP_OBSERVATIONS, partitions * 4):
        raise InsufficientEvidenceError("PBO requires at least 60 rows and four per partition")
    strategy_count = len(rows[0]) if rows else 0
    if strategy_count < 2:
        raise InsufficientEvidenceError("PBO requires at least two strategy configurations")
    if any(len(row) != strategy_count for row in rows):
        raise ValueError("performance matrix must be rectangular")
    if not all(math.isfinite(value) and value >= -1 for row in rows for value in row):
        raise ValueError("performance matrix returns must be finite and no less than -1")

    base_size, remainder = divmod(len(rows), partitions)
    partition_rows: list[tuple[int, ...]] = []
    start = 0
    for partition in range(partitions):
        size = base_size + (1 if partition < remainder else 0)
        partition_rows.append(tuple(range(start, start + size)))
        start += size

    logits: list[float] = []
    partition_ids = tuple(range(partitions))
    for in_sample_ids in itertools.combinations(partition_ids, partitions // 2):
        in_sample_set = set(in_sample_ids)
        in_sample_rows = tuple(
            row for partition in in_sample_ids for row in partition_rows[partition]
        )
        out_sample_rows = tuple(
            row
            for partition in partition_ids
            if partition not in in_sample_set
            for row in partition_rows[partition]
        )
        in_sample_scores = _column_sharpes(rows, in_sample_rows)
        out_sample_scores = _column_sharpes(rows, out_sample_rows)
        best_score = max(in_sample_scores)
        selected = tuple(
            index for index, score in enumerate(in_sample_scores) if score == best_score
        )
        out_sample_ranks = _average_ranks(out_sample_scores)
        selected_rank = min(out_sample_ranks[index] for index in selected)
        relative_rank = selected_rank / (strategy_count + 1)
        logits.append(math.log(relative_rank / (1 - relative_rank)))

    probability = sum(value <= 0 for value in logits) / len(logits)
    return PboResult(
        probability=probability,
        logits=tuple(logits),
        combinations_evaluated=len(logits),
        partitions=partitions,
        strategies=strategy_count,
    )


def derive_certification_statistics(
    *,
    candidate_net_returns: Iterable[float],
    candidate_excess_returns: Iterable[float],
    pbo_performance_matrix: Sequence[Sequence[float]],
    expected_trial_count: int = 12,
    benchmark_sharpe_per_period: float = 0,
    periods_per_year: int = 252,
    confidence: float = 0.95,
    bootstrap_block_length: int | None = None,
    bootstrap_resamples: int = 5_000,
    bootstrap_seed: int = 0,
    effective_sample_max_lag: int | None = None,
    pbo_partitions: int = 8,
) -> DerivedCertificationStatistics:
    """Derive statistics from values a caller must separately bind to Rust outputs."""

    net_returns = _returns(candidate_net_returns, minimum=MINIMUM_BOOTSTRAP_OBSERVATIONS)
    excess_returns = _returns(
        candidate_excess_returns,
        minimum=MINIMUM_BOOTSTRAP_OBSERVATIONS,
    )
    if len(net_returns) != len(excess_returns):
        raise ValueError("net and excess candidate returns must be timestamp-aligned")
    matrix = tuple(tuple(float(value) for value in row) for row in pbo_performance_matrix)
    if expected_trial_count < 2:
        raise ValueError("expected_trial_count must be at least two")
    if not matrix or any(len(row) != expected_trial_count for row in matrix):
        raise InsufficientEvidenceError(
            f"certification requires all {expected_trial_count} preregistered trial columns"
        )
    trial_sharpes = _column_sharpes(matrix, tuple(range(len(matrix))))
    return DerivedCertificationStatistics(
        deflated_sharpe=deflated_sharpe_probability(excess_returns, trial_sharpes),
        track_record=track_record_diagnostics(
            excess_returns,
            benchmark_sharpe_per_period=benchmark_sharpe_per_period,
            confidence=confidence,
            max_lag=effective_sample_max_lag,
        ),
        bootstrap_lcb=moving_block_bootstrap_annualized_lcb(
            net_returns,
            periods_per_year=periods_per_year,
            confidence=confidence,
            block_length=bootstrap_block_length,
            resamples=bootstrap_resamples,
            seed=bootstrap_seed,
        ),
        pbo=probability_of_backtest_overfitting(
            matrix,
            partitions=pbo_partitions,
        ),
        trial_sharpes_per_period=trial_sharpes,
    )
