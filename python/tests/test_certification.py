import math
import unittest

from alpaca_autotrader_research.certification import (
    InsufficientEvidenceError,
    deflated_sharpe_probability,
    derive_certification_statistics,
    effective_sample_size,
    expected_maximum_sharpe_hurdle,
    minimum_track_record_length,
    moving_block_bootstrap_annualized_lcb,
    probabilistic_sharpe_ratio,
    probability_of_backtest_overfitting,
    return_moments,
    track_record_diagnostics,
)


def positive_returns(observations: int = 120) -> list[float]:
    return [0.001 + 0.004 * math.sin(index * 0.71) for index in range(observations)]


class SharpeCertificationTests(unittest.TestCase):
    def test_probabilistic_sharpe_uses_higher_moments(self) -> None:
        returns = positive_returns()
        moments = return_moments(returns)
        probability = probabilistic_sharpe_ratio(returns)
        self.assertGreater(moments.kurtosis, 1)
        self.assertGreater(probability, 0.95)

    def test_deflated_sharpe_applies_positive_trial_hurdle(self) -> None:
        trial_sharpes = [-0.02, 0.01, 0.015, 0.02, 0.025, 0.03]
        hurdle = expected_maximum_sharpe_hurdle(trial_sharpes)
        result = deflated_sharpe_probability(positive_returns(), trial_sharpes)
        self.assertGreater(hurdle, 0)
        self.assertEqual(hurdle, result.trial_hurdle_sharpe_per_period)
        self.assertEqual(6, result.trials)

    def test_insufficient_or_degenerate_return_samples_fail_closed(self) -> None:
        with self.assertRaises(InsufficientEvidenceError):
            probabilistic_sharpe_ratio([0.001] * 29)
        with self.assertRaises(InsufficientEvidenceError):
            probabilistic_sharpe_ratio([0.001] * 30)


class TrackRecordTests(unittest.TestCase):
    def test_effective_sample_never_exceeds_observations(self) -> None:
        returns = positive_returns()
        effective, _ = effective_sample_size(returns)
        self.assertGreater(effective, 0)
        self.assertLessEqual(effective, len(returns))

    def test_minimum_track_record_and_diagnostics(self) -> None:
        returns = positive_returns(240)
        result = track_record_diagnostics(returns, benchmark_sharpe_per_period=0)
        moments = return_moments(returns)
        required = minimum_track_record_length(
            observed_sharpe_per_period=moments.sharpe_per_period,
            benchmark_sharpe_per_period=0,
            skewness=moments.skewness,
            kurtosis=moments.kurtosis,
        )
        self.assertEqual(required, result.required_effective_observations)
        self.assertGreaterEqual(result.estimated_required_observations, required)


class BootstrapAndPboTests(unittest.TestCase):
    def test_moving_block_bootstrap_is_deterministic(self) -> None:
        returns = positive_returns()
        first = moving_block_bootstrap_annualized_lcb(
            returns, block_length=5, resamples=1_000, seed=734
        )
        second = moving_block_bootstrap_annualized_lcb(
            returns, block_length=5, resamples=1_000, seed=734
        )
        self.assertEqual(first, second)

    def test_pbo_from_chronological_performance_matrix(self) -> None:
        matrix = [
            [
                0.001 + 0.003 * math.sin(row * 0.37),
                0.0005 + 0.004 * math.cos(row * 0.23),
                -0.0002 + 0.005 * math.sin(row * 0.51 + 1),
            ]
            for row in range(80)
        ]
        result = probability_of_backtest_overfitting(matrix, partitions=4)
        self.assertGreaterEqual(result.probability, 0)
        self.assertLessEqual(result.probability, 1)
        self.assertEqual(6, result.combinations_evaluated)

    def test_pbo_rejects_too_little_evidence(self) -> None:
        with self.assertRaises(InsufficientEvidenceError):
            probability_of_backtest_overfitting([[0.1, 0.2]] * 20, partitions=4)

    def test_combined_statistics_are_derived_from_returns(self) -> None:
        returns = positive_returns(120)
        matrix = [
            [
                0.0002 * (column - 4)
                + 0.004 * math.sin(row * (0.17 + column * 0.01) + column)
                for column in range(12)
            ]
            for row in range(120)
        ]
        result = derive_certification_statistics(
            candidate_net_returns=returns,
            candidate_excess_returns=returns,
            pbo_performance_matrix=matrix,
            bootstrap_resamples=1_000,
            pbo_partitions=4,
        )
        self.assertEqual(120, result.deflated_sharpe.observations)
        self.assertEqual(6, result.pbo.combinations_evaluated)
        self.assertEqual(12, len(result.trial_sharpes_per_period))

    def test_combined_statistics_require_every_preregistered_trial(self) -> None:
        returns = positive_returns(120)
        incomplete_matrix = [[returns[row], returns[row] * 0.5] for row in range(120)]
        with self.assertRaises(InsufficientEvidenceError):
            derive_certification_statistics(
                candidate_net_returns=returns,
                candidate_excess_returns=returns,
                pbo_performance_matrix=incomplete_matrix,
                bootstrap_resamples=1_000,
                pbo_partitions=4,
            )


if __name__ == "__main__":
    unittest.main()
