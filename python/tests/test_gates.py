from decimal import Decimal
import math
import unittest

from alpaca_autotrader_research.gates import (
    GateEvidence,
    annualized_normal_mean_lcb,
    economic_hurdle,
    evaluate_gates,
    evaluate_gates_from_core_outputs,
)


def passing_evidence(
    *,
    probability_backtest_overfit: float | None = 0.08,
    sealed_holdout_passed: bool = True,
) -> GateEvidence:
    return GateEvidence(
        annual_recurring_cost=Decimal("1200"),
        planned_live_capital=Decimal("25000"),
        annualized_oos_return_lcb=0.08,
        deflated_sharpe_probability=0.97,
        probability_backtest_overfit=probability_backtest_overfit,
        familywise_p_value=0.04,
        statistical_power=0.85,
        stressed_drawdown=0.08,
        certified_max_drawdown=0.10,
        minimum_track_record_passed=True,
        concentration_passed=True,
        independent_reproduction_passed=True,
        data_quality_passed=True,
        sealed_holdout_passed=sealed_holdout_passed,
    )


class GateTests(unittest.TestCase):
    def test_economic_hurdle_includes_cost_and_buffer(self) -> None:
        self.assertEqual(Decimal("0.068"), economic_hurdle(Decimal("1200"), Decimal("25000")))

    def test_all_checks_must_pass(self) -> None:
        report = evaluate_gates(passing_evidence())
        self.assertTrue(report.eligible)
        self.assertTrue(all(check.passed for check in report.checks))

    def test_missing_or_failed_statistic_is_fail_closed(self) -> None:
        missing = evaluate_gates(passing_evidence(probability_backtest_overfit=None))
        failed = evaluate_gates(passing_evidence(sealed_holdout_passed=False))
        self.assertFalse(missing.eligible)
        self.assertFalse(failed.eligible)

    def test_lcb_requires_adequate_observations(self) -> None:
        with self.assertRaises(ValueError):
            annualized_normal_mean_lcb([0.001] * 29)
        self.assertAlmostEqual(0.252, annualized_normal_mean_lcb([0.001] * 30))

    def test_derived_report_calculates_return_based_gates(self) -> None:
        returns = [0.001 + 0.004 * math.sin(index * 0.71) for index in range(120)]
        matrix = [
            [
                0.0002 * (column - 4)
                + 0.004 * math.sin(row * (0.17 + column * 0.01) + column)
                for column in range(12)
            ]
            for row in range(120)
        ]
        derived = evaluate_gates_from_core_outputs(
            candidate_net_returns=returns,
            candidate_excess_returns=returns,
            pbo_performance_matrix=matrix,
            annual_recurring_cost=Decimal("0"),
            planned_live_capital=Decimal("25000"),
            familywise_p_value=0.04,
            statistical_power=0.85,
            stressed_drawdown=0.08,
            certified_max_drawdown=0.10,
            concentration_passed=True,
            independent_reproduction_passed=True,
            data_quality_passed=True,
            sealed_holdout_passed=True,
            bootstrap_resamples=1_000,
            pbo_partitions=4,
        )
        check_names = {check.name for check in derived.report.checks}
        self.assertIn("deflated_sharpe_probability", check_names)
        self.assertIn("probability_backtest_overfit", check_names)
        self.assertEqual(120, derived.statistics.bootstrap_lcb.observations)


if __name__ == "__main__":
    unittest.main()
