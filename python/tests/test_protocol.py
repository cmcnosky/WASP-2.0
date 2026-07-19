from datetime import date, datetime, timezone
from dataclasses import replace
import unittest

from alpaca_autotrader_research.hashing import sha256_digest
from alpaca_autotrader_research.models import ResearchStage
from alpaca_autotrader_research.protocol import (
    LOCKED_COST_MODEL,
    LOCKED_HOLDOUT_POLICY,
    LOCKED_SPLITS,
    LOCKED_VALIDATION_DESIGN,
    assert_dates_belong_to_stage,
    generate_preregistration,
    locked_configurations,
)


class ProtocolTests(unittest.TestCase):
    def test_generates_exactly_twelve_unique_locked_configurations(self) -> None:
        configurations = locked_configurations()
        self.assertEqual(12, len(configurations))
        self.assertEqual(12, len({item.configuration_id for item in configurations}))

    def test_preregistration_is_deterministic(self) -> None:
        first = generate_preregistration(
            family_id="family-v1",
            created_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            universe_manifest_hash=sha256_digest({"symbols": ["EXAMPLE"]}),
            data_snapshot_hash=sha256_digest({"snapshot": "fixture"}),
        )
        second = generate_preregistration(
            family_id="family-v1",
            created_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            universe_manifest_hash=sha256_digest({"symbols": ["EXAMPLE"]}),
            data_snapshot_hash=sha256_digest({"snapshot": "fixture"}),
        )
        self.assertEqual(first.registration_hash, second.registration_hash)

    def test_registration_hash_binds_cost_statistics_and_holdout_governance(self) -> None:
        registration = generate_preregistration(
            family_id="family-v1",
            created_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            universe_manifest_hash=sha256_digest({"symbols": ["EXAMPLE"]}),
            data_snapshot_hash=sha256_digest({"snapshot": "fixture"}),
        )
        changed_cost = replace(
            registration,
            cost_model=replace(LOCKED_COST_MODEL, half_spread_bps=6),
        )
        changed_validation = replace(
            registration,
            validation_design=replace(LOCKED_VALIDATION_DESIGN, embargo_sessions=6),
        )
        self.assertNotEqual(registration.registration_hash, changed_cost.registration_hash)
        self.assertNotEqual(registration.registration_hash, changed_validation.registration_hash)
        self.assertTrue(registration.holdout_policy.one_shot)
        self.assertEqual("operator_only", registration.holdout_policy.authority)

    def test_preregistration_rejects_weakened_or_incomplete_governance(self) -> None:
        registration = generate_preregistration(
            family_id="family-v1",
            created_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
            universe_manifest_hash=sha256_digest({"symbols": ["EXAMPLE"]}),
            data_snapshot_hash=sha256_digest({"snapshot": "fixture"}),
        )
        with self.assertRaises(ValueError):
            replace(registration, every_attempt_counts=False)
        with self.assertRaises(ValueError):
            replace(LOCKED_COST_MODEL, stress_variable_cost_multiplier_bps=19_999)
        with self.assertRaises(ValueError):
            replace(LOCKED_VALIDATION_DESIGN, maximum_pbo_bps=1_001)
        with self.assertRaises(ValueError):
            replace(LOCKED_HOLDOUT_POLICY, prohibit_retuning=False)

    def test_locked_boundaries_and_stage_leakage(self) -> None:
        self.assertEqual(date(2022, 12, 31), LOCKED_SPLITS.development.end)
        self.assertEqual(date(2023, 1, 1), LOCKED_SPLITS.validation.start)
        self.assertEqual(date(2025, 1, 1), LOCKED_SPLITS.holdout.start)
        self.assertEqual(date(2026, 6, 30), LOCKED_SPLITS.holdout.end)
        assert_dates_belong_to_stage([date(2024, 6, 1)], ResearchStage.VALIDATION)
        with self.assertRaises(ValueError):
            assert_dates_belong_to_stage([date(2025, 1, 1)], ResearchStage.VALIDATION)


if __name__ == "__main__":
    unittest.main()
