use chrono::Utc;
use trader_core::{
    risk::apply_risk, AccountSnapshot, AccountStatus, DecisionSchedule, DecisionSnapshot, Fixed,
    HashDigest, Money, Price, RebalanceCadence, RiskLimitSnapshot, Symbol, TargetPortfolio,
    TargetPosition, WholeQuantity,
};

fn snapshot(cash: Money, buying_power: Money) -> DecisionSnapshot {
    let account = AccountSnapshot {
        account_fingerprint: HashDigest::sha256("account"),
        status: AccountStatus::Active,
        trading_blocked: false,
        cash,
        buying_power,
        equity: Money::from_units(1_000).unwrap(),
        day_pnl: Money::ZERO,
        drawdown: Money::ZERO,
        positions: Vec::new(),
    };
    DecisionSnapshot {
        decision_id: "decision-risk".into(),
        release_id: "release-risk".into(),
        as_of: Utc::now(),
        market_session: Utc::now().date_naive(),
        schedule: DecisionSchedule {
            eligible_cadences: vec![RebalanceCadence::Weekly],
            calendar_evidence_hash: HashDigest::sha256("calendar"),
        },
        account_snapshot_hash: HashDigest::of_json(&account).unwrap(),
        account,
        observations: Vec::new(),
        input_data_hash: HashDigest::of_json(&Vec::<u8>::new()).unwrap(),
    }
}

fn target(price: Price, quantity: u64) -> TargetPortfolio {
    TargetPortfolio {
        decision_id: "decision-risk".into(),
        release_id: "release-risk".into(),
        generated_at: Utc::now(),
        positions: vec![TargetPosition {
            symbol: Symbol::new("SPY").unwrap(),
            target_weight: Fixed::ONE,
            target_quantity: WholeQuantity::new(quantity),
            raw_reference_price: price,
            reason_codes: vec!["test".into()],
        }],
        cash_target: false,
        reason_codes: Vec::new(),
    }
}

fn limits() -> RiskLimitSnapshot {
    RiskLimitSnapshot {
        max_gross_exposure: Money::from_units(10_000).unwrap(),
        max_position_weight: Fixed::ONE,
        max_positions: 1,
        max_order_notional: Money::from_units(10_000).unwrap(),
        max_planned_loss: Money::from_units(1_000).unwrap(),
        daily_loss_limit: Money::from_units(100).unwrap(),
        hard_drawdown_limit: Money::from_units(500).unwrap(),
        planned_stop_distance_bps: 100,
        marketable_limit_band_bps: 10,
        new_positions_enabled: true,
    }
}

#[test]
fn v1_rejects_any_position_limit_other_than_one() {
    let mut limits = limits();
    limits.max_positions = 2;
    assert!(limits.validate().is_err());
}

#[test]
fn buy_quantity_uses_cash_even_when_buying_power_is_higher() {
    let snapshot = snapshot(
        Money::from_units(100).unwrap(),
        Money::from_units(1_000).unwrap(),
    );
    let decision = apply_risk(&snapshot, &target("50".parse().unwrap(), 10), &limits()).unwrap();
    assert_eq!(decision.approved_positions[0].target_quantity.get(), 1);
}

#[test]
fn marketable_limit_price_controls_order_notional_sizing() {
    let snapshot = snapshot(
        Money::from_units(1_000).unwrap(),
        Money::from_units(1_000).unwrap(),
    );
    let mut limits = limits();
    limits.max_order_notional = "100.50".parse().unwrap();
    limits.marketable_limit_band_bps = 100;
    let decision = apply_risk(&snapshot, &target("100".parse().unwrap(), 1), &limits).unwrap();
    assert_eq!(
        decision.approved_positions[0].target_quantity,
        WholeQuantity::ZERO
    );
}

#[test]
fn marketable_limit_price_controls_planned_loss_sizing() {
    let snapshot = snapshot(
        Money::from_units(1_000).unwrap(),
        Money::from_units(1_000).unwrap(),
    );
    let mut limits = limits();
    limits.max_planned_loss = "10.05".parse().unwrap();
    limits.planned_stop_distance_bps = 1_000;
    limits.marketable_limit_band_bps = 100;
    let decision = apply_risk(&snapshot, &target("100".parse().unwrap(), 1), &limits).unwrap();
    assert_eq!(
        decision.approved_positions[0].target_quantity,
        WholeQuantity::ZERO
    );
}
