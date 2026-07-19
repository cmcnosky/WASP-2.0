use chrono::{Duration, TimeZone, Utc};
use trader_core::{
    evaluate_decision, materialize_order_intent, run_backtest, AccountPosition, AccountSnapshot,
    AccountStatus, CompletedObservation, DecisionReplayRequest, DecisionSchedule, DecisionSnapshot,
    Fixed, FreshExecutionQuote, HashDigest, MomentumTrendSpec, Money, RebalanceCadence,
    RiskDisposition, RiskLimitSnapshot, StrategyRelease, StrategySpec, Symbol, WholeQuantity,
};

fn release() -> StrategyRelease {
    let strategy = StrategySpec::MomentumTrend(MomentumTrendSpec {
        momentum_lookback_sessions: 63,
        trend_lookback_sessions: 126,
        cadence: RebalanceCadence::Weekly,
    });
    StrategyRelease {
        release_id: "release-deterministic-1".into(),
        code_hash: HashDigest::sha256("code"),
        parameters_hash: HashDigest::of_json(&strategy).unwrap(),
        universe: ["DIA", "IVV", "IWM", "QQQ", "SCHB", "SPY", "VOO", "VTI"]
            .into_iter()
            .map(|symbol| Symbol::new(symbol).unwrap())
            .collect(),
        data_hash: HashDigest::sha256("data"),
        cost_model_hash: HashDigest::sha256("costs"),
        statistical_certificate_hash: HashDigest::sha256("certificate"),
        strategy,
        valid_from: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        expires_at: Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap(),
    }
}

fn snapshot(release: &StrategyRelease, decision_id: &str) -> DecisionSnapshot {
    let start = Utc.with_ymd_and_hms(2025, 1, 1, 21, 0, 0).unwrap();
    let mut observations = Vec::new();
    for (symbol_index, symbol) in release.universe.iter().enumerate() {
        for session_index in 0..127i64 {
            let completed_at = start + Duration::days(session_index);
            let scaled_price = 100_000_000
                + i128::try_from(symbol_index + 1).unwrap() * i128::from(session_index) * 10_000;
            observations.push(CompletedObservation {
                symbol: symbol.clone(),
                session: completed_at.date_naive(),
                completed_at,
                raw_close: trader_core::Price::from_scaled(
                    50_000_000 + i128::try_from(symbol_index).unwrap() * 1_000_000,
                ),
                total_return_close: trader_core::Price::from_scaled(scaled_price),
            });
        }
    }
    let as_of = start + Duration::days(127);
    let account = AccountSnapshot {
        account_fingerprint: HashDigest::sha256("account"),
        status: AccountStatus::Active,
        trading_blocked: false,
        cash: Money::from_units(1_000).unwrap(),
        buying_power: Money::from_units(1_000).unwrap(),
        equity: Money::from_units(1_000).unwrap(),
        day_pnl: Money::ZERO,
        drawdown: Money::ZERO,
        positions: Vec::new(),
    };
    DecisionSnapshot {
        decision_id: decision_id.into(),
        release_id: release.release_id.clone(),
        as_of,
        market_session: (start + Duration::days(126)).date_naive(),
        schedule: DecisionSchedule {
            eligible_cadences: vec![RebalanceCadence::Weekly],
            calendar_evidence_hash: HashDigest::sha256("calendar"),
        },
        account_snapshot_hash: HashDigest::of_json(&account).unwrap(),
        account,
        input_data_hash: HashDigest::of_json(&observations).unwrap(),
        observations,
    }
}

fn limits() -> RiskLimitSnapshot {
    RiskLimitSnapshot {
        max_gross_exposure: Money::from_units(500).unwrap(),
        max_position_weight: Fixed::from_scaled(500_000),
        max_positions: 1,
        max_order_notional: Money::from_units(500).unwrap(),
        max_planned_loss: Money::from_units(10).unwrap(),
        daily_loss_limit: Money::from_units(25).unwrap(),
        hard_drawdown_limit: Money::from_units(100).unwrap(),
        planned_stop_distance_bps: 500,
        marketable_limit_band_bps: 10,
        new_positions_enabled: true,
    }
}

#[test]
fn identical_inputs_produce_byte_identical_decisions_and_ids() {
    let release = release();
    let snapshot = snapshot(&release, "decision-1");
    let first = evaluate_decision(&snapshot, &release, &limits()).unwrap();
    let second = evaluate_decision(&snapshot, &release, &limits()).unwrap();
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
    assert_eq!(first.risk.disposition, RiskDisposition::Reduced);
    assert_eq!(first.order_plans.len(), 1);
    assert_eq!(first.order_plans[0].plan_id, second.order_plans[0].plan_id);
    assert_ne!(
        first.target.positions[0].raw_reference_price,
        snapshot.observations.last().unwrap().total_return_close
    );
}

#[test]
fn appending_a_later_snapshot_cannot_change_an_earlier_step() {
    let release = release();
    let first_snapshot = snapshot(&release, "decision-1");
    let first_only = run_backtest(&DecisionReplayRequest {
        release: release.clone(),
        risk_limits: limits(),
        snapshots: vec![first_snapshot.clone()],
    })
    .unwrap();
    let mut later = first_snapshot.clone();
    later.decision_id = "decision-2".into();
    later.as_of += Duration::days(1);
    let extended = run_backtest(&DecisionReplayRequest {
        release,
        risk_limits: limits(),
        snapshots: vec![first_snapshot, later],
    })
    .unwrap();
    assert!(!first_only.performance_evidence_available);
    assert!(first_only.hold_reason.starts_with("HOLD:"));
    assert_eq!(
        first_only.decision_replay.steps[0],
        extended.decision_replay.steps[0]
    );
}

#[test]
fn future_observation_or_hash_tampering_is_rejected() {
    let release = release();
    let mut future_snapshot = snapshot(&release, "decision-1");
    future_snapshot.observations[0].completed_at = future_snapshot.as_of + Duration::seconds(1);
    future_snapshot.input_data_hash = HashDigest::of_json(&future_snapshot.observations).unwrap();
    assert!(evaluate_decision(&future_snapshot, &release, &limits()).is_err());

    let mut data_tampered = snapshot(&release, "decision-2");
    data_tampered.observations[0].total_return_close = "999".parse().unwrap();
    assert!(evaluate_decision(&data_tampered, &release, &limits()).is_err());

    let mut account_tampered = snapshot(&release, "decision-3");
    account_tampered.account.cash = Money::from_units(999).unwrap();
    assert!(evaluate_decision(&account_tampered, &release, &limits()).is_err());

    let mut duplicate_positions = snapshot(&release, "decision-4");
    let duplicate = AccountPosition {
        symbol: Symbol::new("SPY").unwrap(),
        quantity: WholeQuantity::new(1),
        average_entry_price: "100".parse().unwrap(),
        market_price: "101".parse().unwrap(),
    };
    duplicate_positions.account.positions = vec![duplicate.clone(), duplicate];
    duplicate_positions.account_snapshot_hash =
        HashDigest::of_json(&duplicate_positions.account).unwrap();
    assert!(evaluate_decision(&duplicate_positions, &release, &limits()).is_err());
}

#[test]
fn intent_requires_a_fresh_raw_post_decision_quote() {
    let release = release();
    let snapshot = snapshot(&release, "decision-quote");
    let evaluated = evaluate_decision(&snapshot, &release, &limits()).unwrap();
    let plan = &evaluated.order_plans[0];
    let predecision = FreshExecutionQuote {
        symbol: plan.symbol.clone(),
        raw_price: "57".parse().unwrap(),
        observed_at: snapshot.as_of,
        valid_until: snapshot.as_of + Duration::seconds(10),
        source_hash: HashDigest::sha256("quote"),
    };
    assert!(
        materialize_order_intent(&snapshot, &release, &evaluated.risk, plan, &predecision).is_err()
    );

    let fresh = FreshExecutionQuote {
        observed_at: snapshot.as_of + Duration::seconds(1),
        ..predecision
    };
    let intent =
        materialize_order_intent(&snapshot, &release, &evaluated.risk, plan, &fresh).unwrap();
    assert_eq!(intent.quote_source_hash, fresh.source_hash);
    assert!(intent.limit_price > fresh.raw_price);
}

#[test]
fn an_ineligible_calendar_cadence_holds_current_state_without_orders() {
    let release = release();
    let mut snapshot = snapshot(&release, "decision-off-cadence");
    snapshot.schedule.eligible_cadences = vec![RebalanceCadence::Monthly];

    let evaluated = evaluate_decision(&snapshot, &release, &limits()).unwrap();

    assert!(evaluated.order_plans.is_empty());
    assert!(evaluated
        .target
        .reason_codes
        .contains(&"calendar_cadence_not_eligible".to_owned()));
}
