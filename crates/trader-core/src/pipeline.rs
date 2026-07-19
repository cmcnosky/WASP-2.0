use std::collections::BTreeMap;

use chrono::Duration;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    domain::{
        DecisionSnapshot, FreshExecutionQuote, HashDigest, OrderIntent, OrderPlan, OrderSide,
        RiskDecision, RiskDisposition, RiskLimitSnapshot, StrategyRelease, TargetPortfolio,
        TimeInForce, WholeQuantity,
    },
    error::{CoreError, CoreResult},
    fixed::{Fixed, Money, Price},
    market::validate_snapshot,
    risk::apply_risk,
    strategy::generate_target,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationResult {
    pub target: TargetPortfolio,
    pub risk: RiskDecision,
    /// Non-executable deltas. An adapter must supply a fresh post-decision raw
    /// quote to `materialize_order_intent` before submission is possible.
    pub order_plans: Vec<OrderPlan>,
}

pub fn evaluate_decision(
    snapshot: &DecisionSnapshot,
    release: &StrategyRelease,
    limits: &RiskLimitSnapshot,
) -> CoreResult<EvaluationResult> {
    let target = generate_target(release, snapshot)?;
    let risk = apply_risk(snapshot, &target, limits)?;
    let order_plans = plan_orders(snapshot, release, &risk)?;
    Ok(EvaluationResult {
        target,
        risk,
        order_plans,
    })
}

pub fn plan_orders(
    snapshot: &DecisionSnapshot,
    release: &StrategyRelease,
    risk: &RiskDecision,
) -> CoreResult<Vec<OrderPlan>> {
    if risk.decision_id != snapshot.decision_id {
        return Err(CoreError::InvalidDomain(
            "risk decision does not match decision snapshot".into(),
        ));
    }
    if risk.disposition == RiskDisposition::Rejected {
        return Ok(Vec::new());
    }
    let current: BTreeMap<_, _> = snapshot
        .account
        .positions
        .iter()
        .map(|position| (&position.symbol, position.quantity.get()))
        .collect();
    let release_hash = release.release_hash()?;
    let mut plans = Vec::new();
    for approved in &risk.approved_positions {
        let current_quantity = current.get(&approved.symbol).copied().unwrap_or(0);
        let target_quantity = approved.target_quantity.get();
        if current_quantity == target_quantity {
            continue;
        }
        let (side, quantity) = if target_quantity > current_quantity {
            (OrderSide::Buy, target_quantity - current_quantity)
        } else {
            (OrderSide::Sell, current_quantity - target_quantity)
        };
        if quantity == 0 {
            continue;
        }
        let evidence = HashDigest::of_json(&(
            &snapshot.input_data_hash,
            &snapshot.account_snapshot_hash,
            &snapshot.account.account_fingerprint,
            &snapshot.schedule,
            &release_hash,
            &risk,
            &approved.symbol,
            side,
            quantity,
        ))?;
        let stable_material = format!(
            "{}:{}:{}:{:?}:{}:{}",
            release.release_id, snapshot.decision_id, approved.symbol, side, quantity, evidence
        );
        let plan_uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, stable_material.as_bytes());
        plans.push(OrderPlan {
            plan_id: plan_uuid.to_string(),
            release_id: release.release_id.clone(),
            decision_id: snapshot.decision_id.clone(),
            symbol: approved.symbol.clone(),
            side,
            quantity: WholeQuantity::new(quantity),
            decision_reference_price: approved.raw_reference_price,
            decision_evidence_hash: evidence,
            created_at: snapshot.as_of,
        });
    }
    // Exits always precede entries so replacement holdings cannot spend unsettled capacity.
    plans.sort_by(|left, right| {
        let side_rank = |side| match side {
            OrderSide::Sell => 0,
            OrderSide::Buy => 1,
        };
        side_rank(left.side)
            .cmp(&side_rank(right.side))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    // A replacement entry is not authorized until all exit fills have changed
    // the next account snapshot. Sorting alone is not an execution barrier.
    if plans.iter().any(|plan| plan.side == OrderSide::Sell) {
        plans.retain(|plan| plan.side == OrderSide::Sell);
    }
    Ok(plans)
}

/// Convert a non-executable plan into an intent only after receiving a raw,
/// post-decision quote. The adapter must submit before `quote_valid_until`.
pub fn materialize_order_intent(
    snapshot: &DecisionSnapshot,
    release: &StrategyRelease,
    risk: &RiskDecision,
    plan: &OrderPlan,
    quote: &FreshExecutionQuote,
) -> CoreResult<OrderIntent> {
    validate_snapshot(snapshot, release)?;
    if risk.disposition == RiskDisposition::Rejected
        || risk.decision_id != snapshot.decision_id
        || plan.release_id != release.release_id
        || plan.decision_id != snapshot.decision_id
        || plan.symbol != quote.symbol
        || plan.quantity == WholeQuantity::ZERO
    {
        return Err(CoreError::InvalidDomain(
            "intent materialization identities or risk state do not match".into(),
        ));
    }
    if !plan_orders(snapshot, release, risk)?.contains(plan) {
        return Err(CoreError::InvalidDomain(
            "order plan is not an exact output of the authorized decision".into(),
        ));
    }
    let quote_validity = quote.valid_until.signed_duration_since(quote.received_at);
    if quote.provider_at <= snapshot.as_of
        || quote.received_at < quote.provider_at
        || quote_validity <= Duration::zero()
        || quote_validity > Duration::seconds(15)
        || !quote.raw_price.fixed().is_positive()
    {
        return Err(CoreError::InvalidDomain(
            "execution quote has invalid price, provenance ordering, or validity window".into(),
        ));
    }
    let band = Fixed::from_scaled(i128::from(risk.limits.marketable_limit_band_bps) * 100);
    let price_factor = match plan.side {
        OrderSide::Buy => Fixed::ONE.checked_add(band)?,
        OrderSide::Sell => Fixed::ONE.checked_sub(band)?,
    };
    if !price_factor.is_positive() {
        return Err(CoreError::InvalidDomain(
            "marketable price band produced a non-positive limit".into(),
        ));
    }
    let limit_price = Price(quote.raw_price.fixed().checked_mul(price_factor)?);
    if plan.side == OrderSide::Buy {
        let order_notional = limit_price.checked_mul_quantity(plan.quantity.get())?;
        let available_cash = Money(
            snapshot
                .account
                .cash
                .fixed()
                .min(snapshot.account.buying_power.fixed()),
        );
        if order_notional > risk.limits.max_order_notional || order_notional > available_cash {
            return Err(CoreError::RiskRejected(
                "fresh quote exceeds unleveraged cash/order-notional authority".into(),
            ));
        }
        let stop_fraction =
            Fixed::from_scaled(i128::from(risk.limits.planned_stop_distance_bps) * 100);
        let planned_loss = Money(order_notional.fixed().checked_mul(stop_fraction)?);
        if planned_loss > risk.limits.max_planned_loss {
            return Err(CoreError::RiskRejected(
                "fresh quote exceeds planned-loss authority".into(),
            ));
        }
        let approved = risk
            .approved_positions
            .iter()
            .find(|position| position.symbol == plan.symbol)
            .ok_or_else(|| {
                CoreError::InvalidDomain(
                    "fresh quote has no matching risk-approved position".into(),
                )
            })?;
        let target_notional = limit_price.checked_mul_quantity(approved.target_quantity.get())?;
        let weight_cap = Money(
            snapshot
                .account
                .equity
                .fixed()
                .checked_mul(risk.limits.max_position_weight)?,
        );
        let gross_cap = risk.limits.max_gross_exposure.min(weight_cap);
        if target_notional > gross_cap {
            return Err(CoreError::RiskRejected(
                "fresh quote exceeds gross/position exposure authority".into(),
            ));
        }
    }
    let release_hash = release.release_hash()?;
    let materialization_evidence = HashDigest::of_json(&(
        &snapshot.input_data_hash,
        &snapshot.account_snapshot_hash,
        &snapshot.account.account_fingerprint,
        &release_hash,
        &risk,
        &plan,
        &quote,
        limit_price,
    ))?;
    let stable_material = format!("{}:{}", plan.plan_id, materialization_evidence);
    let intent_uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, stable_material.as_bytes());
    let client_hash = HashDigest::sha256(stable_material.as_bytes()).as_hex();
    Ok(OrderIntent {
        intent_id: intent_uuid.to_string(),
        client_order_id: format!("autotrader-{}", &client_hash[..24]),
        release_id: release.release_id.clone(),
        decision_id: snapshot.decision_id.clone(),
        symbol: plan.symbol.clone(),
        side: plan.side,
        quantity: plan.quantity,
        limit_price,
        decision_at: snapshot.as_of,
        arrival_quote: quote.raw_price,
        quote_provider_at: quote.provider_at,
        quote_received_at: quote.received_at,
        quote_valid_until: quote.valid_until,
        quote_payload_hash: quote.payload_hash,
        time_in_force: TimeInForce::Day,
        decision_evidence_hash: plan.decision_evidence_hash,
        materialization_evidence_hash: materialization_evidence,
        created_at: quote.received_at,
    })
}
