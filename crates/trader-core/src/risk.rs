use std::collections::BTreeMap;

use crate::{
    domain::{
        AccountStatus, DecisionSnapshot, RiskDecision, RiskDisposition, RiskLimitSnapshot,
        TargetPortfolio, TargetPosition, WholeQuantity,
    },
    error::{CoreError, CoreResult},
    fixed::{Fixed, Money},
};

pub fn apply_risk(
    snapshot: &DecisionSnapshot,
    target: &TargetPortfolio,
    limits: &RiskLimitSnapshot,
) -> CoreResult<RiskDecision> {
    limits.validate()?;
    if target.decision_id != snapshot.decision_id || target.release_id != snapshot.release_id {
        return Err(CoreError::InvalidDomain(
            "risk input identities do not match".into(),
        ));
    }
    let mut rejection_reasons = Vec::new();
    if snapshot.account.status != AccountStatus::Active {
        rejection_reasons.push("account_not_active".into());
    }
    if snapshot.account.trading_blocked {
        rejection_reasons.push("account_trading_blocked".into());
    }
    if snapshot.account.positions.len() > 1 {
        rejection_reasons.push("v1_unexpected_multiple_account_positions".into());
    }
    if snapshot.account.equity.is_negative()
        || snapshot.account.cash.is_negative()
        || snapshot.account.buying_power.is_negative()
    {
        rejection_reasons.push("invalid_negative_account_value".into());
    }
    let daily_loss = snapshot.account.day_pnl.fixed().checked_abs()?;
    if snapshot.account.day_pnl.is_negative() && daily_loss >= limits.daily_loss_limit.fixed() {
        rejection_reasons.push("daily_loss_limit_reached".into());
    }
    if snapshot.account.drawdown.fixed().checked_abs()? >= limits.hard_drawdown_limit.fixed() {
        rejection_reasons.push("hard_drawdown_limit_reached".into());
    }
    if !rejection_reasons.is_empty() {
        return Ok(RiskDecision {
            decision_id: target.decision_id.clone(),
            disposition: RiskDisposition::Rejected,
            approved_positions: Vec::new(),
            limits: limits.clone(),
            reason_codes: rejection_reasons,
        });
    }

    let current: BTreeMap<_, _> = snapshot
        .account
        .positions
        .iter()
        .map(|position| (&position.symbol, position.quantity.get()))
        .collect();
    let positive_targets = target
        .positions
        .iter()
        .filter(|position| position.target_quantity.get() > 0)
        .count();
    if positive_targets > usize::from(limits.max_positions) {
        return Ok(RiskDecision {
            decision_id: target.decision_id.clone(),
            disposition: RiskDisposition::Rejected,
            approved_positions: Vec::new(),
            limits: limits.clone(),
            reason_codes: vec!["max_positions_exceeded".into()],
        });
    }

    let equity_weight_cap = Money(
        snapshot
            .account
            .equity
            .fixed()
            .checked_mul(limits.max_position_weight)?,
    );
    let exposure_cap = if equity_weight_cap < limits.max_gross_exposure {
        equity_weight_cap
    } else {
        limits.max_gross_exposure
    };
    let mut approved_positions = Vec::with_capacity(target.positions.len());
    let mut reduced = false;
    let mut reasons = Vec::new();

    for requested in &target.positions {
        if !requested.raw_reference_price.fixed().is_positive() {
            return Err(CoreError::InvalidDomain(format!(
                "non-positive risk reference price for {}",
                requested.symbol
            )));
        }
        let current_quantity = current.get(&requested.symbol).copied().unwrap_or(0);
        let mut approved_quantity = requested.target_quantity.get();
        if approved_quantity > current_quantity
            && current_quantity == 0
            && !limits.new_positions_enabled
        {
            approved_quantity = 0;
            reduced = true;
            reasons.push(format!("new_position_disabled:{}", requested.symbol));
        }
        let buy_price_factor = Fixed::ONE.checked_add(Fixed::from_scaled(
            i128::from(limits.marketable_limit_band_bps) * 100,
        ))?;
        let worst_case_buy_price = crate::Price(
            requested
                .raw_reference_price
                .fixed()
                .checked_mul(buy_price_factor)?,
        );
        if approved_quantity > 0 {
            let exposure_price = if approved_quantity > current_quantity {
                worst_case_buy_price
            } else {
                requested.raw_reference_price
            };
            let exposure_shares = exposure_cap
                .fixed()
                .checked_div(exposure_price.fixed())?
                .floor_units();
            let exposure_shares = u64::try_from(exposure_shares).unwrap_or(0);
            if approved_quantity > exposure_shares {
                approved_quantity = exposure_shares;
                reduced = true;
                reasons.push(format!("position_exposure_reduced:{}", requested.symbol));
            }
        }
        if approved_quantity > current_quantity {
            let unleveraged_cash = snapshot
                .account
                .cash
                .fixed()
                .min(snapshot.account.buying_power.fixed());
            let max_order_cash = limits.max_order_notional.fixed().min(unleveraged_cash);
            let max_order_shares = max_order_cash
                .checked_div(worst_case_buy_price.fixed())?
                .floor_units();
            let max_order_shares = u64::try_from(max_order_shares).unwrap_or(0);
            let max_target = current_quantity.saturating_add(max_order_shares);
            if approved_quantity > max_target {
                approved_quantity = max_target;
                reduced = true;
                reasons.push(format!("order_notional_reduced:{}", requested.symbol));
            }
            let stop_fraction =
                Fixed::from_scaled(i128::from(limits.planned_stop_distance_bps) * 100);
            let planned_loss_per_share =
                Money(worst_case_buy_price.fixed().checked_mul(stop_fraction)?);
            let max_loss_shares = limits
                .max_planned_loss
                .fixed()
                .checked_div(planned_loss_per_share.fixed())?
                .floor_units();
            let max_loss_shares = u64::try_from(max_loss_shares).unwrap_or(0);
            let max_target = current_quantity.saturating_add(max_loss_shares);
            if approved_quantity > max_target {
                approved_quantity = max_target;
                reduced = true;
                reasons.push(format!("planned_loss_reduced:{}", requested.symbol));
            }
        }
        let mut approved: TargetPosition = requested.clone();
        approved.target_quantity = WholeQuantity::new(approved_quantity);
        if approved_quantity != requested.target_quantity.get() {
            approved.target_weight = if snapshot.account.equity == Money::ZERO {
                Fixed::ZERO
            } else {
                requested
                    .raw_reference_price
                    .checked_mul_quantity(approved_quantity)?
                    .fixed()
                    .checked_div(snapshot.account.equity.fixed())?
            };
        }
        approved_positions.push(approved);
    }

    Ok(RiskDecision {
        decision_id: target.decision_id.clone(),
        disposition: if reduced {
            RiskDisposition::Reduced
        } else {
            RiskDisposition::Approved
        },
        approved_positions,
        limits: limits.clone(),
        reason_codes: reasons,
    })
}
