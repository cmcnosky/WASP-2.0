use std::collections::{BTreeMap, BTreeSet};

use crate::{
    domain::{
        DecisionSnapshot, MomentumTrendSpec, StrategyRelease, StrategySpec, Symbol,
        TargetPortfolio, TargetPosition, WholeQuantity,
    },
    error::{CoreError, CoreResult},
    fixed::Fixed,
    market::{series_by_symbol, validate_snapshot},
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Candidate {
    symbol: Symbol,
    momentum: Fixed,
    above_trend: bool,
    raw_reference_price: crate::Price,
}

pub fn generate_target(
    release: &StrategyRelease,
    snapshot: &DecisionSnapshot,
) -> CoreResult<TargetPortfolio> {
    validate_snapshot(snapshot, release)?;
    match &release.strategy {
        StrategySpec::MomentumTrend(spec)
            if !snapshot.schedule.eligible_cadences.contains(&spec.cadence) =>
        {
            hold_current_target(release, snapshot)
        }
        StrategySpec::MomentumTrend(spec) => momentum_trend_target(release, snapshot, spec),
    }
}

fn hold_current_target(
    release: &StrategyRelease,
    snapshot: &DecisionSnapshot,
) -> CoreResult<TargetPortfolio> {
    let mut positions = Vec::with_capacity(snapshot.account.positions.len());
    for current in &snapshot.account.positions {
        let market_value = current
            .market_price
            .checked_mul_quantity(current.quantity.get())?;
        let weight = if snapshot.account.equity == crate::Money::ZERO {
            Fixed::ZERO
        } else {
            market_value
                .fixed()
                .checked_div(snapshot.account.equity.fixed())?
        };
        positions.push(TargetPosition {
            symbol: current.symbol.clone(),
            target_weight: weight,
            target_quantity: current.quantity,
            raw_reference_price: current.market_price,
            reason_codes: vec!["not_rebalance_session".into()],
        });
    }
    Ok(TargetPortfolio {
        decision_id: snapshot.decision_id.clone(),
        release_id: release.release_id.clone(),
        generated_at: snapshot.as_of,
        cash_target: positions.is_empty(),
        positions,
        reason_codes: vec!["calendar_cadence_not_eligible".into()],
    })
}

fn momentum_trend_target(
    release: &StrategyRelease,
    snapshot: &DecisionSnapshot,
    spec: &MomentumTrendSpec,
) -> CoreResult<TargetPortfolio> {
    let grouped = series_by_symbol(&snapshot.observations);
    let required = usize::from(
        spec.momentum_lookback_sessions
            .max(spec.trend_lookback_sessions),
    ) + 1;
    let mut candidates = Vec::with_capacity(release.universe.len());
    for symbol in &release.universe {
        let series = grouped
            .get(symbol)
            .ok_or_else(|| CoreError::InsufficientHistory {
                symbol: symbol.to_string(),
                required,
                actual: 0,
            })?;
        if series.len() < required {
            return Err(CoreError::InsufficientHistory {
                symbol: symbol.to_string(),
                required,
                actual: series.len(),
            });
        }
        let latest = series[series.len() - 1].total_return_close;
        let past_index = series.len() - 1 - usize::from(spec.momentum_lookback_sessions);
        let past = series[past_index].total_return_close;
        let momentum = latest
            .fixed()
            .checked_div(past.fixed())?
            .checked_sub(Fixed::ONE)?;

        let trend_count = usize::from(spec.trend_lookback_sessions);
        let trend_slice = &series[series.len() - trend_count..];
        let trend_sum = trend_slice
            .iter()
            .try_fold(Fixed::ZERO, |sum, observation| {
                sum.checked_add(observation.total_return_close.fixed())
            })?;
        let trend = trend_sum.checked_div(Fixed::from_units(trend_count as i128)?)?;
        candidates.push(Candidate {
            symbol: symbol.clone(),
            momentum,
            above_trend: latest.fixed() > trend,
            raw_reference_price: series[series.len() - 1].raw_close,
        });
    }

    // Stable tie break is lexical symbol order, making every execution mode identical.
    candidates.sort_by(|left, right| {
        right
            .momentum
            .cmp(&left.momentum)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    let selected = candidates
        .first()
        .filter(|candidate| candidate.momentum.is_positive() && candidate.above_trend);

    let mut positions = BTreeMap::<Symbol, TargetPosition>::new();
    let current_symbols: BTreeSet<_> = snapshot
        .account
        .positions
        .iter()
        .map(|position| position.symbol.clone())
        .collect();
    for current in &snapshot.account.positions {
        positions.insert(
            current.symbol.clone(),
            TargetPosition {
                symbol: current.symbol.clone(),
                target_weight: Fixed::ZERO,
                target_quantity: WholeQuantity::ZERO,
                raw_reference_price: current.market_price,
                reason_codes: vec!["not_selected_exit".into()],
            },
        );
    }

    let mut reason_codes = Vec::new();
    let mut invested = false;
    if let Some(candidate) = selected {
        let quantity_fixed = snapshot
            .account
            .equity
            .fixed()
            .checked_div(candidate.raw_reference_price.fixed())?;
        let quantity = u64::try_from(quantity_fixed.floor_units()).map_err(|_| {
            CoreError::InvalidDomain("target quantity is outside whole-share range".into())
        })?;
        if quantity > 0 {
            invested = true;
            positions.insert(
                candidate.symbol.clone(),
                TargetPosition {
                    symbol: candidate.symbol.clone(),
                    target_weight: Fixed::ONE,
                    target_quantity: WholeQuantity::new(quantity),
                    raw_reference_price: candidate.raw_reference_price,
                    reason_codes: vec!["highest_positive_momentum".into(), "above_trend".into()],
                },
            );
            reason_codes.push("risk_review_required".into());
        } else {
            reason_codes.push("whole_share_unaffordable".into());
        }
    } else {
        reason_codes.push("cash_filter_active".into());
    }
    // Keep the explicit current-symbol set in the calculation: it documents that
    // holdings outside the selected target are always emitted as zero targets.
    debug_assert!(current_symbols
        .iter()
        .all(|symbol| positions.contains_key(symbol)));

    Ok(TargetPortfolio {
        decision_id: snapshot.decision_id.clone(),
        release_id: release.release_id.clone(),
        generated_at: snapshot.as_of,
        cash_target: !invested,
        positions: positions.into_values().collect(),
        reason_codes,
    })
}
