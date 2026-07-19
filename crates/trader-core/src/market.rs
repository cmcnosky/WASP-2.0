use std::collections::{BTreeMap, BTreeSet};

use crate::{
    domain::{CompletedObservation, DecisionSnapshot, HashDigest, StrategyRelease, Symbol},
    error::{CoreError, CoreResult},
    fixed::Price,
};

/// Validate that a decision only contains completed, release-authorized data and
/// that its provenance hash binds the exact ordered observations.
pub fn validate_snapshot(snapshot: &DecisionSnapshot, release: &StrategyRelease) -> CoreResult<()> {
    release.validate()?;
    if snapshot.decision_id.trim().is_empty() || snapshot.release_id != release.release_id {
        return Err(CoreError::InvalidDomain(
            "decision identity does not match strategy release".into(),
        ));
    }
    if snapshot.as_of < release.valid_from || snapshot.as_of >= release.expires_at {
        return Err(CoreError::InvalidDomain(
            "decision is outside the release validity interval".into(),
        ));
    }
    if HashDigest::of_json(&snapshot.account)? != snapshot.account_snapshot_hash {
        return Err(CoreError::InvalidDomain(
            "account_snapshot_hash does not bind the supplied account state".into(),
        ));
    }
    let eligible_cadences: BTreeSet<_> = snapshot.schedule.eligible_cadences.iter().collect();
    if eligible_cadences.len() != snapshot.schedule.eligible_cadences.len() {
        return Err(CoreError::InvalidDomain(
            "decision schedule contains duplicate cadence eligibility".into(),
        ));
    }
    let mut account_symbols = BTreeSet::new();
    for position in &snapshot.account.positions {
        if !account_symbols.insert(&position.symbol) {
            return Err(CoreError::InvalidDomain(format!(
                "duplicate account position for {}",
                position.symbol
            )));
        }
        if position.quantity == crate::WholeQuantity::ZERO
            || !position.average_entry_price.fixed().is_positive()
            || !position.market_price.fixed().is_positive()
        {
            return Err(CoreError::InvalidDomain(format!(
                "account position for {} has non-positive quantity or price",
                position.symbol
            )));
        }
    }
    let universe: BTreeSet<_> = release.universe.iter().collect();
    let mut observed = BTreeSet::new();
    let mut previous: Option<(&Symbol, chrono::NaiveDate)> = None;
    for observation in &snapshot.observations {
        if !universe.contains(&observation.symbol) {
            return Err(CoreError::InvalidDomain(format!(
                "observation for {} is outside release universe",
                observation.symbol
            )));
        }
        if observation.completed_at > snapshot.as_of
            || observation.session > snapshot.market_session
        {
            return Err(CoreError::InvalidDomain(format!(
                "observation for {} was not available by decision time",
                observation.symbol
            )));
        }
        if !observation.total_return_close.fixed().is_positive()
            || !observation.raw_close.fixed().is_positive()
        {
            return Err(CoreError::InvalidDomain(format!(
                "non-positive raw or adjusted price for {}",
                observation.symbol
            )));
        }
        if !observed.insert((&observation.symbol, observation.session)) {
            return Err(CoreError::InvalidDomain(format!(
                "duplicate observation for {} on {}",
                observation.symbol, observation.session
            )));
        }
        if let Some((previous_symbol, previous_session)) = previous {
            if (&observation.symbol, observation.session) < (previous_symbol, previous_session) {
                return Err(CoreError::InvalidDomain(
                    "observations must be canonicalized by symbol then session".into(),
                ));
            }
        }
        previous = Some((&observation.symbol, observation.session));
    }
    if HashDigest::of_json(&snapshot.observations)? != snapshot.input_data_hash {
        return Err(CoreError::InvalidDomain(
            "input_data_hash does not bind the supplied observations".into(),
        ));
    }
    Ok(())
}

pub fn series_by_symbol(
    observations: &[CompletedObservation],
) -> BTreeMap<&Symbol, Vec<&CompletedObservation>> {
    let mut grouped: BTreeMap<&Symbol, Vec<&CompletedObservation>> = BTreeMap::new();
    for observation in observations {
        grouped
            .entry(&observation.symbol)
            .or_default()
            .push(observation);
    }
    grouped
}

pub fn latest_raw_prices(snapshot: &DecisionSnapshot) -> BTreeMap<Symbol, Price> {
    let mut result = BTreeMap::new();
    for observation in &snapshot.observations {
        result.insert(observation.symbol.clone(), observation.raw_close);
    }
    result
}
