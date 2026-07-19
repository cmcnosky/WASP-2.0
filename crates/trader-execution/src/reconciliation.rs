use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use trader_core::{
    HashDigest, Money, ReconciliationDifference, ReconciliationDifferenceKind,
    ReconciliationReport, Symbol, WholeQuantity,
};

use crate::lifecycle::BrokerOrderStatus;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationInput {
    pub generated_at: DateTime<Utc>,
    pub account_fingerprint: HashDigest,
    pub execution_fencing_token: u64,
    pub local_cash: Money,
    pub broker_cash: Money,
    pub local_positions: BTreeMap<Symbol, WholeQuantity>,
    pub broker_positions: BTreeMap<Symbol, WholeQuantity>,
    pub local_order_statuses: BTreeMap<String, String>,
    pub broker_order_statuses: BTreeMap<String, String>,
    pub local_fill_fingerprints: Vec<HashDigest>,
    pub broker_fill_fingerprints: Vec<HashDigest>,
}

pub fn reconcile(input: ReconciliationInput) -> ReconciliationReport {
    let mut differences = Vec::new();
    if input.local_cash != input.broker_cash {
        differences.push(ReconciliationDifference {
            kind: ReconciliationDifferenceKind::CashMismatch,
            subject: "cash".into(),
            local_value: Some(input.local_cash.to_string()),
            broker_value: Some(input.broker_cash.to_string()),
            detail: format!("local={} broker={}", input.local_cash, input.broker_cash),
        });
    }
    for (symbol, local) in &input.local_positions {
        match input.broker_positions.get(symbol) {
            None if *local != WholeQuantity::ZERO => differences.push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::MissingAtBroker,
                subject: symbol.to_string(),
                local_value: Some(local.get().to_string()),
                broker_value: None,
                detail: format!("local quantity {}", local.get()),
            }),
            Some(broker) if broker != local => differences.push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::QuantityMismatch,
                subject: symbol.to_string(),
                local_value: Some(local.get().to_string()),
                broker_value: Some(broker.get().to_string()),
                detail: format!("local={} broker={}", local.get(), broker.get()),
            }),
            _ => {}
        }
    }
    for (symbol, broker) in &input.broker_positions {
        if !input.local_positions.contains_key(symbol) && *broker != WholeQuantity::ZERO {
            differences.push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::MissingLocally,
                subject: symbol.to_string(),
                local_value: None,
                broker_value: Some(broker.get().to_string()),
                detail: format!("broker quantity {}", broker.get()),
            });
        }
    }
    for (client_id, local) in &input.local_order_statuses {
        if matches!(
            BrokerOrderStatus::from_provider(local),
            BrokerOrderStatus::Unknown(_)
        ) {
            differences.push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::UnknownProviderState,
                subject: client_id.clone(),
                local_value: Some(local.clone()),
                broker_value: input.broker_order_statuses.get(client_id).cloned(),
                detail: format!("unrecognized local order status {local}"),
            });
            continue;
        }
        match input.broker_order_statuses.get(client_id) {
            None => differences.push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::MissingAtBroker,
                subject: client_id.clone(),
                local_value: Some(local.clone()),
                broker_value: None,
                detail: format!("local status {local}"),
            }),
            Some(broker)
                if matches!(
                    BrokerOrderStatus::from_provider(broker),
                    BrokerOrderStatus::Unknown(_)
                ) =>
            {
                differences.push(ReconciliationDifference {
                    kind: ReconciliationDifferenceKind::UnknownProviderState,
                    subject: client_id.clone(),
                    local_value: Some(local.clone()),
                    broker_value: Some(broker.clone()),
                    detail: format!("unrecognized broker order status {broker}"),
                })
            }
            Some(broker) if broker != local => differences.push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::StatusMismatch,
                subject: client_id.clone(),
                local_value: Some(local.clone()),
                broker_value: Some(broker.clone()),
                detail: format!("local={local} broker={broker}"),
            }),
            _ => {}
        }
    }
    for (client_id, broker) in &input.broker_order_statuses {
        if !input.local_order_statuses.contains_key(client_id) {
            differences.push(ReconciliationDifference {
                kind: if matches!(
                    BrokerOrderStatus::from_provider(broker),
                    BrokerOrderStatus::Unknown(_)
                ) {
                    ReconciliationDifferenceKind::UnknownProviderState
                } else {
                    ReconciliationDifferenceKind::MissingLocally
                },
                subject: client_id.clone(),
                local_value: None,
                broker_value: Some(broker.clone()),
                detail: format!("broker status {broker}"),
            });
        }
    }
    let mut local_fill_counts = BTreeMap::<HashDigest, usize>::new();
    let mut broker_fill_counts = BTreeMap::<HashDigest, usize>::new();
    for fingerprint in input.local_fill_fingerprints {
        *local_fill_counts.entry(fingerprint).or_default() += 1;
    }
    for fingerprint in input.broker_fill_fingerprints {
        *broker_fill_counts.entry(fingerprint).or_default() += 1;
    }
    let fill_fingerprints: BTreeSet<_> = local_fill_counts
        .keys()
        .chain(broker_fill_counts.keys())
        .copied()
        .collect();
    for fingerprint in fill_fingerprints {
        let local = local_fill_counts.get(&fingerprint).copied().unwrap_or(0);
        let broker = broker_fill_counts.get(&fingerprint).copied().unwrap_or(0);
        if local != broker {
            differences.push(ReconciliationDifference {
                kind: if local < broker {
                    ReconciliationDifferenceKind::MissingLocally
                } else {
                    ReconciliationDifferenceKind::MissingAtBroker
                },
                subject: format!("fill:{fingerprint}"),
                local_value: Some(local.to_string()),
                broker_value: Some(broker.to_string()),
                detail: format!("local_count={local} broker_count={broker}"),
            });
        }
    }
    ReconciliationReport {
        generated_at: input.generated_at,
        account_fingerprint: input.account_fingerprint,
        execution_fencing_token: input.execution_fencing_token,
        may_resume_execution: differences.is_empty(),
        differences,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> ReconciliationInput {
        ReconciliationInput {
            generated_at: "2026-07-18T14:00:00Z".parse().unwrap(),
            account_fingerprint: HashDigest::sha256("account"),
            execution_fencing_token: 7,
            local_cash: Money::from_units(1_000).unwrap(),
            broker_cash: Money::from_units(1_000).unwrap(),
            local_positions: BTreeMap::new(),
            broker_positions: BTreeMap::new(),
            local_order_statuses: BTreeMap::new(),
            broker_order_statuses: BTreeMap::new(),
            local_fill_fingerprints: Vec::new(),
            broker_fill_fingerprints: Vec::new(),
        }
    }

    #[test]
    fn equal_unknown_statuses_are_not_clean() {
        let mut input = input();
        input
            .local_order_statuses
            .insert("client-1".into(), "future_status".into());
        input
            .broker_order_statuses
            .insert("client-1".into(), "future_status".into());
        let report = reconcile(input);
        assert!(!report.may_resume_execution);
        assert!(report.differences.iter().any(
            |difference| difference.kind == ReconciliationDifferenceKind::UnknownProviderState
        ));
    }

    #[test]
    fn missing_fill_identity_is_not_clean() {
        let mut input = input();
        input
            .broker_fill_fingerprints
            .push(HashDigest::sha256("fill-1"));
        let report = reconcile(input);
        assert!(!report.may_resume_execution);
        assert!(report
            .differences
            .iter()
            .any(|difference| difference.subject.starts_with("fill:")));
    }
}
