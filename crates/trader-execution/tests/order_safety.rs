use chrono::{DateTime, Utc};
use serde_json::Value;
use trader_core::{
    BrokerEvent, Environment, HashDigest, OrderIntent, OrderSide, Price, Symbol, TimeInForce,
    WholeQuantity,
};
use trader_execution::{
    ledger::{ExecutionEvent, ExecutionLedger, OutboxAuthority},
    lifecycle::{OrderLifecycle, OrderPhase},
};

fn intent(client_order_id: &str, quantity: u64) -> OrderIntent {
    OrderIntent {
        intent_id: format!("intent-{client_order_id}"),
        client_order_id: client_order_id.into(),
        release_id: "release-1".into(),
        decision_id: "decision-1".into(),
        symbol: Symbol::new("TEST").unwrap(),
        side: OrderSide::Buy,
        quantity: WholeQuantity::new(quantity),
        limit_price: "10.05".parse().unwrap(),
        quote_observed_at: "2026-07-18T13:59:59Z".parse().unwrap(),
        quote_valid_until: "2026-07-18T14:01:00Z".parse().unwrap(),
        quote_source_hash: HashDigest::sha256("quote"),
        time_in_force: TimeInForce::Day,
        decision_evidence_hash: HashDigest::sha256("decision"),
        created_at: "2026-07-18T14:00:00Z".parse().unwrap(),
    }
}

fn event_from_fixture(line: &str) -> BrokerEvent {
    let value: Value = serde_json::from_str(line).unwrap();
    let data = &value["data"];
    let order = &data["order"];
    let status = order["status"].as_str().unwrap();
    let fill_price = order
        .get("filled_avg_price")
        .and_then(Value::as_str)
        .map(str::parse::<Price>)
        .transpose()
        .unwrap();
    BrokerEvent {
        provider_order_id: Some(order["id"].as_str().unwrap().into()),
        client_order_id: order["client_order_id"].as_str().unwrap().into(),
        status: status.into(),
        filled_quantity: WholeQuantity::new(order["filled_qty"].as_str().unwrap().parse().unwrap()),
        fill_price,
        provider_timestamp: data["timestamp"].as_str().unwrap().parse().unwrap(),
        received_at: data["timestamp"].as_str().unwrap().parse().unwrap(),
        raw_payload_hash: HashDigest::sha256(line),
        request_id: Some(format!("fixture-{status}")),
    }
}

#[test]
fn checked_in_lifecycle_fixture_reaches_fill_without_quantity_drift() {
    let fixture = include_str!("../../../fixtures/alpaca/order_lifecycle.jsonl");
    let lines: Vec<_> = fixture.lines().collect();
    let mut lifecycle = OrderLifecycle::committed(intent("v1-paper-release-decision-001", 10));
    for line in &lines[..4] {
        lifecycle
            .apply_broker_event(&event_from_fixture(line))
            .unwrap();
    }
    assert_eq!(lifecycle.phase, OrderPhase::Filled);
    assert_eq!(lifecycle.filled_quantity, WholeQuantity::new(10));
    assert_eq!(
        lifecycle.average_fill_price,
        Some("10.016".parse().unwrap())
    );
}

#[test]
fn checked_in_future_status_fails_closed() {
    let fixture = include_str!("../../../fixtures/alpaca/order_lifecycle.jsonl");
    let last = fixture.lines().last().unwrap();
    let event = event_from_fixture(last);
    let mut lifecycle = OrderLifecycle::committed(intent(&event.client_order_id, 1));
    assert!(lifecycle.apply_broker_event(&event).is_err());
    assert!(matches!(
        lifecycle.phase,
        OrderPhase::FailClosedUnknown { .. }
    ));
    assert!(!lifecycle.may_submit());
}

#[test]
fn fill_without_positive_price_fails_closed() {
    let fixture = include_str!("../../../fixtures/alpaca/order_lifecycle.jsonl");
    let mut event = event_from_fixture(fixture.lines().nth(2).unwrap());
    event.fill_price = None;
    let mut lifecycle = OrderLifecycle::committed(intent(&event.client_order_id, 10));
    assert!(lifecycle.apply_broker_event(&event).is_err());
    assert!(matches!(
        lifecycle.phase,
        OrderPhase::FailClosedUnknown { .. }
    ));
}

#[test]
fn ambiguous_submission_can_only_be_resolved_by_lookup() {
    let mut lifecycle = OrderLifecycle::committed(intent("stable-client-id", 1));
    lifecycle.begin_submission().unwrap();
    lifecycle.mark_submission_unknown("timeout").unwrap();
    assert!(lifecycle.requires_client_id_lookup());
    assert!(!lifecycle.may_submit());
    assert!(lifecycle.begin_submission().is_err());
}

#[test]
fn ledger_commits_before_submission_and_is_idempotent() {
    let at: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
    let order = intent("stable-client-id", 1);
    let mut ledger = ExecutionLedger::default();
    let authority = OutboxAuthority {
        environment: Environment::Paper,
        account_fingerprint: HashDigest::sha256("paper-account"),
        created_fencing_token: 7,
    };
    let first = ledger.commit_intent(order.clone(), &authority, at).unwrap();
    let duplicate = ledger.commit_intent(order.clone(), &authority, at).unwrap();
    assert_eq!(first, duplicate);
    assert_eq!(ledger.records().len(), 1);
    assert_eq!(ledger.outbox().len(), 1);
    let mut economically_changed = order.clone();
    economically_changed.limit_price = "10.06".parse().unwrap();
    assert!(ledger
        .commit_intent(economically_changed, &authority, at)
        .is_err());
    assert_eq!(ledger.outbox()[0].created_fencing_token, 7);
    ledger.claim_outbox(first, "owner-2", 8, at).unwrap();
    assert!(ledger.claim_outbox(first, "owner-3", 7, at).is_err());
    assert_eq!(ledger.outbox()[0].created_fencing_token, 7);
    assert_eq!(ledger.outbox()[0].claim_fencing_token, Some(8));

    ledger
        .append(
            ExecutionEvent::SubmissionStarted {
                client_order_id: order.client_order_id.clone(),
            },
            at,
        )
        .unwrap();
    ledger
        .append(
            ExecutionEvent::SubmissionUnknown {
                client_order_id: order.client_order_id.clone(),
                detail: "connection reset".into(),
            },
            at,
        )
        .unwrap();
    let projected = ledger.project_orders().unwrap();
    assert!(projected[&order.client_order_id].requires_client_id_lookup());
}

#[test]
fn outbox_completion_cannot_be_written_twice() {
    let at: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
    let mut ledger = ExecutionLedger::default();
    let sequence = ledger
        .commit_intent(
            intent("single-completion", 1),
            &OutboxAuthority {
                environment: Environment::Paper,
                account_fingerprint: HashDigest::sha256("paper-account"),
                created_fencing_token: 7,
            },
            at,
        )
        .unwrap();
    ledger.claim_outbox(sequence, "owner", 7, at).unwrap();
    ledger
        .mark_outbox_completed(sequence, "owner", 7, at)
        .unwrap();

    assert!(ledger
        .mark_outbox_completed(sequence, "owner", 7, at)
        .is_err());
}
