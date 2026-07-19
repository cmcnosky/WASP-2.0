//! Provider-free deterministic execution and portfolio-performance replay.
//!
//! This module consumes immutable evidence and calls the same strategy, risk,
//! order-planning, and intent-materialization functions used by production. It
//! deliberately does not perform statistical certification: a successful
//! replay produces performance measurements, never permission to trade.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    accounting::{replay as replay_accounting, AccountingEvent, AccountingState},
    domain::{
        AccountPosition, AccountSnapshot, AccountStatus, CompletedObservation, DecisionSchedule,
        DecisionSnapshot, FreshExecutionQuote, HashDigest, OrderIntent, OrderSide,
        RebalanceCadence, RiskLimitSnapshot, StrategyRelease, Symbol, WholeQuantity,
    },
    error::{CoreError, CoreResult},
    fixed::{Fixed, Money, Price},
    pipeline::{evaluate_decision, materialize_order_intent, EvaluationResult},
};

const MAX_SYNTHETIC_SESSIONS: usize = 512;
const MAX_SYNTHETIC_DECISIONS: usize = 64;
const MIN_SYNTHETIC_SYMBOLS: usize = 8;
const MAX_SYNTHETIC_SYMBOLS: usize = 12;
const MAX_OBSERVATION_PARTITIONS: usize = MAX_SYNTHETIC_DECISIONS + 1;
const MAX_EXECUTION_QUOTE_HASHES: usize = MAX_SYNTHETIC_DECISIONS * MAX_OUTCOMES_PER_DECISION;
const MAX_OBSERVATIONS_PER_DECISION: usize = 8_192;
const MAX_TOTAL_DECISION_OBSERVATIONS: usize = 131_072;
const MAX_OUTCOMES_PER_DECISION: usize = 4;
const MAX_FILLS_PER_OUTCOME: usize = 64;
const MAX_TOTAL_DIVIDENDS: usize = 4_096;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_DESCRIPTOR_BYTES: usize = 256;

/// Serialized request ceiling enforced before parsing at every public
/// performance-replay boundary.
pub const MAX_PERFORMANCE_REQUEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchStage {
    Synthetic,
    Development,
    Validation,
    Holdout,
    ProspectiveShadow,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CertifiedSession {
    pub session: NaiveDate,
    pub regular_open_at: DateTime<Utc>,
    pub regular_close_at: DateTime<Utc>,
    pub eligible_cadences: Vec<RebalanceCadence>,
    pub calendar_payload_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationPartition {
    pub partition_id: String,
    pub through_session: NaiveDate,
    pub available_at: DateTime<Utc>,
    pub rows_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DividendPartition {
    pub partition_id: String,
    pub available_at: DateTime<Utc>,
    pub event_count: u32,
    pub events_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub dataset_id: String,
    pub stage: ResearchStage,
    pub source: String,
    pub feed: String,
    pub adjustment_mode: String,
    pub symbols: Vec<Symbol>,
    pub sessions: Vec<CertifiedSession>,
    pub evaluation_start: NaiveDate,
    pub evaluation_end: NaiveDate,
    pub observation_partitions: Vec<ObservationPartition>,
    pub dividend_partitions: Vec<DividendPartition>,
    pub execution_quote_hashes: Vec<HashDigest>,
    pub raw_objects_hash: HashDigest,
    pub normalized_rows_hash: HashDigest,
    pub execution_rows_hash: HashDigest,
    pub corporate_actions_hash: HashDigest,
    pub unresolved_critical_defects: u32,
    pub certified_at: Option<DateTime<Utc>>,
    pub certifier_subject: Option<String>,
}

impl DatasetManifest {
    pub fn manifest_hash(&self) -> CoreResult<HashDigest> {
        HashDigest::of_json(self)
    }

    fn validate(&self, release: &StrategyRelease) -> CoreResult<()> {
        if self.dataset_id.trim().is_empty()
            || self.source.trim().is_empty()
            || self.feed.trim().is_empty()
            || self.adjustment_mode.trim().is_empty()
        {
            return Err(CoreError::InvalidDomain(
                "dataset manifest identity is incomplete".into(),
            ));
        }
        if self.symbols != release.universe {
            return Err(CoreError::InvalidDomain(
                "dataset symbols do not exactly match the released universe".into(),
            ));
        }
        if !(MIN_SYNTHETIC_SYMBOLS..=MAX_SYNTHETIC_SYMBOLS).contains(&self.symbols.len())
            || self.observation_partitions.len() > MAX_OBSERVATION_PARTITIONS
            || self.dividend_partitions.len() > MAX_OBSERVATION_PARTITIONS
            || self.execution_quote_hashes.len() > MAX_EXECUTION_QUOTE_HASHES
        {
            return Err(CoreError::InvalidDomain(
                "bounded synthetic manifest exceeds its symbol, partition, or quote limits".into(),
            ));
        }
        let unique_symbols: BTreeSet<_> = self.symbols.iter().collect();
        if unique_symbols.len() != self.symbols.len() {
            return Err(CoreError::InvalidDomain(
                "dataset manifest contains duplicate symbols".into(),
            ));
        }
        if self.sessions.is_empty() || self.sessions.len() > MAX_SYNTHETIC_SESSIONS {
            return Err(CoreError::InvalidDomain(
                "bounded synthetic dataset has an invalid certified-session count".into(),
            ));
        }
        let mut previous = None;
        for session in &self.sessions {
            if previous.is_some_and(|value| session.session <= value) {
                return Err(CoreError::InvalidDomain(
                    "dataset sessions are duplicate or out of order".into(),
                ));
            }
            if session.regular_open_at.date_naive() != session.session
                || session.regular_close_at.date_naive() != session.session
                || session.regular_open_at >= session.regular_close_at
            {
                return Err(CoreError::InvalidDomain(
                    "dataset regular session has invalid open/close evidence".into(),
                ));
            }
            let eligible: BTreeSet<_> = session.eligible_cadences.iter().collect();
            if eligible.len() != session.eligible_cadences.len() {
                return Err(CoreError::InvalidDomain(
                    "dataset session has duplicate cadence eligibility".into(),
                ));
            }
            previous = Some(session.session);
        }
        if self.evaluation_start >= self.evaluation_end
            || !self
                .sessions
                .iter()
                .any(|session| session.session == self.evaluation_start)
            || !self
                .sessions
                .iter()
                .any(|session| session.session == self.evaluation_end)
        {
            return Err(CoreError::InvalidDomain(
                "dataset evaluation window is invalid or absent from its calendar".into(),
            ));
        }
        validate_partition_ids(
            self.observation_partitions
                .iter()
                .map(|partition| partition.partition_id.as_str()),
            "observation",
        )?;
        validate_partition_ids(
            self.dividend_partitions
                .iter()
                .map(|partition| partition.partition_id.as_str()),
            "dividend",
        )?;
        if self.observation_partitions.iter().any(|partition| {
            !self
                .sessions
                .iter()
                .any(|session| session.session == partition.through_session)
        }) {
            return Err(CoreError::InvalidDomain(
                "observation partition ends outside the certified calendar".into(),
            ));
        }
        if HashDigest::of_json(&self.observation_partitions)? != self.normalized_rows_hash
            || HashDigest::of_json(&self.dividend_partitions)? != self.corporate_actions_hash
            || HashDigest::of_json(&self.execution_quote_hashes)? != self.execution_rows_hash
        {
            return Err(CoreError::InvalidDomain(
                "dataset evidence roots do not bind their exact partitions".into(),
            ));
        }
        if self.execution_quote_hashes.is_empty()
            || self
                .execution_quote_hashes
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(CoreError::InvalidDomain(
                "execution quote evidence hashes must be nonempty, unique, and sorted".into(),
            ));
        }
        if self.unresolved_critical_defects != 0 {
            return Err(CoreError::InvalidDomain(
                "dataset manifest has unresolved critical defects".into(),
            ));
        }
        match (&self.certified_at, &self.certifier_subject) {
            (Some(_), Some(subject)) if !subject.trim().is_empty() => {}
            (None, None) if self.stage == ResearchStage::Synthetic => {}
            _ => {
                return Err(CoreError::InvalidDomain(
                    "non-synthetic data needs explicit certification evidence".into(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_partition_ids<'a>(ids: impl Iterator<Item = &'a str>, kind: &str) -> CoreResult<()> {
    let mut seen = BTreeSet::new();
    for id in ids {
        if id.trim().is_empty() || !seen.insert(id) {
            return Err(CoreError::InvalidDomain(format!(
                "dataset {kind} partition IDs are empty or duplicate"
            )));
        }
    }
    if seen.is_empty() {
        return Err(CoreError::InvalidDomain(format!(
            "dataset contains no {kind} partitions"
        )));
    }
    Ok(())
}

pub fn execution_quote_evidence_hash(quote: &FreshExecutionQuote) -> CoreResult<HashDigest> {
    HashDigest::of_json(&(
        &quote.symbol,
        quote.raw_price,
        quote.provider_at,
        quote.received_at,
        quote.valid_until,
        quote.payload_hash,
    ))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceCostModel {
    pub model_id: String,
    pub decision_to_arrival_latency_ms: u32,
    pub half_spread_bps: u16,
    pub adverse_slippage_bps: u16,
    pub opportunity_cost_bps: u16,
    pub non_fill_probability_bps: u16,
    pub partial_fill_probability_bps: u16,
    pub minimum_order_fee: Money,
    pub stress_variable_cost_multiplier_bps: u32,
    pub stress_empirical_percentile_bps: u16,
}

impl PerformanceCostModel {
    pub fn model_hash(&self) -> CoreResult<HashDigest> {
        HashDigest::of_json(self)
    }

    fn validate(&self) -> CoreResult<()> {
        if self.model_id != "locked-cost-v1"
            || self.decision_to_arrival_latency_ms != 500
            || self.half_spread_bps != 5
            || self.adverse_slippage_bps != 5
            || self.opportunity_cost_bps != 5
            || self.non_fill_probability_bps != 500
            || self.partial_fill_probability_bps != 1_000
            || self.minimum_order_fee != Money::ZERO
            || self.stress_variable_cost_multiplier_bps != 20_000
            || self.stress_empirical_percentile_bps != 9_500
        {
            return Err(CoreError::InvalidDomain(
                "performance cost model does not exactly match the frozen v1 preregistration"
                    .into(),
            ));
        }
        Ok(())
    }

    fn adverse_fill_price(&self, quote: Price, side: OrderSide) -> CoreResult<Price> {
        let total_bps = u32::from(self.half_spread_bps)
            .checked_add(u32::from(self.adverse_slippage_bps))
            .ok_or(CoreError::ArithmeticOverflow(
                "modeled adverse basis points",
            ))?;
        if total_bps > 10_000 {
            return Err(CoreError::InvalidDomain(
                "modeled spread and slippage exceed one hundred percent".into(),
            ));
        }
        let fraction = Fixed::from_scaled(i128::from(total_bps) * 100);
        let factor = match side {
            OrderSide::Buy => Fixed::ONE.checked_add(fraction)?,
            OrderSide::Sell => Fixed::ONE.checked_sub(fraction)?,
        };
        if !factor.is_positive() {
            return Err(CoreError::InvalidDomain(
                "modeled adverse price is non-positive".into(),
            ));
        }
        Ok(Price(quote.fixed().checked_mul(factor)?))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HoldoutAccessPermit {
    pub permit_id: String,
    pub run_id: String,
    pub preregistration_hash: HashDigest,
    pub dataset_manifest_hash: HashDigest,
    pub operator_subject: String,
    pub one_shot: bool,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approval_digest: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceDecisionInput {
    pub decision_id: String,
    pub as_of: DateTime<Utc>,
    pub market_session: NaiveDate,
    pub schedule: DecisionSchedule,
    pub observations: Vec<CompletedObservation>,
    pub input_data_hash: HashDigest,
    pub observation_partition_id: String,
    pub dividend_partition_id: String,
    pub outcomes: Vec<PerformanceOrderOutcome>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalValuation {
    pub session: NaiveDate,
    pub as_of: DateTime<Utc>,
    pub observations: Vec<CompletedObservation>,
    pub input_data_hash: HashDigest,
    pub observation_partition_id: String,
    pub dividend_partition_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DividendEvidence {
    pub event_id: String,
    pub symbol: Symbol,
    pub amount_per_share: Price,
    pub record_at: DateTime<Utc>,
    pub payable_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub payload_hash: HashDigest,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTerminalReason {
    Filled,
    PartiallyFilledCancelled,
    PartiallyFilledExpired,
    CancelledNoFill,
    ExpiredNoFill,
    RejectedNoFill,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceFill {
    pub fill_id: String,
    pub quantity: WholeQuantity,
    pub price: Price,
    pub fee: Money,
    pub filled_at: DateTime<Utc>,
    pub payload_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceOrderOutcome {
    pub plan_id: String,
    pub execution_session: NaiveDate,
    pub quote: FreshExecutionQuote,
    pub quote_evidence_hash: HashDigest,
    pub submitted_at: DateTime<Utc>,
    pub fills: Vec<PerformanceFill>,
    pub terminal_at: DateTime<Utc>,
    pub terminal_reason: ExecutionTerminalReason,
    pub terminal_payload_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceBacktestRequest {
    pub run_id: String,
    pub run_at: DateTime<Utc>,
    pub preregistration_hash: HashDigest,
    pub release: StrategyRelease,
    pub risk_limits: RiskLimitSnapshot,
    pub account_fingerprint: HashDigest,
    pub initial_cash: Money,
    pub started_at: DateTime<Utc>,
    pub dataset_manifest: DatasetManifest,
    pub dataset_manifest_hash: HashDigest,
    pub cost_model: PerformanceCostModel,
    pub cost_model_hash: HashDigest,
    pub dividend_events: Vec<DividendEvidence>,
    pub holdout_access: Option<HoldoutAccessPermit>,
    pub decisions: Vec<PerformanceDecisionInput>,
    pub terminal_valuation: TerminalValuation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceExecutionResult {
    pub plan_id: String,
    pub intent: OrderIntent,
    pub terminal_reason: ExecutionTerminalReason,
    pub filled_quantity: WholeQuantity,
    pub fees: Money,
    pub modeled_opportunity_cost: Money,
    pub terminal_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceReplayStep {
    pub decision_id: String,
    pub decision_snapshot_hash: HashDigest,
    pub decision_account: AccountSnapshot,
    pub evaluation: EvaluationResult,
    pub executions: Vec<PerformanceExecutionResult>,
    pub accounting_after_execution: AccountingState,
    pub marked_equity_before_execution: Money,
    pub marked_equity_after_execution: Money,
    pub step_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceBacktestResult {
    pub run_id: String,
    pub release_id: String,
    pub request_hash: HashDigest,
    pub dataset_manifest_hash: HashDigest,
    pub cost_model_hash: HashDigest,
    pub decision_evaluation_start: NaiveDate,
    pub covered_through_decision_session: NaiveDate,
    pub last_eligible_decision_session: NaiveDate,
    pub decision_window_complete: bool,
    pub last_execution_session: Option<NaiveDate>,
    pub execution_lifecycles_complete: bool,
    pub terminal_valuation_session: NaiveDate,
    pub terminal_valuation_hash: HashDigest,
    pub terminal_valuation_complete: bool,
    pub evaluation_complete: bool,
    pub steps: Vec<PerformanceReplayStep>,
    pub final_accounting: AccountingState,
    pub ending_equity: Money,
    pub net_pnl: Money,
    pub close_and_execution_max_drawdown: Money,
    pub modeled_opportunity_cost: Money,
    pub net_pnl_after_modeled_opportunity_cost: Money,
    pub mechanical_metrics_available: bool,
    pub stressed_performance_evidence_available: bool,
    pub qualifies_as_strategy_evidence: bool,
    pub hold_reasons: Vec<String>,
    pub holdout_consumption_hash: Option<HashDigest>,
    pub result_hash: HashDigest,
}

pub fn run_performance_backtest(
    request: &PerformanceBacktestRequest,
) -> CoreResult<PerformanceBacktestResult> {
    validate_request(request)?;
    let request_hash = HashDigest::of_json(request)?;
    let holdout_consumption_hash = None;

    let mut accounting_events = vec![AccountingEvent::CashDeposit {
        amount: request.initial_cash,
        at: request.started_at,
    }];
    let mut previous_boundary = request.started_at;
    let mut peak_equity = request.initial_cash;
    let mut close_and_execution_max_drawdown = Money::ZERO;
    let mut last_drawdown_session = None;
    let mut seen_decision_ids = BTreeSet::new();
    let mut applied_dividends = BTreeSet::new();
    let mut seen_fill_ids = BTreeSet::new();
    let mut steps = Vec::with_capacity(request.decisions.len());

    for input in &request.decisions {
        if input.as_of <= previous_boundary {
            return Err(CoreError::InvalidDomain(
                "performance decisions overlap prior execution or are out of order".into(),
            ));
        }
        if input.decision_id.trim().is_empty() || !seen_decision_ids.insert(&input.decision_id) {
            return Err(CoreError::InvalidDomain(
                "performance decision IDs are empty or duplicate".into(),
            ));
        }
        apply_due_dividends(
            request,
            input.as_of,
            DividendTimestampTie::BeforeBoundary,
            &mut applied_dividends,
            &mut accounting_events,
        )?;
        let mut accounting = replay_accounting(&accounting_events)?;

        validate_observation_coverage(request, input)?;
        let session_index = request
            .dataset_manifest
            .sessions
            .iter()
            .position(|session| session.session == input.market_session)
            .ok_or_else(|| {
                CoreError::InvalidDomain("decision session is absent from the manifest".into())
            })?;
        let prior_session = session_index
            .checked_sub(1)
            .and_then(|index| request.dataset_manifest.sessions.get(index))
            .ok_or_else(|| {
                CoreError::InvalidDomain(
                    "daily P&L requires a certified prior regular session".into(),
                )
            })?;
        let prior_accounting =
            accounting_as_of(&accounting_events, prior_session.regular_close_at)?;
        let prior_marks = marks_for_session(&input.observations, prior_session.session)?;
        let prior_session_close_equity = marked_equity(&prior_accounting, &prior_marks)?;

        for session in &request.dataset_manifest.sessions {
            if session.session < request.dataset_manifest.evaluation_start
                || session.session > input.market_session
                || last_drawdown_session.is_some_and(|last| session.session <= last)
            {
                continue;
            }
            let session_accounting =
                accounting_as_of(&accounting_events, session.regular_close_at)?;
            let session_marks = marks_for_session(&input.observations, session.session)?;
            let session_equity = marked_equity(&session_accounting, &session_marks)?;
            update_drawdown(
                session_equity,
                &mut peak_equity,
                &mut close_and_execution_max_drawdown,
            )?;
            last_drawdown_session = Some(session.session);
        }

        let marks = marks_for_session(&input.observations, input.market_session)?;
        let marked_equity_before_execution = marked_equity(&accounting, &marks)?;
        let drawdown = update_drawdown(
            marked_equity_before_execution,
            &mut peak_equity,
            &mut close_and_execution_max_drawdown,
        )?;
        let day_pnl = marked_equity_before_execution.checked_sub(prior_session_close_equity)?;
        let account = account_from_accounting(
            request.account_fingerprint,
            &accounting,
            &marks,
            marked_equity_before_execution,
            day_pnl,
            drawdown,
        )?;
        let account_snapshot_hash = HashDigest::of_json(&account)?;
        let snapshot = DecisionSnapshot {
            decision_id: input.decision_id.clone(),
            release_id: request.release.release_id.clone(),
            as_of: input.as_of,
            market_session: input.market_session,
            schedule: input.schedule.clone(),
            account,
            account_snapshot_hash,
            observations: input.observations.clone(),
            input_data_hash: input.input_data_hash,
        };
        let decision_snapshot_hash = HashDigest::of_json(&snapshot)?;
        let evaluation = evaluate_decision(&snapshot, &request.release, &request.risk_limits)?;
        let executions = execute_outcomes(
            request,
            &snapshot,
            &evaluation,
            input,
            &mut accounting_events,
            &mut applied_dividends,
            &mut seen_fill_ids,
        )?;
        accounting = replay_accounting(&accounting_events)?;
        if accounting.cash.is_negative() {
            return Err(CoreError::AccountingInvariant(
                "performance replay attempted leveraged spending".into(),
            ));
        }
        let terminal_at = executions
            .last()
            .map(|execution| execution.terminal_at)
            .unwrap_or(input.as_of);
        let mut post_execution_marks = marks;
        for execution in &executions {
            post_execution_marks.insert(
                execution.intent.symbol.clone(),
                execution.intent.arrival_quote,
            );
        }
        let marked_equity_after_execution = marked_equity(&accounting, &post_execution_marks)?;
        update_drawdown(
            marked_equity_after_execution,
            &mut peak_equity,
            &mut close_and_execution_max_drawdown,
        )?;
        previous_boundary = terminal_at;
        let step_hash = HashDigest::of_json(&(
            decision_snapshot_hash,
            &snapshot.decision_id,
            &evaluation,
            &executions,
            &accounting,
            marked_equity_before_execution,
            marked_equity_after_execution,
        ))?;
        steps.push(PerformanceReplayStep {
            decision_id: snapshot.decision_id,
            decision_snapshot_hash,
            decision_account: snapshot.account,
            evaluation,
            executions,
            accounting_after_execution: accounting.clone(),
            marked_equity_before_execution,
            marked_equity_after_execution,
            step_hash,
        });
    }

    apply_due_dividends(
        request,
        request.terminal_valuation.as_of,
        DividendTimestampTie::BeforeBoundary,
        &mut applied_dividends,
        &mut accounting_events,
    )?;
    if applied_dividends.len() != request.dividend_events.len() {
        return Err(CoreError::InvalidDomain(
            "terminal valuation did not consume the complete dividend stream".into(),
        ));
    }
    let accounting = replay_accounting(&accounting_events)?;
    let terminal_marks = marks_for_session(
        &request.terminal_valuation.observations,
        request.terminal_valuation.session,
    )?;
    let ending_equity = marked_equity(&accounting, &terminal_marks)?;
    update_drawdown(
        ending_equity,
        &mut peak_equity,
        &mut close_and_execution_max_drawdown,
    )?;
    let terminal_valuation_hash = HashDigest::of_json(&request.terminal_valuation)?;
    let net_pnl = ending_equity.checked_sub(request.initial_cash)?;
    let modeled_opportunity_cost = steps.iter().try_fold(Money::ZERO, |total, step| {
        step.executions
            .iter()
            .try_fold(total, |subtotal, execution| {
                subtotal.checked_add(execution.modeled_opportunity_cost)
            })
    })?;
    let net_pnl_after_modeled_opportunity_cost = net_pnl.checked_sub(modeled_opportunity_cost)?;
    let covered_through_decision_session = request
        .decisions
        .last()
        .expect("validated nonempty decisions")
        .market_session;
    let last_eligible_decision_session = request
        .dataset_manifest
        .sessions
        .iter()
        .rev()
        .find(|session| {
            session.session >= request.dataset_manifest.evaluation_start
                && session.session < request.dataset_manifest.evaluation_end
        })
        .expect("validated decision window")
        .session;
    let decision_window_complete =
        covered_through_decision_session == last_eligible_decision_session;
    let last_execution_session = request
        .decisions
        .iter()
        .flat_map(|decision| decision.outcomes.iter())
        .map(|outcome| outcome.execution_session)
        .max();
    let execution_lifecycles_complete = true;
    let terminal_valuation_complete = true;
    let evaluation_complete =
        decision_window_complete && execution_lifecycles_complete && terminal_valuation_complete;
    let mut hold_reasons = evidence_holds(request);
    if !decision_window_complete {
        hold_reasons.push("decision_evaluation_window_is_only_a_prefix".into());
        hold_reasons.push("prefix_close_drawdown_metric_is_incomplete".into());
    }
    let qualifies_as_strategy_evidence = false;
    let mechanical_metrics_available = decision_window_complete;
    let stressed_performance_evidence_available = false;
    let result_hash = HashDigest::of_json(&(
        (
            &request.run_id,
            &request.release.release_id,
            request_hash,
            request.dataset_manifest_hash,
            request.cost_model_hash,
            request.dataset_manifest.evaluation_start,
            covered_through_decision_session,
            last_eligible_decision_session,
            decision_window_complete,
            last_execution_session,
            execution_lifecycles_complete,
            request.dataset_manifest.evaluation_end,
            terminal_valuation_hash,
            terminal_valuation_complete,
            evaluation_complete,
        ),
        (
            &steps,
            &accounting,
            ending_equity,
            net_pnl,
            close_and_execution_max_drawdown,
            modeled_opportunity_cost,
            net_pnl_after_modeled_opportunity_cost,
            mechanical_metrics_available,
            stressed_performance_evidence_available,
            qualifies_as_strategy_evidence,
            &hold_reasons,
            holdout_consumption_hash,
        ),
    ))?;
    Ok(PerformanceBacktestResult {
        run_id: request.run_id.clone(),
        release_id: request.release.release_id.clone(),
        request_hash,
        dataset_manifest_hash: request.dataset_manifest_hash,
        cost_model_hash: request.cost_model_hash,
        decision_evaluation_start: request.dataset_manifest.evaluation_start,
        covered_through_decision_session,
        last_eligible_decision_session,
        decision_window_complete,
        last_execution_session,
        execution_lifecycles_complete,
        terminal_valuation_session: request.dataset_manifest.evaluation_end,
        terminal_valuation_hash,
        terminal_valuation_complete,
        evaluation_complete,
        steps,
        final_accounting: accounting,
        ending_equity,
        net_pnl,
        close_and_execution_max_drawdown,
        modeled_opportunity_cost,
        net_pnl_after_modeled_opportunity_cost,
        mechanical_metrics_available,
        stressed_performance_evidence_available,
        qualifies_as_strategy_evidence,
        hold_reasons,
        holdout_consumption_hash,
        result_hash,
    })
}

fn validate_request(request: &PerformanceBacktestRequest) -> CoreResult<()> {
    request.release.validate()?;
    request.risk_limits.validate()?;
    request.dataset_manifest.validate(&request.release)?;
    request.cost_model.validate()?;
    if request.run_id.trim().is_empty()
        || !request.initial_cash.fixed().is_positive()
        || request.started_at >= request.run_at
        || request.decisions.is_empty()
        || request.decisions.len() > MAX_SYNTHETIC_DECISIONS
    {
        return Err(CoreError::InvalidDomain(
            "bounded performance request identity, size, cash, or time bounds are invalid".into(),
        ));
    }
    if request.terminal_valuation.observations.len() > MAX_OBSERVATIONS_PER_DECISION {
        return Err(CoreError::InvalidDomain(
            "bounded terminal valuation exceeds its observation limit".into(),
        ));
    }
    let total_observations = request.decisions.iter().try_fold(
        request.terminal_valuation.observations.len(),
        |total, decision| {
            if decision.observations.len() > MAX_OBSERVATIONS_PER_DECISION
                || decision.outcomes.len() > MAX_OUTCOMES_PER_DECISION
                || decision
                    .outcomes
                    .iter()
                    .any(|outcome| outcome.fills.len() > MAX_FILLS_PER_OUTCOME)
            {
                return Err(CoreError::InvalidDomain(
                    "bounded synthetic decision exceeds an observation, outcome, or fill limit"
                        .into(),
                ));
            }
            total
                .checked_add(decision.observations.len())
                .ok_or(CoreError::ArithmeticOverflow(
                    "synthetic decision observation count",
                ))
        },
    )?;
    if total_observations > MAX_TOTAL_DECISION_OBSERVATIONS {
        return Err(CoreError::InvalidDomain(
            "bounded synthetic request exceeds its total observation limit".into(),
        ));
    }
    if request.dividend_events.len() > MAX_TOTAL_DIVIDENDS {
        return Err(CoreError::InvalidDomain(
            "bounded synthetic request exceeds its total dividend limit".into(),
        ));
    }
    if request.dataset_manifest.manifest_hash()? != request.dataset_manifest_hash
        || request.release.data_hash != request.dataset_manifest_hash
    {
        return Err(CoreError::InvalidDomain(
            "released data hash does not bind the exact dataset manifest".into(),
        ));
    }
    if request.cost_model.model_hash()? != request.cost_model_hash
        || request.release.cost_model_hash != request.cost_model_hash
    {
        return Err(CoreError::InvalidDomain(
            "released cost hash does not bind the exact performance model".into(),
        ));
    }
    validate_bounded_text(request)?;
    validate_dividend_stream(request)?;
    if request
        .decisions
        .last()
        .is_some_and(|last| last.as_of >= request.run_at)
    {
        return Err(CoreError::InvalidDomain(
            "performance request was recorded before its decision evidence".into(),
        ));
    }
    if request
        .dataset_manifest
        .certified_at
        .is_some_and(|certified_at| certified_at >= request.run_at)
        || request.decisions.iter().any(|decision| {
            decision
                .outcomes
                .iter()
                .any(|outcome| outcome.terminal_at >= request.run_at)
        })
    {
        return Err(CoreError::InvalidDomain(
            "performance evidence or certification is not available by run time".into(),
        ));
    }
    if request.dataset_manifest.stage != ResearchStage::Synthetic
        || request.holdout_access.is_some()
    {
        return Err(CoreError::InvalidDomain(
            "certified development, validation, holdout, and prospective replay remain HOLD until trusted data and durable authority verification are implemented"
                .into(),
        ));
    }
    let expected_sessions: Vec<_> = request
        .dataset_manifest
        .sessions
        .iter()
        .filter(|session| {
            session.session >= request.dataset_manifest.evaluation_start
                && session.session < request.dataset_manifest.evaluation_end
        })
        .collect();
    let evaluation_start_index = request
        .dataset_manifest
        .sessions
        .iter()
        .position(|session| session.session == request.dataset_manifest.evaluation_start)
        .ok_or_else(|| CoreError::InvalidDomain("evaluation start is not certified".into()))?;
    let prior_evaluation_session = evaluation_start_index
        .checked_sub(1)
        .and_then(|index| request.dataset_manifest.sessions.get(index))
        .ok_or_else(|| {
            CoreError::InvalidDomain(
                "bounded performance evaluation needs one prior certified session".into(),
            )
        })?;
    if request.started_at > prior_evaluation_session.regular_close_at {
        return Err(CoreError::InvalidDomain(
            "initial accounting starts after the prior-session P&L baseline".into(),
        ));
    }
    if request.decisions.len() > expected_sessions.len()
        || request
            .decisions
            .iter()
            .zip(expected_sessions)
            .any(|(decision, session)| decision.market_session != session.session)
    {
        return Err(CoreError::InvalidDomain(
            "performance decisions must be a gap-free prefix of the certified evaluation calendar"
                .into(),
        ));
    }
    validate_terminal_valuation(request)?;
    Ok(())
}

fn bounded_text(value: &str, maximum_bytes: usize) -> bool {
    !value.trim().is_empty() && value == value.trim() && value.len() <= maximum_bytes
}

fn validate_bounded_text(request: &PerformanceBacktestRequest) -> CoreResult<()> {
    let manifest = &request.dataset_manifest;
    let descriptors = [
        manifest.source.as_str(),
        manifest.feed.as_str(),
        manifest.adjustment_mode.as_str(),
    ];
    let identifiers_are_valid = bounded_text(&request.run_id, MAX_IDENTIFIER_BYTES)
        && bounded_text(&request.release.release_id, MAX_IDENTIFIER_BYTES)
        && bounded_text(&manifest.dataset_id, MAX_IDENTIFIER_BYTES)
        && bounded_text(&request.cost_model.model_id, MAX_IDENTIFIER_BYTES)
        && descriptors
            .iter()
            .all(|value| bounded_text(value, MAX_DESCRIPTOR_BYTES))
        && manifest
            .certifier_subject
            .as_deref()
            .is_none_or(|value| bounded_text(value, MAX_DESCRIPTOR_BYTES))
        && manifest
            .observation_partitions
            .iter()
            .all(|partition| bounded_text(&partition.partition_id, MAX_IDENTIFIER_BYTES))
        && manifest
            .dividend_partitions
            .iter()
            .all(|partition| bounded_text(&partition.partition_id, MAX_IDENTIFIER_BYTES))
        && request.decisions.iter().all(|decision| {
            bounded_text(&decision.decision_id, MAX_IDENTIFIER_BYTES)
                && bounded_text(&decision.observation_partition_id, MAX_IDENTIFIER_BYTES)
                && bounded_text(&decision.dividend_partition_id, MAX_IDENTIFIER_BYTES)
                && decision.outcomes.iter().all(|outcome| {
                    bounded_text(&outcome.plan_id, MAX_IDENTIFIER_BYTES)
                        && outcome
                            .fills
                            .iter()
                            .all(|fill| bounded_text(&fill.fill_id, MAX_IDENTIFIER_BYTES))
                })
        })
        && bounded_text(
            &request.terminal_valuation.observation_partition_id,
            MAX_IDENTIFIER_BYTES,
        )
        && bounded_text(
            &request.terminal_valuation.dividend_partition_id,
            MAX_IDENTIFIER_BYTES,
        )
        && request
            .dividend_events
            .iter()
            .all(|event| bounded_text(&event.event_id, MAX_IDENTIFIER_BYTES))
        && request.holdout_access.as_ref().is_none_or(|permit| {
            bounded_text(&permit.permit_id, MAX_IDENTIFIER_BYTES)
                && bounded_text(&permit.run_id, MAX_IDENTIFIER_BYTES)
                && bounded_text(&permit.operator_subject, MAX_DESCRIPTOR_BYTES)
        });
    if !identifiers_are_valid {
        return Err(CoreError::InvalidDomain(
            "performance request contains an empty, padded, or oversized identifier".into(),
        ));
    }
    Ok(())
}

fn validate_dividend_stream(request: &PerformanceBacktestRequest) -> CoreResult<()> {
    let mut seen_event_ids = BTreeSet::new();
    let mut previous_event = None;
    for dividend in &request.dividend_events {
        let order_key = (dividend.available_at, dividend.event_id.as_str());
        let effective_at = dividend_effective_at(dividend);
        if !seen_event_ids.insert(dividend.event_id.as_str())
            || previous_event.is_some_and(|previous| order_key <= previous)
            || !request.release.universe.contains(&dividend.symbol)
            || !dividend.amount_per_share.fixed().is_positive()
            || dividend.record_at < request.started_at
            || dividend.record_at > dividend.payable_at
            || effective_at <= request.started_at
            || effective_at > request.terminal_valuation.as_of
            || dividend.available_at >= request.run_at
        {
            return Err(CoreError::InvalidDomain(
                "dividend stream is invalid, duplicate, unavailable, or noncanonical".into(),
            ));
        }
        previous_event = Some(order_key);
    }

    let mut previous_partition = None;
    for partition in &request.dataset_manifest.dividend_partitions {
        let count = usize::try_from(partition.event_count).map_err(|_| {
            CoreError::InvalidDomain("dividend partition count does not fit this runtime".into())
        })?;
        if count > request.dividend_events.len()
            || previous_partition.is_some_and(|(available_at, event_count)| {
                partition.available_at < available_at || partition.event_count < event_count
            })
            || partition.events_hash != HashDigest::of_json(&request.dividend_events[..count])?
            || request.dividend_events[..count]
                .iter()
                .any(|event| event.available_at > partition.available_at)
            || request
                .dividend_events
                .get(count)
                .is_some_and(|event| event.available_at <= partition.available_at)
        {
            return Err(CoreError::InvalidDomain(
                "dividend partitions do not bind canonical cumulative availability prefixes".into(),
            ));
        }
        previous_partition = Some((partition.available_at, partition.event_count));
    }

    for decision in &request.decisions {
        let partition = request
            .dataset_manifest
            .dividend_partitions
            .iter()
            .find(|partition| partition.partition_id == decision.dividend_partition_id)
            .ok_or_else(|| {
                CoreError::InvalidDomain(
                    "decision dividend partition is absent from the manifest".into(),
                )
            })?;
        let available_count = request
            .dividend_events
            .iter()
            .take_while(|event| event.available_at <= decision.as_of)
            .count();
        if partition.available_at > decision.as_of
            || usize::try_from(partition.event_count).ok() != Some(available_count)
        {
            return Err(CoreError::InvalidDomain(
                "decision dividend partition omits or exposes unavailable evidence".into(),
            ));
        }
    }

    let terminal_partition = request
        .dataset_manifest
        .dividend_partitions
        .iter()
        .find(|partition| {
            partition.partition_id == request.terminal_valuation.dividend_partition_id
        })
        .ok_or_else(|| {
            CoreError::InvalidDomain(
                "terminal dividend partition is absent from the manifest".into(),
            )
        })?;
    if terminal_partition.available_at > request.terminal_valuation.as_of
        || usize::try_from(terminal_partition.event_count).ok()
            != Some(request.dividend_events.len())
    {
        return Err(CoreError::InvalidDomain(
            "terminal dividend partition does not bind the complete known stream".into(),
        ));
    }
    Ok(())
}

fn validate_terminal_valuation(request: &PerformanceBacktestRequest) -> CoreResult<()> {
    let terminal = &request.terminal_valuation;
    let certified_session = request
        .dataset_manifest
        .sessions
        .iter()
        .find(|session| session.session == terminal.session)
        .ok_or_else(|| CoreError::InvalidDomain("terminal session is not certified".into()))?;
    let latest_decision_or_execution = request
        .decisions
        .iter()
        .flat_map(|decision| {
            std::iter::once(decision.as_of)
                .chain(decision.outcomes.iter().map(|outcome| outcome.terminal_at))
        })
        .max()
        .expect("validated nonempty decisions");
    if terminal.session != request.dataset_manifest.evaluation_end
        || terminal.as_of < certified_session.regular_close_at
        || terminal.as_of <= latest_decision_or_execution
        || terminal.as_of >= request.run_at
    {
        return Err(CoreError::InvalidDomain(
            "terminal valuation is not an after-close, post-execution evaluation boundary".into(),
        ));
    }
    let partition = request
        .dataset_manifest
        .observation_partitions
        .iter()
        .find(|partition| partition.partition_id == terminal.observation_partition_id)
        .ok_or_else(|| {
            CoreError::InvalidDomain(
                "terminal observation partition is absent from the manifest".into(),
            )
        })?;
    if partition.through_session != terminal.session
        || partition.available_at > terminal.as_of
        || partition.rows_hash != terminal.input_data_hash
        || HashDigest::of_json(&terminal.observations)? != terminal.input_data_hash
    {
        return Err(CoreError::InvalidDomain(
            "terminal observations do not match their available manifest partition".into(),
        ));
    }
    let sessions: BTreeMap<_, _> = request
        .dataset_manifest
        .sessions
        .iter()
        .filter(|session| session.session <= terminal.session)
        .map(|session| (session.session, session.regular_close_at))
        .collect();
    let universe: BTreeSet<_> = request.release.universe.iter().collect();
    let mut seen = BTreeSet::new();
    let mut terminal_symbols = BTreeSet::new();
    let mut previous = None;
    for observation in &terminal.observations {
        let close_at = sessions.get(&observation.session).ok_or_else(|| {
            CoreError::InvalidDomain("terminal observation session is not certified".into())
        })?;
        let current = (&observation.symbol, observation.session);
        if !universe.contains(&observation.symbol)
            || observation.completed_at < *close_at
            || observation.completed_at > terminal.as_of
            || !observation.raw_close.fixed().is_positive()
            || !observation.total_return_close.fixed().is_positive()
            || !seen.insert(current)
            || previous.is_some_and(|prior| current < prior)
        {
            return Err(CoreError::InvalidDomain(
                "terminal observations are unavailable, duplicate, noncanonical, or invalid".into(),
            ));
        }
        if observation.session == terminal.session {
            terminal_symbols.insert(&observation.symbol);
        }
        previous = Some(current);
    }
    if terminal_symbols != universe {
        return Err(CoreError::InvalidDomain(
            "terminal valuation does not mark the exact released universe".into(),
        ));
    }
    Ok(())
}

fn validate_observation_coverage(
    request: &PerformanceBacktestRequest,
    input: &PerformanceDecisionInput,
) -> CoreResult<()> {
    let partition = request
        .dataset_manifest
        .observation_partitions
        .iter()
        .find(|partition| partition.partition_id == input.observation_partition_id)
        .ok_or_else(|| {
            CoreError::InvalidDomain(
                "decision observation partition is absent from the manifest".into(),
            )
        })?;
    if partition.through_session != input.market_session
        || partition.available_at > input.as_of
        || partition.rows_hash != input.input_data_hash
        || HashDigest::of_json(&input.observations)? != input.input_data_hash
    {
        return Err(CoreError::InvalidDomain(
            "performance observations do not match their available manifest partition".into(),
        ));
    }
    let certified_session = request
        .dataset_manifest
        .sessions
        .iter()
        .find(|session| session.session == input.market_session)
        .ok_or_else(|| {
            CoreError::InvalidDomain("decision session is absent from the manifest".into())
        })?;
    if input.schedule.eligible_cadences != certified_session.eligible_cadences
        || input.schedule.calendar_evidence_hash != certified_session.calendar_payload_hash
        || input.as_of < certified_session.regular_close_at
    {
        return Err(CoreError::InvalidDomain(
            "decision cadence or timing does not match certified calendar evidence".into(),
        ));
    }
    let required_sessions: BTreeMap<_, _> = request
        .dataset_manifest
        .sessions
        .iter()
        .filter(|session| session.session <= input.market_session)
        .map(|session| (session.session, session.regular_close_at))
        .collect();
    debug_assert!(required_sessions.contains_key(&input.market_session));
    let mut observed = BTreeSet::new();
    for observation in &input.observations {
        if !request.release.universe.contains(&observation.symbol) {
            return Err(CoreError::InvalidDomain(
                "performance observation is outside the released universe".into(),
            ));
        }
        let close_at = required_sessions.get(&observation.session).ok_or_else(|| {
            CoreError::InvalidDomain("observation session is absent from the manifest".into())
        })?;
        if observation.completed_at < *close_at || observation.completed_at > input.as_of {
            return Err(CoreError::InvalidDomain(
                "observation was not available within its certified decision window".into(),
            ));
        }
        if !observed.insert((observation.symbol.clone(), observation.session)) {
            return Err(CoreError::InvalidDomain(
                "performance observations contain a duplicate symbol/session".into(),
            ));
        }
    }
    for symbol in &request.release.universe {
        for session in required_sessions.keys() {
            if !observed.contains(&(symbol.clone(), *session)) {
                return Err(CoreError::InvalidDomain(format!(
                    "missing required strategy observation for {symbol} on {session}"
                )));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum DividendTimestampTie {
    StrictlyBeforeBoundary,
    BeforeBoundary,
}

fn dividend_effective_at(dividend: &DividendEvidence) -> DateTime<Utc> {
    dividend.payable_at.max(dividend.available_at)
}

fn apply_due_dividends(
    request: &PerformanceBacktestRequest,
    through: DateTime<Utc>,
    tie: DividendTimestampTie,
    applied: &mut BTreeSet<String>,
    accounting_events: &mut Vec<AccountingEvent>,
) -> CoreResult<()> {
    let mut due = request
        .dividend_events
        .iter()
        .filter(|dividend| {
            if applied.contains(&dividend.event_id) {
                return false;
            }
            let effective_at = dividend_effective_at(dividend);
            match tie {
                DividendTimestampTie::StrictlyBeforeBoundary => effective_at < through,
                DividendTimestampTie::BeforeBoundary => effective_at <= through,
            }
        })
        .collect::<Vec<_>>();
    due.sort_by_key(|dividend| (dividend_effective_at(dividend), &dividend.event_id));

    for dividend in due {
        let effective_at = dividend_effective_at(dividend);
        let entitled_events = accounting_events
            .iter()
            .filter(|event| accounting_event_at(event) <= dividend.record_at)
            .cloned()
            .collect::<Vec<_>>();
        let state = replay_accounting(&entitled_events)?;
        let quantity = state
            .positions
            .get(&dividend.symbol)
            .map(|position| position.quantity.get())
            .unwrap_or(0);
        let amount = dividend.amount_per_share.checked_mul_quantity(quantity)?;
        accounting_events.push(AccountingEvent::Dividend {
            amount,
            at: effective_at,
        });
        if !applied.insert(dividend.event_id.clone()) {
            return Err(CoreError::InvalidDomain(
                "dividend evidence was applied more than once".into(),
            ));
        }
    }
    Ok(())
}

fn accounting_event_at(event: &AccountingEvent) -> DateTime<Utc> {
    match event {
        AccountingEvent::CashDeposit { at, .. }
        | AccountingEvent::CashWithdrawal { at, .. }
        | AccountingEvent::Dividend { at, .. }
        | AccountingEvent::Fee { at, .. }
        | AccountingEvent::Fill { at, .. } => *at,
    }
}

fn accounting_as_of(
    events: &[AccountingEvent],
    as_of: DateTime<Utc>,
) -> CoreResult<AccountingState> {
    let available: Vec<_> = events
        .iter()
        .filter(|event| accounting_event_at(event) <= as_of)
        .cloned()
        .collect();
    replay_accounting(&available)
}

fn marks_for_session(
    observations: &[CompletedObservation],
    session: NaiveDate,
) -> CoreResult<BTreeMap<Symbol, Price>> {
    let mut marks = BTreeMap::new();
    for observation in observations
        .iter()
        .filter(|observation| observation.session == session)
    {
        if marks
            .insert(observation.symbol.clone(), observation.raw_close)
            .is_some()
        {
            return Err(CoreError::InvalidDomain(
                "session marks contain a duplicate symbol".into(),
            ));
        }
    }
    if marks.is_empty() {
        return Err(CoreError::InvalidDomain(
            "session marks are absent from the decision evidence".into(),
        ));
    }
    Ok(marks)
}

fn update_drawdown(
    equity: Money,
    peak_equity: &mut Money,
    maximum_drawdown: &mut Money,
) -> CoreResult<Money> {
    *peak_equity = (*peak_equity).max(equity);
    let drawdown = equity.checked_sub(*peak_equity)?;
    if drawdown < *maximum_drawdown {
        *maximum_drawdown = drawdown;
    }
    Ok(drawdown)
}

fn marked_equity(
    accounting: &AccountingState,
    marks: &BTreeMap<Symbol, Price>,
) -> CoreResult<Money> {
    let mut equity = accounting.cash;
    for (symbol, position) in &accounting.positions {
        if position.quantity == WholeQuantity::ZERO {
            continue;
        }
        let mark = marks.get(symbol).ok_or_else(|| {
            CoreError::InvalidDomain(format!("missing mark for open position {symbol}"))
        })?;
        equity = equity.checked_add(mark.checked_mul_quantity(position.quantity.get())?)?;
    }
    Ok(equity)
}

fn account_from_accounting(
    fingerprint: HashDigest,
    accounting: &AccountingState,
    marks: &BTreeMap<Symbol, Price>,
    equity: Money,
    day_pnl: Money,
    drawdown: Money,
) -> CoreResult<AccountSnapshot> {
    let mut positions = Vec::new();
    for (symbol, position) in &accounting.positions {
        if position.quantity == WholeQuantity::ZERO {
            continue;
        }
        let market_price = *marks.get(symbol).ok_or_else(|| {
            CoreError::InvalidDomain(format!("missing account mark for {symbol}"))
        })?;
        positions.push(AccountPosition {
            symbol: symbol.clone(),
            quantity: position.quantity,
            average_entry_price: position.average_cost,
            market_price,
        });
    }
    Ok(AccountSnapshot {
        account_fingerprint: fingerprint,
        status: AccountStatus::Active,
        trading_blocked: false,
        cash: accounting.cash,
        buying_power: accounting.cash,
        equity,
        day_pnl,
        drawdown,
        positions,
    })
}

fn execute_outcomes(
    request: &PerformanceBacktestRequest,
    snapshot: &DecisionSnapshot,
    evaluation: &EvaluationResult,
    input: &PerformanceDecisionInput,
    accounting_events: &mut Vec<AccountingEvent>,
    applied_dividends: &mut BTreeSet<String>,
    seen_fill_ids: &mut BTreeSet<String>,
) -> CoreResult<Vec<PerformanceExecutionResult>> {
    let mut outcomes = BTreeMap::new();
    for outcome in &input.outcomes {
        if outcome.plan_id.trim().is_empty() || outcomes.insert(&outcome.plan_id, outcome).is_some()
        {
            return Err(CoreError::InvalidDomain(
                "performance outcomes contain an empty or duplicate plan ID".into(),
            ));
        }
    }
    if outcomes.len() != evaluation.order_plans.len()
        || evaluation
            .order_plans
            .iter()
            .any(|plan| !outcomes.contains_key(&plan.plan_id))
    {
        return Err(CoreError::InvalidDomain(
            "performance outcomes do not exactly cover emitted order plans".into(),
        ));
    }

    let mut results = Vec::with_capacity(evaluation.order_plans.len());
    let mut previous_terminal_at = snapshot.as_of;
    for plan in &evaluation.order_plans {
        let outcome = outcomes[&plan.plan_id];
        let next_session = request
            .dataset_manifest
            .sessions
            .iter()
            .find(|session| session.session > snapshot.market_session)
            .ok_or_else(|| {
                CoreError::InvalidDomain(
                    "order plan has no next certified regular execution session".into(),
                )
            })?;
        let latency_at = snapshot
            .as_of
            .checked_add_signed(Duration::milliseconds(i64::from(
                request.cost_model.decision_to_arrival_latency_ms,
            )))
            .ok_or(CoreError::ArithmeticOverflow("modeled quote latency"))?;
        let minimum_quote_at = latency_at.max(next_session.regular_open_at);
        if outcome.execution_session != next_session.session
            || next_session.session > request.dataset_manifest.evaluation_end
            || outcome.quote_evidence_hash != execution_quote_evidence_hash(&outcome.quote)?
            || request
                .dataset_manifest
                .execution_quote_hashes
                .binary_search(&outcome.quote_evidence_hash)
                .is_err()
            || outcome.quote.provider_at < minimum_quote_at
            || outcome.quote.provider_at >= next_session.regular_close_at
            || outcome.quote.received_at >= next_session.regular_close_at
            || outcome.quote.valid_until > next_session.regular_close_at
            || outcome.submitted_at < previous_terminal_at
            || outcome.submitted_at < outcome.quote.received_at
            || outcome.submitted_at >= outcome.quote.valid_until
            || outcome.submitted_at >= request.release.expires_at
            || outcome.terminal_at < outcome.submitted_at
            || outcome.terminal_at > next_session.regular_close_at
        {
            return Err(CoreError::InvalidDomain(
                "execution evidence is not an authenticated next-session regular-hours lifecycle"
                    .into(),
            ));
        }
        apply_due_dividends(
            request,
            outcome.submitted_at,
            DividendTimestampTie::StrictlyBeforeBoundary,
            applied_dividends,
            accounting_events,
        )?;
        let intent = materialize_order_intent(
            snapshot,
            &request.release,
            &evaluation.risk,
            plan,
            &outcome.quote,
        )?;
        let adverse_price = request
            .cost_model
            .adverse_fill_price(outcome.quote.raw_price, plan.side)?;
        let mut filled_quantity = 0_u64;
        let mut fees = Money::ZERO;
        let mut previous_fill_at = outcome.submitted_at;
        for fill in &outcome.fills {
            if fill.fill_id.trim().is_empty()
                || fill.quantity == WholeQuantity::ZERO
                || !fill.price.fixed().is_positive()
                || fill.fee.is_negative()
                || fill.filled_at < previous_fill_at
                || fill.filled_at > outcome.terminal_at
                || !seen_fill_ids.insert(fill.fill_id.clone())
            {
                return Err(CoreError::InvalidDomain(
                    "fill evidence has invalid quantity, price, fee, or chronology".into(),
                ));
            }
            match plan.side {
                OrderSide::Buy if fill.price > intent.limit_price || fill.price < adverse_price => {
                    return Err(CoreError::InvalidDomain(
                        "buy fill is outside its limit or modeled adverse-cost floor".into(),
                    ));
                }
                OrderSide::Sell
                    if fill.price < intent.limit_price || fill.price > adverse_price =>
                {
                    return Err(CoreError::InvalidDomain(
                        "sell fill is outside its limit or modeled adverse-cost ceiling".into(),
                    ));
                }
                _ => {}
            }
            filled_quantity = filled_quantity
                .checked_add(fill.quantity.get())
                .ok_or(CoreError::ArithmeticOverflow("performance filled quantity"))?;
            if filled_quantity > intent.quantity.get() {
                return Err(CoreError::InvalidDomain(
                    "performance fills exceed the authorized order quantity".into(),
                ));
            }
            fees = fees.checked_add(fill.fee)?;
            apply_due_dividends(
                request,
                fill.filled_at,
                DividendTimestampTie::StrictlyBeforeBoundary,
                applied_dividends,
                accounting_events,
            )?;
            accounting_events.push(AccountingEvent::Fill {
                symbol: plan.symbol.clone(),
                side: plan.side,
                quantity: fill.quantity,
                price: fill.price,
                fee: fill.fee,
                at: fill.filled_at,
            });
            let state = replay_accounting(accounting_events)?;
            if state.cash.is_negative() {
                return Err(CoreError::AccountingInvariant(
                    "fill would create negative unleveraged cash".into(),
                ));
            }
            apply_due_dividends(
                request,
                fill.filled_at,
                DividendTimestampTie::BeforeBoundary,
                applied_dividends,
                accounting_events,
            )?;
            previous_fill_at = fill.filled_at;
        }
        apply_due_dividends(
            request,
            outcome.terminal_at,
            DividendTimestampTie::BeforeBoundary,
            applied_dividends,
            accounting_events,
        )?;
        if !outcome.fills.is_empty() && fees < request.cost_model.minimum_order_fee {
            return Err(CoreError::InvalidDomain(
                "filled order fee is below the frozen minimum".into(),
            ));
        }
        validate_terminal_reason(
            outcome.terminal_reason,
            filled_quantity,
            intent.quantity.get(),
        )?;
        let unfilled_quantity = intent.quantity.get() - filled_quantity;
        let unfilled_notional = outcome
            .quote
            .raw_price
            .checked_mul_quantity(unfilled_quantity)?;
        let opportunity_fraction =
            Fixed::from_scaled(i128::from(request.cost_model.opportunity_cost_bps) * 100);
        let modeled_opportunity_cost = Money(
            unfilled_notional
                .fixed()
                .checked_mul(opportunity_fraction)?,
        );
        results.push(PerformanceExecutionResult {
            plan_id: plan.plan_id.clone(),
            intent,
            terminal_reason: outcome.terminal_reason,
            filled_quantity: WholeQuantity::new(filled_quantity),
            fees,
            modeled_opportunity_cost,
            terminal_at: outcome.terminal_at,
        });
        previous_terminal_at = outcome.terminal_at;
    }
    Ok(results)
}

fn validate_terminal_reason(
    reason: ExecutionTerminalReason,
    filled: u64,
    ordered: u64,
) -> CoreResult<()> {
    let valid = match reason {
        ExecutionTerminalReason::Filled => filled == ordered,
        ExecutionTerminalReason::PartiallyFilledCancelled
        | ExecutionTerminalReason::PartiallyFilledExpired => filled > 0 && filled < ordered,
        ExecutionTerminalReason::CancelledNoFill
        | ExecutionTerminalReason::ExpiredNoFill
        | ExecutionTerminalReason::RejectedNoFill => filled == 0,
    };
    if valid {
        Ok(())
    } else {
        Err(CoreError::InvalidDomain(
            "terminal reason contradicts cumulative fill quantity".into(),
        ))
    }
}

fn evidence_holds(request: &PerformanceBacktestRequest) -> Vec<String> {
    let mut reasons = Vec::new();
    if request.dataset_manifest.stage == ResearchStage::Synthetic {
        reasons.push("synthetic_data_is_not_strategy_efficacy_evidence".into());
    } else {
        reasons.push("external_dataset_certification_requires_independent_verification".into());
    }
    reasons.push("statistical_and_economic_promotion_gates_not_evaluated_by_replay".into());
    reasons.push("stress_cost_distribution_and_concentration_gates_not_evaluated".into());
    reasons
        .push("nonfill_and_partial_fill_probabilities_are_not_applied_to_explicit_outcomes".into());
    reasons.push("intraday_high_low_stress_drawdown_is_not_evaluated".into());
    reasons.push("bounded_synthetic_mechanics_harness_is_not_a_full_horizon_backtester".into());
    if request.dataset_manifest.stage == ResearchStage::Holdout {
        reasons.push("durable_one_shot_holdout_consumption_must_be_recorded_externally".into());
    }
    reasons
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::domain::{MomentumTrendSpec, RebalanceCadence, StrategySpec};

    const SYMBOLS: [&str; 8] = ["DIA", "IVV", "IWM", "QQQ", "SCHB", "SPY", "VOO", "VTI"];

    fn digest(label: &str) -> HashDigest {
        HashDigest::sha256(label.as_bytes())
    }

    fn risk_limits() -> RiskLimitSnapshot {
        RiskLimitSnapshot {
            max_gross_exposure: Money::from_units(1_000).unwrap(),
            max_position_weight: Fixed::ONE,
            max_positions: 1,
            max_order_notional: Money::from_units(1_000).unwrap(),
            max_planned_loss: Money::from_units(1_000).unwrap(),
            daily_loss_limit: Money::from_units(1_000).unwrap(),
            hard_drawdown_limit: Money::from_units(1_000).unwrap(),
            planned_stop_distance_bps: 500,
            marketable_limit_band_bps: 20,
            new_positions_enabled: true,
        }
    }

    fn cost_model() -> PerformanceCostModel {
        PerformanceCostModel {
            model_id: "locked-cost-v1".into(),
            decision_to_arrival_latency_ms: 500,
            half_spread_bps: 5,
            adverse_slippage_bps: 5,
            opportunity_cost_bps: 5,
            non_fill_probability_bps: 500,
            partial_fill_probability_bps: 1_000,
            minimum_order_fee: Money::ZERO,
            stress_variable_cost_multiplier_bps: 20_000,
            stress_empirical_percentile_bps: 9_500,
        }
    }

    fn sessions() -> Vec<CertifiedSession> {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 21, 0, 0).unwrap();
        (0..129)
            .map(|offset| {
                let close = start + Duration::days(offset);
                let eligible_cadences = match offset {
                    126 => vec![RebalanceCadence::Weekly],
                    127 => vec![RebalanceCadence::Monthly],
                    _ => Vec::new(),
                };
                CertifiedSession {
                    session: close.date_naive(),
                    regular_open_at: close - Duration::hours(6) - Duration::minutes(30),
                    regular_close_at: close,
                    eligible_cadences,
                    calendar_payload_hash: digest(&format!("calendar-{offset}")),
                }
            })
            .collect()
    }

    fn observations(sessions: &[CertifiedSession], through: usize) -> Vec<CompletedObservation> {
        let mut observations = Vec::new();
        for (symbol_index, symbol) in SYMBOLS.iter().enumerate() {
            for (session_index, session) in sessions.iter().take(through + 1).enumerate() {
                observations.push(CompletedObservation {
                    symbol: Symbol::new(*symbol).unwrap(),
                    session: session.session,
                    completed_at: session.regular_close_at,
                    raw_close: Price::from_scaled(100_000_000),
                    total_return_close: Price::from_scaled(
                        100_000_000
                            + i128::try_from((symbol_index + 1) * session_index * 10_000).unwrap(),
                    ),
                });
            }
        }
        observations
    }

    fn synthetic_manifest(
        sessions: Vec<CertifiedSession>,
        mut execution_quote_hashes: Vec<HashDigest>,
    ) -> DatasetManifest {
        let observation_partitions = [126_usize, 127, 128]
            .into_iter()
            .map(|through| ObservationPartition {
                partition_id: format!("observations-{through}"),
                through_session: sessions[through].session,
                available_at: sessions[through].regular_close_at + Duration::hours(1),
                rows_hash: HashDigest::of_json(&observations(&sessions, through)).unwrap(),
            })
            .collect::<Vec<_>>();
        let empty_dividends = Vec::<DividendEvidence>::new();
        let dividend_partitions = [
            ("decision-1", 126_usize),
            ("decision-2", 127_usize),
            ("terminal-127", 127_usize),
            ("terminal-128", 128_usize),
        ]
        .into_iter()
        .map(|(decision_id, session_index)| DividendPartition {
            partition_id: format!("dividends-{decision_id}"),
            available_at: sessions[session_index].regular_close_at + Duration::hours(1),
            event_count: 0,
            events_hash: HashDigest::of_json(&empty_dividends).unwrap(),
        })
        .collect::<Vec<_>>();
        execution_quote_hashes.sort();
        DatasetManifest {
            dataset_id: "synthetic-dataset-v1".into(),
            stage: ResearchStage::Synthetic,
            source: "deterministic-fixture".into(),
            feed: "synthetic".into(),
            adjustment_mode: "raw-and-total-return".into(),
            symbols: SYMBOLS
                .iter()
                .map(|symbol| Symbol::new(*symbol).unwrap())
                .collect(),
            evaluation_start: sessions[126].session,
            evaluation_end: sessions[127].session,
            normalized_rows_hash: HashDigest::of_json(&observation_partitions).unwrap(),
            corporate_actions_hash: HashDigest::of_json(&dividend_partitions).unwrap(),
            execution_rows_hash: HashDigest::of_json(&execution_quote_hashes).unwrap(),
            observation_partitions,
            dividend_partitions,
            execution_quote_hashes,
            sessions,
            raw_objects_hash: digest("synthetic-raw"),
            unresolved_critical_defects: 0,
            certified_at: None,
            certifier_subject: None,
        }
    }

    fn release(manifest_hash: HashDigest, cost_hash: HashDigest) -> StrategyRelease {
        let strategy = StrategySpec::MomentumTrend(MomentumTrendSpec {
            momentum_lookback_sessions: 63,
            trend_lookback_sessions: 126,
            cadence: RebalanceCadence::Weekly,
        });
        StrategyRelease {
            release_id: "performance-release-v1".into(),
            code_hash: digest("code"),
            parameters_hash: HashDigest::of_json(&strategy).unwrap(),
            universe: SYMBOLS
                .iter()
                .map(|symbol| Symbol::new(*symbol).unwrap())
                .collect(),
            data_hash: manifest_hash,
            cost_model_hash: cost_hash,
            statistical_certificate_hash: digest("not-yet-certified"),
            strategy,
            valid_from: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    fn decision_input(
        sessions: &[CertifiedSession],
        through: usize,
        decision_id: &str,
    ) -> PerformanceDecisionInput {
        let observations = observations(sessions, through);
        PerformanceDecisionInput {
            decision_id: decision_id.into(),
            as_of: sessions[through].regular_close_at + Duration::hours(1),
            market_session: sessions[through].session,
            schedule: DecisionSchedule {
                eligible_cadences: sessions[through].eligible_cadences.clone(),
                calendar_evidence_hash: sessions[through].calendar_payload_hash,
            },
            input_data_hash: HashDigest::of_json(&observations).unwrap(),
            observations,
            observation_partition_id: format!("observations-{through}"),
            dividend_partition_id: format!("dividends-{decision_id}"),
            outcomes: Vec::new(),
        }
    }

    fn terminal_valuation(sessions: &[CertifiedSession], through: usize) -> TerminalValuation {
        let observations = observations(sessions, through);
        TerminalValuation {
            session: sessions[through].session,
            as_of: sessions[through].regular_close_at + Duration::hours(1),
            input_data_hash: HashDigest::of_json(&observations).unwrap(),
            observations,
            observation_partition_id: format!("observations-{through}"),
            dividend_partition_id: format!("dividends-terminal-{through}"),
        }
    }

    fn initial_snapshot(
        input: &PerformanceDecisionInput,
        release: &StrategyRelease,
        fingerprint: HashDigest,
    ) -> DecisionSnapshot {
        let account = AccountSnapshot {
            account_fingerprint: fingerprint,
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
            decision_id: input.decision_id.clone(),
            release_id: release.release_id.clone(),
            as_of: input.as_of,
            market_session: input.market_session,
            schedule: input.schedule.clone(),
            account_snapshot_hash: HashDigest::of_json(&account).unwrap(),
            account,
            observations: input.observations.clone(),
            input_data_hash: input.input_data_hash,
        }
    }

    fn filled_request() -> PerformanceBacktestRequest {
        let sessions = sessions();
        let provider_at = sessions[127].regular_open_at + Duration::milliseconds(500);
        let received_at = provider_at + Duration::milliseconds(1);
        let quote = FreshExecutionQuote {
            symbol: Symbol::new("VTI").unwrap(),
            raw_price: Price::from_scaled(100_000_000),
            provider_at,
            received_at,
            valid_until: received_at + Duration::seconds(10),
            payload_hash: digest("quote-1"),
        };
        let quote_hash = execution_quote_evidence_hash(&quote).unwrap();
        let manifest = synthetic_manifest(sessions.clone(), vec![quote_hash]);
        let manifest_hash = manifest.manifest_hash().unwrap();
        let cost_model = cost_model();
        let cost_hash = cost_model.model_hash().unwrap();
        let release = release(manifest_hash, cost_hash);
        let fingerprint = digest("account");
        let mut first = decision_input(&sessions, 126, "decision-1");
        let snapshot = initial_snapshot(&first, &release, fingerprint);
        let evaluation = evaluate_decision(&snapshot, &release, &risk_limits()).unwrap();
        assert_eq!(evaluation.order_plans.len(), 1);
        let plan = &evaluation.order_plans[0];
        assert_eq!(plan.symbol, quote.symbol);
        let submitted_at = received_at + Duration::milliseconds(1);
        let filled_at = submitted_at + Duration::milliseconds(1);
        let terminal_at = filled_at + Duration::milliseconds(1);
        first.outcomes.push(PerformanceOrderOutcome {
            plan_id: plan.plan_id.clone(),
            execution_session: sessions[127].session,
            quote,
            quote_evidence_hash: quote_hash,
            submitted_at,
            fills: vec![PerformanceFill {
                fill_id: "fill-1".into(),
                quantity: plan.quantity,
                price: Price::from_scaled(100_100_000),
                fee: Money::ZERO,
                filled_at,
                payload_hash: digest("fill-1"),
            }],
            terminal_at,
            terminal_reason: ExecutionTerminalReason::Filled,
            terminal_payload_hash: digest("terminal-1"),
        });
        PerformanceBacktestRequest {
            run_id: "performance-run-1".into(),
            run_at: sessions[127].regular_close_at + Duration::days(1),
            preregistration_hash: digest("preregistration"),
            release,
            risk_limits: risk_limits(),
            account_fingerprint: fingerprint,
            initial_cash: Money::from_units(1_000).unwrap(),
            started_at: sessions[0].regular_close_at - Duration::hours(1),
            dataset_manifest: manifest,
            dataset_manifest_hash: manifest_hash,
            cost_model,
            cost_model_hash: cost_hash,
            dividend_events: Vec::new(),
            holdout_access: None,
            decisions: vec![first],
            terminal_valuation: terminal_valuation(&sessions, 127),
        }
    }

    fn rebind_dataset_and_first_plan(request: &mut PerformanceBacktestRequest) {
        request.dataset_manifest.normalized_rows_hash =
            HashDigest::of_json(&request.dataset_manifest.observation_partitions).unwrap();
        request.dataset_manifest.corporate_actions_hash =
            HashDigest::of_json(&request.dataset_manifest.dividend_partitions).unwrap();
        request.dataset_manifest.execution_rows_hash =
            HashDigest::of_json(&request.dataset_manifest.execution_quote_hashes).unwrap();
        request.dataset_manifest_hash = request.dataset_manifest.manifest_hash().unwrap();
        request.release.data_hash = request.dataset_manifest_hash;
        let snapshot = initial_snapshot(
            &request.decisions[0],
            &request.release,
            request.account_fingerprint,
        );
        let evaluation =
            evaluate_decision(&snapshot, &request.release, &request.risk_limits).unwrap();
        request.decisions[0].outcomes[0].plan_id = evaluation.order_plans[0].plan_id.clone();
    }

    #[test]
    fn research_stage_wire_names_match_the_python_protocol() {
        assert_eq!(
            serde_json::to_string(&ResearchStage::ProspectiveShadow).unwrap(),
            "\"prospective_shadow\""
        );
    }

    #[test]
    fn deterministic_full_fill_derives_cash_equity_and_explicit_hold() {
        let request = filled_request();
        let first = run_performance_backtest(&request).unwrap();
        let second = run_performance_backtest(&request).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.steps.len(), 1);
        assert_eq!(first.final_accounting.cash, Money::from_scaled(99_100_000));
        assert_eq!(first.ending_equity, Money::from_scaled(999_100_000));
        assert_eq!(first.net_pnl, Money::from_scaled(-900_000));
        assert_eq!(first.modeled_opportunity_cost, Money::ZERO);
        assert_eq!(first.net_pnl_after_modeled_opportunity_cost, first.net_pnl);
        assert_eq!(
            first.close_and_execution_max_drawdown,
            Money::from_scaled(-900_000)
        );
        assert!(first.mechanical_metrics_available);
        assert!(!first.stressed_performance_evidence_available);
        assert!(!first.qualifies_as_strategy_evidence);
        assert!(first.decision_window_complete);
        assert!(first.terminal_valuation_complete);
        assert!(first.evaluation_complete);
        assert!(first
            .hold_reasons
            .iter()
            .any(|reason| reason.contains("synthetic_data")));
        assert!(first
            .hold_reasons
            .iter()
            .any(|reason| reason.contains("probabilities_are_not_applied")));
    }

    #[test]
    fn every_plan_requires_one_explicit_outcome() {
        let mut missing = filled_request();
        missing.decisions[0].outcomes.clear();
        assert!(matches!(
            run_performance_backtest(&missing),
            Err(CoreError::InvalidDomain(message))
                if message == "performance outcomes do not exactly cover emitted order plans"
        ));

        let mut duplicate = filled_request();
        let repeated = duplicate.decisions[0].outcomes[0].clone();
        duplicate.decisions[0].outcomes.push(repeated);
        assert!(run_performance_backtest(&duplicate).is_err());
    }

    #[test]
    fn quote_latency_limits_and_evidence_hashes_fail_closed() {
        let mut early = filled_request();
        early.decisions[0].outcomes[0].execution_session = early.decisions[0].market_session;
        assert!(run_performance_backtest(&early).is_err());

        let mut outside_limit = filled_request();
        outside_limit.decisions[0].outcomes[0].fills[0].price = Price::from_scaled(100_300_000);
        assert!(run_performance_backtest(&outside_limit).is_err());

        let mut tampered = filled_request();
        tampered.decisions[0].observations[0].raw_close = Price::from_scaled(99_000_000);
        tampered.decisions[0].input_data_hash =
            HashDigest::of_json(&tampered.decisions[0].observations).unwrap();
        assert!(run_performance_backtest(&tampered).is_err());

        let mut changed_cost = filled_request();
        changed_cost.cost_model.half_spread_bps = 0;
        changed_cost.cost_model.adverse_slippage_bps = 0;
        changed_cost.cost_model.opportunity_cost_bps = 0;
        changed_cost.cost_model.non_fill_probability_bps = 0;
        changed_cost.cost_model.partial_fill_probability_bps = 0;
        changed_cost.cost_model_hash = changed_cost.cost_model.model_hash().unwrap();
        changed_cost.release.cost_model_hash = changed_cost.cost_model_hash;
        assert!(run_performance_backtest(&changed_cost).is_err());

        let mut changed_schedule = filled_request();
        changed_schedule.decisions[0]
            .schedule
            .eligible_cadences
            .push(RebalanceCadence::Monthly);
        assert!(run_performance_backtest(&changed_schedule).is_err());

        let mut changed_quote = filled_request();
        changed_quote.decisions[0].outcomes[0].quote.raw_price = Price::from_scaled(99_000_000);
        changed_quote.decisions[0].outcomes[0].quote_evidence_hash =
            execution_quote_evidence_hash(&changed_quote.decisions[0].outcomes[0].quote).unwrap();
        assert!(run_performance_backtest(&changed_quote).is_err());
    }

    #[test]
    fn submission_at_the_exclusive_quote_deadline_is_rejected() {
        let mut request = filled_request();
        let outcome = &mut request.decisions[0].outcomes[0];
        outcome.submitted_at = outcome.quote.valid_until;
        outcome.fills[0].filled_at = outcome.submitted_at + Duration::milliseconds(1);
        outcome.terminal_at = outcome.fills[0].filled_at + Duration::milliseconds(1);

        assert!(matches!(
            run_performance_backtest(&request),
            Err(CoreError::InvalidDomain(message))
                if message.contains("next-session regular-hours lifecycle")
        ));
    }

    #[test]
    fn release_expiring_between_decision_and_submission_rejects_the_lifecycle() {
        let mut request = filled_request();
        let decision_at = request.decisions[0].as_of;
        let submitted_at = request.decisions[0].outcomes[0].submitted_at;
        request.release.expires_at = decision_at + Duration::hours(1);
        assert!(request.release.expires_at < submitted_at);
        rebind_dataset_and_first_plan(&mut request);

        assert!(matches!(
            run_performance_backtest(&request),
            Err(CoreError::InvalidDomain(message))
                if message.contains("next-session regular-hours lifecycle")
        ));
    }

    #[test]
    fn oversized_identifiers_and_descriptors_are_rejected() {
        let mut oversized_identifier = filled_request();
        oversized_identifier.run_id = "x".repeat(MAX_IDENTIFIER_BYTES + 1);
        assert!(matches!(
            run_performance_backtest(&oversized_identifier),
            Err(CoreError::InvalidDomain(message))
                if message.contains("oversized identifier")
        ));

        let mut oversized_descriptor = filled_request();
        oversized_descriptor.dataset_manifest.source = "x".repeat(MAX_DESCRIPTOR_BYTES + 1);
        rebind_dataset_and_first_plan(&mut oversized_descriptor);
        assert!(matches!(
            run_performance_backtest(&oversized_descriptor),
            Err(CoreError::InvalidDomain(message))
                if message.contains("oversized identifier")
        ));
    }

    #[test]
    fn no_fill_and_partial_fill_are_explicit_and_conservative() {
        let mut no_fill = filled_request();
        no_fill.decisions[0].outcomes[0].fills.clear();
        no_fill.decisions[0].outcomes[0].terminal_reason = ExecutionTerminalReason::ExpiredNoFill;
        let result = run_performance_backtest(&no_fill).unwrap();
        assert!(result
            .final_accounting
            .positions
            .values()
            .all(|position| { position.quantity == WholeQuantity::ZERO }));
        assert_eq!(result.ending_equity, Money::from_units(1_000).unwrap());
        assert_eq!(result.modeled_opportunity_cost, Money::from_scaled(450_000));

        let mut partial = filled_request();
        partial.decisions[0].outcomes[0].fills[0].quantity = WholeQuantity::new(1);
        partial.decisions[0].outcomes[0].terminal_reason =
            ExecutionTerminalReason::PartiallyFilledCancelled;
        let result = run_performance_backtest(&partial).unwrap();
        assert_eq!(
            result
                .final_accounting
                .positions
                .values()
                .next()
                .unwrap()
                .quantity,
            WholeQuantity::new(1)
        );

        let mut duplicate_fill = filled_request();
        duplicate_fill.decisions[0].outcomes[0].fills[0].quantity = WholeQuantity::new(1);
        let repeated_fill = duplicate_fill.decisions[0].outcomes[0].fills[0].clone();
        duplicate_fill.decisions[0].outcomes[0]
            .fills
            .push(repeated_fill);
        duplicate_fill.decisions[0].outcomes[0].terminal_reason =
            ExecutionTerminalReason::PartiallyFilledCancelled;
        assert!(run_performance_backtest(&duplicate_fill).is_err());
    }

    #[test]
    fn daily_pnl_and_drawdown_walk_each_certified_close_with_as_of_holdings() {
        let mut request = filled_request();
        let sessions = request.dataset_manifest.sessions.clone();
        request.dataset_manifest.evaluation_end = sessions[128].session;
        request.terminal_valuation = terminal_valuation(&sessions, 128);
        request.run_at = request.terminal_valuation.as_of + Duration::hours(1);
        let mut second = decision_input(&sessions, 127, "decision-2");
        let vti = Symbol::new("VTI").unwrap();
        let close = second
            .observations
            .iter_mut()
            .find(|observation| {
                observation.symbol == vti && observation.session == sessions[127].session
            })
            .unwrap();
        close.raw_close = Price::from_units(80).unwrap();
        second.input_data_hash = HashDigest::of_json(&second.observations).unwrap();
        request.decisions.push(second);
        request
            .dataset_manifest
            .observation_partitions
            .iter_mut()
            .find(|partition| partition.partition_id == "observations-127")
            .unwrap()
            .rows_hash = request.decisions[1].input_data_hash;
        rebind_dataset_and_first_plan(&mut request);

        let result = run_performance_backtest(&request).unwrap();
        assert_eq!(
            result.steps[1].decision_account.day_pnl,
            Money::from_scaled(-180_900_000)
        );
        assert_eq!(
            result.close_and_execution_max_drawdown,
            Money::from_scaled(-180_900_000)
        );
    }

    #[test]
    fn fees_cannot_create_leverage_and_missing_sessions_are_rejected() {
        let mut leveraged = filled_request();
        leveraged.decisions[0].outcomes[0].fills[0].fee = Money::from_units(200).unwrap();
        assert!(matches!(
            run_performance_backtest(&leveraged),
            Err(CoreError::AccountingInvariant(_))
        ));

        let mut gap = filled_request();
        gap.decisions[0].observations.remove(0);
        gap.decisions[0].input_data_hash =
            HashDigest::of_json(&gap.decisions[0].observations).unwrap();
        assert!(run_performance_backtest(&gap).is_err());

        let mut late_accounting_start = filled_request();
        late_accounting_start.started_at = late_accounting_start.dataset_manifest.sessions[125]
            .regular_close_at
            + Duration::seconds(1);
        assert!(run_performance_backtest(&late_accounting_start).is_err());
    }

    #[test]
    fn later_decision_on_a_frozen_manifest_cannot_change_the_earlier_step() {
        let mut short = filled_request();
        let sessions = short.dataset_manifest.sessions.clone();
        short.dataset_manifest.evaluation_end = sessions[128].session;
        short.terminal_valuation = terminal_valuation(&sessions, 128);
        short.run_at = short.terminal_valuation.as_of + Duration::hours(1);
        rebind_dataset_and_first_plan(&mut short);
        let mut long = short.clone();
        long.decisions
            .push(decision_input(&sessions, 127, "decision-2"));
        let short_result = run_performance_backtest(&short).unwrap();
        let long_result = run_performance_backtest(&long).unwrap();
        assert_eq!(short_result.steps[0], long_result.steps[0]);
        assert_ne!(short_result.request_hash, long_result.request_hash);
        assert!(!short_result.decision_window_complete);
        assert!(long_result.decision_window_complete);
        assert!(short_result.terminal_valuation_complete);
        assert!(long_result.terminal_valuation_complete);
        assert!(!short_result.evaluation_complete);
        assert!(long_result.evaluation_complete);
        assert_eq!(
            long_result.last_execution_session,
            Some(sessions[127].session)
        );
        assert!(long_result.execution_lifecycles_complete);
        assert_eq!(
            long_result.terminal_valuation_session,
            sessions[128].session
        );
    }

    #[test]
    fn delayed_dividends_are_symbol_bound_and_derived_from_held_quantity() {
        let mut request = filled_request();
        let sessions = request.dataset_manifest.sessions.clone();
        request.dataset_manifest.evaluation_end = sessions[128].session;
        request.terminal_valuation = terminal_valuation(&sessions, 128);
        request.run_at = request.terminal_valuation.as_of + Duration::hours(1);
        let second = decision_input(&sessions, 127, "decision-2");
        request.dividend_events.push(DividendEvidence {
            event_id: "dividend-1".into(),
            symbol: request.decisions[0].outcomes[0].quote.symbol.clone(),
            amount_per_share: Price::from_units(1).unwrap(),
            record_at: request.decisions[0].outcomes[0].terminal_at + Duration::minutes(30),
            payable_at: request.decisions[0].outcomes[0].terminal_at + Duration::hours(1),
            available_at: request.decisions[0].outcomes[0].terminal_at + Duration::hours(2),
            payload_hash: digest("dividend-1"),
        });
        request.decisions.push(second);
        for dividend_partition in request
            .dataset_manifest
            .dividend_partitions
            .iter_mut()
            .filter(|partition| partition.available_at >= request.dividend_events[0].available_at)
        {
            dividend_partition.event_count = 1;
            dividend_partition.events_hash = HashDigest::of_json(&request.dividend_events).unwrap();
        }
        rebind_dataset_and_first_plan(&mut request);
        let result = run_performance_backtest(&request).unwrap();
        assert_eq!(
            result.final_accounting.dividends,
            Money::from_units(9).unwrap()
        );
        assert_eq!(result.ending_equity, Money::from_scaled(1_008_100_000));

        let mut not_entitled = request;
        not_entitled.dividend_events[0].record_at =
            not_entitled.decisions[0].outcomes[0].submitted_at;
        for partition in not_entitled
            .dataset_manifest
            .dividend_partitions
            .iter_mut()
            .filter(|partition| partition.event_count == 1)
        {
            partition.events_hash = HashDigest::of_json(&not_entitled.dividend_events).unwrap();
        }
        rebind_dataset_and_first_plan(&mut not_entitled);
        let result = run_performance_backtest(&not_entitled).unwrap();
        assert_eq!(result.final_accounting.dividends, Money::ZERO);
    }

    #[test]
    fn dividend_effective_after_decision_and_before_sale_uses_record_date_entitlement() {
        let mut request = filled_request();
        request.dataset_manifest.sessions[127].eligible_cadences = vec![RebalanceCadence::Weekly];
        let sessions = request.dataset_manifest.sessions.clone();
        request.dataset_manifest.evaluation_end = sessions[128].session;
        request.terminal_valuation = terminal_valuation(&sessions, 128);
        request.run_at = request.terminal_valuation.as_of + Duration::hours(1);

        let mut second = decision_input(&sessions, 127, "decision-2");
        for observation in second
            .observations
            .iter_mut()
            .filter(|observation| observation.session == second.market_session)
        {
            observation.total_return_close = Price::from_units(1).unwrap();
        }
        second.input_data_hash = HashDigest::of_json(&second.observations).unwrap();
        request
            .dataset_manifest
            .observation_partitions
            .iter_mut()
            .find(|partition| partition.partition_id == second.observation_partition_id)
            .unwrap()
            .rows_hash = second.input_data_hash;

        let first_outcome = &request.decisions[0].outcomes[0];
        let first_symbol = first_outcome.quote.symbol.clone();
        let first_terminal_at = first_outcome.terminal_at;
        let first_fill = first_outcome.fills[0].clone();
        let dividend_effective_at = second.as_of + Duration::hours(1);
        request.dividend_events.push(DividendEvidence {
            event_id: "between-decision-and-sale".into(),
            symbol: first_symbol.clone(),
            amount_per_share: Price::from_units(1).unwrap(),
            record_at: first_terminal_at + Duration::minutes(1),
            payable_at: dividend_effective_at,
            available_at: dividend_effective_at,
            payload_hash: digest("between-decision-and-sale"),
        });
        for partition in request
            .dataset_manifest
            .dividend_partitions
            .iter_mut()
            .filter(|partition| partition.available_at >= dividend_effective_at)
        {
            partition.event_count = 1;
            partition.events_hash = HashDigest::of_json(&request.dividend_events).unwrap();
        }

        let provider_at = sessions[128].regular_open_at + Duration::milliseconds(500);
        let received_at = provider_at + Duration::milliseconds(1);
        let quote = FreshExecutionQuote {
            symbol: first_symbol.clone(),
            raw_price: Price::from_units(100).unwrap(),
            provider_at,
            received_at,
            valid_until: received_at + Duration::seconds(10),
            payload_hash: digest("quote-2"),
        };
        let quote_hash = execution_quote_evidence_hash(&quote).unwrap();
        request
            .dataset_manifest
            .execution_quote_hashes
            .push(quote_hash);
        request.dataset_manifest.execution_quote_hashes.sort();
        rebind_dataset_and_first_plan(&mut request);

        let accounting = replay_accounting(&[
            AccountingEvent::CashDeposit {
                amount: request.initial_cash,
                at: request.started_at,
            },
            AccountingEvent::Fill {
                symbol: first_symbol,
                side: OrderSide::Buy,
                quantity: first_fill.quantity,
                price: first_fill.price,
                fee: first_fill.fee,
                at: first_fill.filled_at,
            },
        ])
        .unwrap();
        let marks = marks_for_session(&second.observations, second.market_session).unwrap();
        let equity = marked_equity(&accounting, &marks).unwrap();
        let drawdown = equity.checked_sub(request.initial_cash).unwrap();
        let account = account_from_accounting(
            request.account_fingerprint,
            &accounting,
            &marks,
            equity,
            drawdown,
            drawdown,
        )
        .unwrap();
        let snapshot = DecisionSnapshot {
            decision_id: second.decision_id.clone(),
            release_id: request.release.release_id.clone(),
            as_of: second.as_of,
            market_session: second.market_session,
            schedule: second.schedule.clone(),
            account_snapshot_hash: HashDigest::of_json(&account).unwrap(),
            account,
            observations: second.observations.clone(),
            input_data_hash: second.input_data_hash,
        };
        let evaluation =
            evaluate_decision(&snapshot, &request.release, &request.risk_limits).unwrap();
        assert_eq!(evaluation.order_plans.len(), 1);
        let plan = &evaluation.order_plans[0];
        assert_eq!(plan.side, OrderSide::Sell);
        assert_eq!(plan.quantity, first_fill.quantity);

        let submitted_at = received_at + Duration::milliseconds(1);
        assert!(dividend_effective_at > second.as_of);
        assert!(dividend_effective_at < submitted_at);
        let filled_at = submitted_at + Duration::milliseconds(1);
        second.outcomes.push(PerformanceOrderOutcome {
            plan_id: plan.plan_id.clone(),
            execution_session: sessions[128].session,
            quote,
            quote_evidence_hash: quote_hash,
            submitted_at,
            fills: vec![PerformanceFill {
                fill_id: "fill-2".into(),
                quantity: plan.quantity,
                price: Price::from_scaled(99_900_000),
                fee: Money::ZERO,
                filled_at,
                payload_hash: digest("fill-2"),
            }],
            terminal_at: filled_at + Duration::milliseconds(1),
            terminal_reason: ExecutionTerminalReason::Filled,
            terminal_payload_hash: digest("terminal-2"),
        });
        request.decisions.push(second);

        let result = run_performance_backtest(&request).unwrap();
        assert_eq!(
            result.steps[1].decision_account.cash,
            Money::from_scaled(99_100_000)
        );
        assert_eq!(
            result.final_accounting.dividends,
            Money::from_units(9).unwrap()
        );
        assert_eq!(
            result.final_accounting.cash,
            Money::from_scaled(1_007_200_000)
        );
        assert_eq!(result.ending_equity, result.final_accounting.cash);
        assert!(result
            .final_accounting
            .positions
            .values()
            .all(|position| { position.quantity == WholeQuantity::ZERO }));
        assert!(result.evaluation_complete);
    }

    #[test]
    fn real_research_stages_remain_blocked_even_with_structural_permit() {
        let mut request = filled_request();
        request.dataset_manifest.stage = ResearchStage::Holdout;
        request.dataset_manifest.certified_at = Some(request.started_at);
        request.dataset_manifest.certifier_subject = Some("independent-data-certifier".into());
        rebind_dataset_and_first_plan(&mut request);
        assert!(run_performance_backtest(&request).is_err());

        request.holdout_access = Some(HoldoutAccessPermit {
            permit_id: "holdout-permit-1".into(),
            run_id: request.run_id.clone(),
            preregistration_hash: request.preregistration_hash,
            dataset_manifest_hash: request.dataset_manifest_hash,
            operator_subject: "operator".into(),
            one_shot: true,
            issued_at: request.started_at,
            expires_at: request.run_at + Duration::hours(1),
            approval_digest: digest("operator-approval"),
        });
        assert!(run_performance_backtest(&request).is_err());

        request.dataset_manifest.stage = ResearchStage::Validation;
        rebind_dataset_and_first_plan(&mut request);
        assert!(run_performance_backtest(&request).is_err());
    }
}
