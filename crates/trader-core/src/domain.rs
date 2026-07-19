use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::{
    error::{CoreError, CoreResult},
    fixed::{Fixed, Money, Price},
};

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct WholeQuantity(u64);

impl WholeQuantity {
    pub const ZERO: Self = Self(0);
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> CoreResult<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 16
            || !value.chars().all(|character| {
                character.is_ascii_uppercase() || character == '.' || character == '-'
            })
        {
            return Err(CoreError::InvalidSymbol(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

impl FromStr for Symbol {
    type Err = CoreError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HashDigest([u8; 32]);

pub const JSON_HASH_PROFILE: &str = "wasp-json-sha256-v1";

impl HashDigest {
    pub fn sha256(bytes: impl AsRef<[u8]>) -> Self {
        Self(Sha256::digest(bytes.as_ref()).into())
    }

    pub fn of_json<T: Serialize + ?Sized>(value: &T) -> CoreResult<Self> {
        let canonical = canonicalize_json_value(serde_json::to_value(value)?)?;
        Ok(Self::sha256(serde_json::to_vec(&canonical)?))
    }

    pub fn as_hex(&self) -> String {
        hex::encode(self.0)
    }
}

fn canonicalize_json_value(value: serde_json::Value) -> CoreResult<serde_json::Value> {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, canonicalize_json_value(value)?);
            }
            Ok(serde_json::Value::Object(canonical))
        }
        serde_json::Value::Array(values) => Ok(serde_json::Value::Array(
            values
                .into_iter()
                .map(canonicalize_json_value)
                .collect::<CoreResult<Vec<_>>>()?,
        )),
        serde_json::Value::Number(number) => {
            let encoded = number.to_string();
            let is_integer = if encoded.starts_with('-') {
                encoded.parse::<i128>().is_ok()
            } else {
                encoded.parse::<u128>().is_ok()
            };
            if !is_integer {
                return Err(CoreError::Serialization(
                    "canonical JSON evidence permits only i128/u128 integers".into(),
                ));
            }
            Ok(serde_json::Value::Number(number))
        }
        scalar => Ok(scalar),
    }
}

impl fmt::Display for HashDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.as_hex())
    }
}

impl FromStr for HashDigest {
    type Err = CoreError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let decoded = hex::decode(value).map_err(|_| CoreError::InvalidHash(value.into()))?;
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|_| CoreError::InvalidHash(value.into()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for HashDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_hex())
    }
}

impl<'de> Deserialize<'de> for HashDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceCadence {
    Weekly,
    Monthly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MomentumTrendSpec {
    pub momentum_lookback_sessions: u16,
    pub trend_lookback_sessions: u16,
    pub cadence: RebalanceCadence,
}

impl MomentumTrendSpec {
    pub fn validate(&self) -> CoreResult<()> {
        if ![63, 126, 252].contains(&self.momentum_lookback_sessions) {
            return Err(CoreError::InvalidDomain(
                "momentum lookback is outside the preregistered family".into(),
            ));
        }
        if ![126, 252].contains(&self.trend_lookback_sessions) {
            return Err(CoreError::InvalidDomain(
                "trend lookback is outside the preregistered family".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StrategySpec {
    MomentumTrend(MomentumTrendSpec),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyRelease {
    pub release_id: String,
    pub code_hash: HashDigest,
    pub parameters_hash: HashDigest,
    pub universe: Vec<Symbol>,
    pub data_hash: HashDigest,
    pub cost_model_hash: HashDigest,
    pub statistical_certificate_hash: HashDigest,
    pub strategy: StrategySpec,
    pub valid_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl StrategyRelease {
    pub fn validate(&self) -> CoreResult<()> {
        if self.release_id.trim().is_empty() {
            return Err(CoreError::InvalidDomain("release_id is empty".into()));
        }
        if self.valid_from >= self.expires_at {
            return Err(CoreError::InvalidDomain(
                "release validity interval is empty".into(),
            ));
        }
        if !(8..=12).contains(&self.universe.len()) {
            return Err(CoreError::InvalidDomain(
                "v1 universe must contain 8 through 12 instruments".into(),
            ));
        }
        let unique: BTreeSet<_> = self.universe.iter().collect();
        if unique.len() != self.universe.len() {
            return Err(CoreError::InvalidDomain(
                "release universe contains duplicates".into(),
            ));
        }
        match &self.strategy {
            StrategySpec::MomentumTrend(spec) => spec.validate()?,
        }
        if HashDigest::of_json(&self.strategy)? != self.parameters_hash {
            return Err(CoreError::InvalidDomain(
                "parameters_hash does not match the embedded immutable strategy parameters".into(),
            ));
        }
        Ok(())
    }

    pub fn release_hash(&self) -> CoreResult<HashDigest> {
        HashDigest::of_json(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    Active,
    Restricted,
    Closed,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountPosition {
    pub symbol: Symbol,
    pub quantity: WholeQuantity,
    pub average_entry_price: Price,
    pub market_price: Price,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshot {
    pub account_fingerprint: HashDigest,
    pub status: AccountStatus,
    pub trading_blocked: bool,
    pub cash: Money,
    pub buying_power: Money,
    pub equity: Money,
    pub day_pnl: Money,
    pub drawdown: Money,
    pub positions: Vec<AccountPosition>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedObservation {
    pub symbol: Symbol,
    pub session: NaiveDate,
    pub completed_at: DateTime<Utc>,
    /// Unadjusted close used only as a decision-time risk reference. It is not
    /// executable and must be refreshed after the decision before submission.
    pub raw_close: Price,
    /// Total-return-adjusted close used only for signal calculations.
    pub total_return_close: Price,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionSnapshot {
    pub decision_id: String,
    pub release_id: String,
    pub as_of: DateTime<Utc>,
    pub market_session: NaiveDate,
    pub schedule: DecisionSchedule,
    pub account: AccountSnapshot,
    pub account_snapshot_hash: HashDigest,
    pub observations: Vec<CompletedObservation>,
    pub input_data_hash: HashDigest,
}

/// Calendar-derived eligibility. The upstream market adapter obtains this from
/// the broker calendar; strategy code never hardcodes weekdays or month ends.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionSchedule {
    pub eligible_cadences: Vec<RebalanceCadence>,
    pub calendar_evidence_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetPosition {
    pub symbol: Symbol,
    pub target_weight: Fixed,
    pub target_quantity: WholeQuantity,
    /// Unadjusted decision-time reference; never an executable order price.
    pub raw_reference_price: Price,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetPortfolio {
    pub decision_id: String,
    pub release_id: String,
    pub generated_at: DateTime<Utc>,
    pub positions: Vec<TargetPosition>,
    pub cash_target: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskLimitSnapshot {
    pub max_gross_exposure: Money,
    pub max_position_weight: Fixed,
    pub max_positions: u16,
    pub max_order_notional: Money,
    pub max_planned_loss: Money,
    pub daily_loss_limit: Money,
    pub hard_drawdown_limit: Money,
    /// Certified distance from entry to catastrophic protection, in basis points.
    pub planned_stop_distance_bps: u16,
    pub marketable_limit_band_bps: u16,
    pub new_positions_enabled: bool,
}

impl RiskLimitSnapshot {
    pub fn validate(&self) -> CoreResult<()> {
        if self.max_gross_exposure.is_negative()
            || self.max_order_notional.is_negative()
            || self.max_planned_loss.is_negative()
            || self.daily_loss_limit.is_negative()
            || self.hard_drawdown_limit.is_negative()
            || self.max_position_weight.is_negative()
            || self.max_position_weight > Fixed::ONE
        {
            return Err(CoreError::InvalidDomain(
                "risk limits must be non-negative and weight cannot exceed one".into(),
            ));
        }
        if self.max_positions != 1
            || self.planned_stop_distance_bps == 0
            || self.planned_stop_distance_bps > 5_000
            || self.marketable_limit_band_bps > 1_000
        {
            return Err(CoreError::InvalidDomain(
                "v1 requires exactly one position and valid stop/price bands".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskDisposition {
    Approved,
    Reduced,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskDecision {
    pub decision_id: String,
    pub disposition: RiskDisposition,
    pub approved_positions: Vec<TargetPosition>,
    pub limits: RiskLimitSnapshot,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
}

/// Broker-independent order delta. It deliberately has no executable price,
/// but retains the raw decision-time price used by strategy and risk.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrderPlan {
    pub plan_id: String,
    pub release_id: String,
    pub decision_id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub quantity: WholeQuantity,
    pub decision_reference_price: Price,
    pub decision_evidence_hash: HashDigest,
    pub created_at: DateTime<Utc>,
}

/// A raw, post-decision quote supplied by the execution adapter. Materializing
/// an OrderIntent requires this evidence and rejects stale/pre-decision quotes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FreshExecutionQuote {
    pub symbol: Symbol,
    pub raw_price: Price,
    pub provider_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub payload_hash: HashDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrderIntent {
    pub intent_id: String,
    pub client_order_id: String,
    pub release_id: String,
    pub decision_id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub quantity: WholeQuantity,
    pub limit_price: Price,
    pub decision_at: DateTime<Utc>,
    pub arrival_quote: Price,
    pub quote_provider_at: DateTime<Utc>,
    pub quote_received_at: DateTime<Utc>,
    pub quote_valid_until: DateTime<Utc>,
    pub quote_payload_hash: HashDigest,
    pub time_in_force: TimeInForce,
    /// Hash of the released decision, risk result, and non-executable plan.
    pub decision_evidence_hash: HashDigest,
    /// Hash of the complete materialization inputs, including raw quote
    /// evidence and the resulting executable limit price.
    pub materialization_evidence_hash: HashDigest,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerEvent {
    pub provider_order_id: Option<String>,
    pub client_order_id: String,
    pub status: String,
    pub filled_quantity: WholeQuantity,
    pub fill_price: Option<Price>,
    pub provider_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_payload_hash: HashDigest,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationDifferenceKind {
    MissingLocally,
    MissingAtBroker,
    QuantityMismatch,
    CashMismatch,
    StatusMismatch,
    UnknownProviderState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationDifference {
    pub kind: ReconciliationDifferenceKind,
    pub subject: String,
    /// Canonical, bounded textual value used on the local side of the exact
    /// comparison. `None` means the value was absent, not zero.
    pub local_value: Option<String>,
    /// Canonical, bounded textual value used on the broker side of the exact
    /// comparison. `None` means the value was absent, not zero.
    pub broker_value: Option<String>,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationReport {
    pub generated_at: DateTime<Utc>,
    pub account_fingerprint: HashDigest,
    pub execution_fencing_token: u64,
    pub differences: Vec<ReconciliationDifference>,
    pub may_resume_execution: bool,
}

impl ReconciliationReport {
    pub fn validate(&self) -> CoreResult<()> {
        if self.execution_fencing_token == 0 {
            return Err(CoreError::InvalidDomain(
                "reconciliation lacks a positive execution fence".into(),
            ));
        }
        if self.may_resume_execution && !self.differences.is_empty() {
            return Err(CoreError::InvalidDomain(
                "reconciliation cannot resume with unresolved differences".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Shadow,
    Paper,
    Live,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationPermit {
    pub permit_id: String,
    pub environment: Environment,
    pub account_fingerprint: HashDigest,
    pub strategy_release_id: String,
    pub strategy_release_hash: HashDigest,
    pub max_gross_notional: Money,
    pub max_position_notional: Money,
    pub max_daily_loss: Money,
    pub max_drawdown: Money,
    pub risk_limits_hash: HashDigest,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub operator_subject: String,
    pub approval_digest: HashDigest,
}

impl ActivationPermit {
    pub fn validate(&self, now: DateTime<Utc>) -> CoreResult<()> {
        if self.issued_at >= self.expires_at || now < self.issued_at || now >= self.expires_at {
            return Err(CoreError::InvalidDomain(
                "activation permit is not currently valid".into(),
            ));
        }
        if self.permit_id.trim().is_empty()
            || self.strategy_release_id.trim().is_empty()
            || self.operator_subject.trim().is_empty()
            || !self.max_gross_notional.fixed().is_positive()
            || !self.max_position_notional.fixed().is_positive()
            || !self.max_daily_loss.fixed().is_positive()
            || !self.max_drawdown.fixed().is_positive()
            || self.max_position_notional > self.max_gross_notional
        {
            return Err(CoreError::InvalidDomain(
                "activation permit lacks valid operator authority".into(),
            ));
        }
        Ok(())
    }
}

/// Revocation is a separate immutable event; permits themselves are never edited.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationPermitRevocation {
    pub revocation_id: String,
    pub permit_id: String,
    pub revoked_at: DateTime<Utc>,
    pub operator_subject: String,
    pub reason_code: String,
    pub approval_digest: HashDigest,
}

impl ActivationPermitRevocation {
    pub fn validate(&self, permit: &ActivationPermit) -> CoreResult<()> {
        if self.revocation_id.trim().is_empty()
            || self.permit_id != permit.permit_id
            || self.revoked_at < permit.issued_at
            || self.operator_subject.trim().is_empty()
            || self.reason_code.trim().is_empty()
        {
            return Err(CoreError::InvalidDomain(
                "activation permit revocation is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KillSeverity {
    Clear,
    Soft,
    Hard,
    Liquidation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KillState {
    pub severity: KillSeverity,
    pub reason_code: String,
    pub detail: BTreeMap<String, String>,
    pub actor: String,
    pub operator_approved: bool,
    pub approval_digest: Option<HashDigest>,
    pub occurred_at: DateTime<Utc>,
}

impl KillState {
    pub fn clear(actor: impl Into<String>, approval_digest: HashDigest, at: DateTime<Utc>) -> Self {
        Self {
            severity: KillSeverity::Clear,
            reason_code: "operator_clearance".into(),
            detail: BTreeMap::new(),
            actor: actor.into(),
            operator_approved: true,
            approval_digest: Some(approval_digest),
            occurred_at: at,
        }
    }

    pub fn hard(
        reason_code: impl Into<String>,
        actor: impl Into<String>,
        at: DateTime<Utc>,
    ) -> Self {
        Self {
            severity: KillSeverity::Hard,
            reason_code: reason_code.into(),
            detail: BTreeMap::new(),
            actor: actor.into(),
            operator_approved: false,
            approval_digest: None,
            occurred_at: at,
        }
    }

    pub fn validate(&self) -> CoreResult<()> {
        if self.reason_code.trim().is_empty() || self.actor.trim().is_empty() {
            return Err(CoreError::InvalidDomain(
                "kill state lacks reason or actor evidence".into(),
            ));
        }
        if self.severity == KillSeverity::Clear
            && (!self.operator_approved || self.approval_digest.is_none())
        {
            return Err(CoreError::InvalidDomain(
                "clear kill event requires operator approval evidence".into(),
            ));
        }
        if self.operator_approved && self.approval_digest.is_none() {
            return Err(CoreError::InvalidDomain(
                "operator-approved kill event lacks approval digest".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use chrono::TimeZone;

    pub fn digest(label: &str) -> HashDigest {
        HashDigest::sha256(label)
    }

    pub fn symbols() -> Vec<Symbol> {
        ["SPY", "QQQ", "DIA", "IWM", "VTI", "VOO", "IVV", "SCHB"]
            .into_iter()
            .map(|s| Symbol::new(s).unwrap())
            .collect()
    }

    pub fn release() -> StrategyRelease {
        let strategy = StrategySpec::MomentumTrend(MomentumTrendSpec {
            momentum_lookback_sessions: 63,
            trend_lookback_sessions: 126,
            cadence: RebalanceCadence::Weekly,
        });
        StrategyRelease {
            release_id: "release-1".into(),
            code_hash: digest("code"),
            parameters_hash: HashDigest::of_json(&strategy).unwrap(),
            universe: symbols(),
            data_hash: digest("data"),
            cost_model_hash: digest("cost"),
            statistical_certificate_hash: digest("certificate"),
            strategy,
            valid_from: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::{fixtures::*, *};

    #[test]
    fn json_hash_is_canonical_across_object_field_order() {
        #[derive(Serialize)]
        struct ReverseOrder {
            b: u8,
            a: u8,
        }

        let digest = HashDigest::of_json(&ReverseOrder { b: 2, a: 1 }).unwrap();
        assert_eq!(digest, HashDigest::sha256(br#"{"a":1,"b":2}"#));
        assert_eq!(
            digest,
            HashDigest::of_json(&serde_json::json!({"a": 1, "b": 2})).unwrap()
        );

        let nested =
            serde_json::from_str::<serde_json::Value>(r#"{"z":{"z":2,"a":1},"a":[{"z":2,"a":1}]}"#)
                .unwrap();
        assert_eq!(
            HashDigest::of_json(&nested).unwrap(),
            HashDigest::sha256(br#"{"a":[{"a":1,"z":2}],"z":{"a":1,"z":2}}"#)
        );
    }

    #[test]
    fn json_hash_uses_rfc3339_autosi_timestamp_precision() {
        #[derive(Serialize)]
        struct TimestampEvidence {
            at: DateTime<Utc>,
        }

        let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::milliseconds(500);
        assert_eq!(
            HashDigest::of_json(&TimestampEvidence { at }).unwrap(),
            HashDigest::sha256(br#"{"at":"2026-01-01T00:00:00.500Z"}"#)
        );
        for (offset, expected) in [
            (Duration::zero(), "2026-01-01T00:00:00Z"),
            (Duration::microseconds(500), "2026-01-01T00:00:00.000500Z"),
            (Duration::nanoseconds(500), "2026-01-01T00:00:00.000000500Z"),
        ] {
            let at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + offset;
            let encoded = format!(r#"{{"at":"{expected}"}}"#);
            assert_eq!(
                HashDigest::of_json(&TimestampEvidence { at }).unwrap(),
                HashDigest::sha256(encoded)
            );
        }
    }

    #[test]
    fn json_hash_profile_accepts_full_integers_and_rejects_floats() {
        assert!(HashDigest::of_json(&i128::MIN).is_ok());
        assert!(HashDigest::of_json(&i128::MAX).is_ok());
        assert!(HashDigest::of_json(&u128::MAX).is_ok());
        assert!(HashDigest::of_json(&1.0_f64).is_err());

        let vector = serde_json::from_str::<serde_json::Value>(
            r#"{"timestamp_zero":"2026-01-01T00:00:00Z","nested":{"z":true,"a":"é"},"integer_min":-170141183460469231731687303715884105728,"timestamp_milli":"2026-01-01T00:00:00.500Z","integer_max":170141183460469231731687303715884105727,"fixed_scaled":-1234567,"array":[3,2,1],"timestamp_micro":"2026-01-01T00:00:00.000500Z"}"#,
        )
        .unwrap();
        assert_eq!(
            HashDigest::of_json(&vector).unwrap().as_hex(),
            "0bb9dbc312710da164d2837c9c00edb4067cb6e57df7fafefd19e3f74723f198"
        );
        let nanos = serde_json::json!({"at": "2026-01-01T00:00:00.000000500Z"});
        assert_eq!(
            HashDigest::of_json(&nanos).unwrap().as_hex(),
            "65eff45abf65bfee39c26619df34a5a60dbde2dfcdb90e59438f226884a824d8"
        );
    }

    #[test]
    fn release_binds_embedded_parameters() {
        let mut release = release();
        release.validate().unwrap();
        release.strategy = StrategySpec::MomentumTrend(MomentumTrendSpec {
            momentum_lookback_sessions: 126,
            trend_lookback_sessions: 126,
            cadence: RebalanceCadence::Weekly,
        });
        assert!(release.validate().is_err());
    }

    #[test]
    fn reconciliation_is_fail_closed() {
        let report = ReconciliationReport {
            generated_at: Utc::now(),
            account_fingerprint: digest("account"),
            execution_fencing_token: 1,
            differences: vec![ReconciliationDifference {
                kind: ReconciliationDifferenceKind::CashMismatch,
                subject: "cash".into(),
                local_value: Some("1.00".into()),
                broker_value: Some("0.99".into()),
                detail: "one cent".into(),
            }],
            may_resume_execution: true,
        };
        assert!(report.validate().is_err());
    }

    #[test]
    fn clearing_a_hard_halt_requires_operator_evidence() {
        let at = Utc::now();
        let mut clear = KillState::clear("operator@example", digest("approval"), at);
        clear.validate().unwrap();
        clear.operator_approved = false;
        assert!(clear.validate().is_err());
    }
}
