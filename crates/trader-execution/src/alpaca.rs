//! Minimal, paper-only Alpaca HTTP contract adapter.
//!
//! The adapter deliberately owns no credentials and no concrete HTTP client.
//! Authentication is injected by [`HttpTransport`] outside this module. It is
//! also deliberately narrower than Alpaca's full API: U.S. equities, SIP
//! quotes, whole-share DAY limit orders, and reconciliation reads only.
//!
//! Primary sources (checked 2026-07-19):
//! - Hosts/authentication: <https://docs.alpaca.markets/us/docs/authentication>
//! - Request IDs: <https://docs.alpaca.markets/us/docs/getting-started-with-trading-api>
//! - Account: <https://docs.alpaca.markets/us/reference/getaccount-1>
//! - Positions: <https://docs.alpaca.markets/us/reference/getallopenpositions>
//! - Latest quote: <https://docs.alpaca.markets/us/reference/stocklatestquotesingle-1>
//! - Orders: <https://docs.alpaca.markets/us/v1.1/reference/getallorders-1>
//! - Create order: <https://docs.alpaca.markets/us/v1.1/reference/postorder>
//! - Client ID lookup: <https://docs.alpaca.markets/us/reference/getorderbyclientorderid>
//! - Cancel: <https://docs.alpaca.markets/us/reference/deleteorderbyorderid-1>
//! - Fill activities: <https://docs.alpaca.markets/us/reference/getaccountactivitiesbyactivitytype-1>
//! - Lifecycle statuses: <https://docs.alpaca.markets/us/docs/orders-at-alpaca>
//! - Current multi-market clock schema: <https://docs.alpaca.markets/us/reference/clock-1>
//! - Current market calendar schema: <https://docs.alpaca.markets/us/reference/calendar-2>

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Timelike, Utc};
use serde::{Deserialize, Serialize};
use trader_core::{
    AccountSnapshot, AccountStatus, BrokerEvent, Environment, Fixed, FreshExecutionQuote,
    HashDigest, Money, OrderIntent, OrderSide, Price, Symbol, TimeInForce, WholeQuantity,
};

use crate::{
    config::{MARKET_DATA_API, PAPER_TRADING_API},
    coordinator::{
        BrokerSnapshot, CoordinatorPortError, ObservedBrokerSnapshot, OrderTruth,
        PageCompletionWitness, ReadOnlyBroker, SourcePageEvidence, SourcePageKind,
    },
    lifecycle::BrokerOrderStatus,
    port::{
        BrokerPort, CancellationNotDispatched, CancellationOutcome, CancellationRequestAccepted,
        RegularTradingSessionPermit, SubmissionOutcome,
    },
    rate_limit::RequestClass,
    ExecutionError,
};

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_PAGE_TOKEN_BYTES: usize = 512;
const MAX_ERROR_MESSAGE_BYTES: usize = 256;
const MAX_QUOTE_TTL_SECONDS: i64 = 15;
const MAX_QUOTE_SOURCE_AGE_SECONDS: i64 = 15;
const MAX_CLOCK_SOURCE_AGE_SECONDS: i64 = 15;
const MIN_ACCOUNT_FINGERPRINT_SALT_BYTES: usize = 32;
const MAX_ACCOUNT_FINGERPRINT_SALT_BYTES: usize = 1_024;
const US_EQUITY_MARKET: &str = "NYSE";
const US_EQUITY_TIMEZONE: &str = "America/New_York";
const ORDER_PAGE_SIZE: usize = 500;
const MAX_ORDER_PAGES: usize = 100;
const MAX_FILL_ACTIVITY_PAGES: usize = 100;

type OptionalTimeInterval = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpMethod {
    Get,
    Post,
    Delete,
}

#[derive(Clone, Eq, PartialEq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    /// Explicit budget class. The concrete transport must acquire this class
    /// before dispatch so routine work cannot consume cancel/reconcile reserve.
    pub request_class: RequestClass,
    /// Latest acceptable broker-arrival time. The concrete transport subtracts
    /// its certified p99-plus-margin arrival guard and rechecks the resulting
    /// dispatch boundary immediately before I/O. Every POST must carry one;
    /// reads/cancels carry none.
    pub not_after: Option<DateTime<Utc>>,
    pub url: String,
    /// Contains protocol headers only. The transport, not this adapter, injects
    /// authentication immediately before dispatch.
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    /// Timestamp captured by the transport when the complete response arrived.
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq)]
pub enum TransportError {
    Timeout { detail: String },
    ConnectionLost { detail: String },
    BeforeSend { detail: String },
}

impl fmt::Debug for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Timeout { .. } => "TransportError::Timeout(<redacted>)",
            Self::ConnectionLost { .. } => "TransportError::ConnectionLost(<redacted>)",
            Self::BeforeSend { .. } => "TransportError::BeforeSend(<redacted>)",
        })
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, detail) = match self {
            Self::Timeout { detail } => ("transport timeout", detail),
            Self::ConnectionLost { detail } => ("transport connection lost", detail),
            Self::BeforeSend { detail } => ("transport rejected before send", detail),
        };
        write!(formatter, "{kind}: {}", bounded_message(detail))
    }
}

impl std::error::Error for TransportError {}

#[async_trait]
pub trait HttpTransport: Send + Sync {
    /// Proves that a POST begun at `now` still has the certified p99-plus-margin
    /// allowance needed to arrive by the provider deadline. The executor calls
    /// this before committing an intent; [`send`](Self::send) must recheck at
    /// the final dispatch boundary.
    fn validate_broker_arrival_window(
        &self,
        broker_arrival_by: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), TransportError>;

    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct ResponseEvidence {
    pub request_id: Option<String>,
    pub raw_payload_hash: HashDigest,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct Observed<T> {
    pub value: T,
    pub evidence: ResponseEvidence,
}

#[derive(Clone, Eq, PartialEq)]
pub struct PagedObserved<T> {
    pub value: T,
    /// Every page retains independent request/payload evidence. Combining page
    /// hashes would destroy the ability to prove exactly which response failed.
    pub page_evidence: Vec<PageResponseEvidence>,
    completion: PaginationCompletionWitness,
}

#[derive(Clone, Eq, PartialEq)]
pub struct PageResponseEvidence {
    pub response: ResponseEvidence,
    pub request_parameters_hash: HashDigest,
    pub item_count: u32,
    pub completion_witness: Option<PaginationCompletionWitness>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum PaginationCompletionWitness {
    ShortPage,
    TimestampHorizonCrossed,
}

impl<T> PagedObserved<T> {
    /// This type cannot be constructed outside this module because its witness
    /// is private. A successful collector returns it only after a provider
    /// short page or the requested exclusive timestamp horizon was crossed.
    pub fn completeness_proven(&self) -> bool {
        matches!(
            self.completion,
            PaginationCompletionWitness::ShortPage
                | PaginationCompletionWitness::TimestampHorizonCrossed
        )
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaAccount {
    pub provider_account_id: String,
    pub account_number: String,
    /// Preserved verbatim so future provider states do not become `ACTIVE` by
    /// accident in a downstream mapper.
    pub status: String,
    pub currency: String,
    pub cash: Money,
    pub buying_power: Money,
    pub non_marginable_buying_power: Money,
    pub equity: Money,
    pub last_equity: Money,
    pub portfolio_value: Money,
    pub long_market_value: Money,
    pub short_market_value: Money,
    pub accrued_fees: Money,
    pub pending_transfer_in: Money,
    pub pending_transfer_out: Money,
    pub initial_margin: Money,
    pub maintenance_margin: Money,
    pub last_maintenance_margin: Money,
    pub regt_buying_power: Money,
    pub multiplier: Fixed,
    pub trading_blocked: bool,
    pub transfers_blocked: bool,
    pub account_blocked: bool,
    pub trade_suspended_by_user: bool,
    pub shorting_enabled: bool,
    pub created_at: DateTime<Utc>,
}

impl AlpacaAccount {
    /// Produces a non-reversible, installation-specific account identity.
    /// The salt belongs in the caller's secret store and must not be logged.
    pub fn account_fingerprint(&self, salt: &[u8]) -> Result<HashDigest, ExecutionError> {
        if !(MIN_ACCOUNT_FINGERPRINT_SALT_BYTES..=MAX_ACCOUNT_FINGERPRINT_SALT_BYTES)
            .contains(&salt.len())
        {
            return Err(ExecutionError::UnsafeConfiguration(
                "account fingerprint salt must contain 32 through 1024 bytes".into(),
            ));
        }
        let domain = b"wasp2/alpaca-paper/account-fingerprint/v1\0";
        let mut material =
            Vec::with_capacity(domain.len() + 8 + salt.len() + self.provider_account_id.len());
        material.extend_from_slice(domain);
        material.extend_from_slice(&(salt.len() as u64).to_be_bytes());
        material.extend_from_slice(salt);
        material.extend_from_slice(self.provider_account_id.as_bytes());
        let fingerprint = HashDigest::sha256(&material);
        material.fill(0);
        Ok(fingerprint)
    }

    pub fn is_trade_eligible(&self) -> bool {
        self.status == "ACTIVE"
            && self.currency == "USD"
            && !self.trading_blocked
            && !self.account_blocked
            && !self.trade_suspended_by_user
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaPosition {
    pub provider_asset_id: String,
    pub symbol: Symbol,
    pub exchange: String,
    pub asset_class: String,
    pub side: String,
    pub quantity: WholeQuantity,
    pub quantity_available: WholeQuantity,
    pub average_entry_price: Price,
    pub current_price: Price,
    pub last_day_price: Price,
    pub market_value: Money,
    pub cost_basis: Money,
    pub unrealized_pnl: Money,
    pub unrealized_intraday_pnl: Money,
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaLatestQuote {
    pub symbol: Symbol,
    pub bid_price: Price,
    pub ask_price: Price,
    pub bid_size: WholeQuantity,
    pub ask_size: WholeQuantity,
    pub provider_at: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaOrder {
    pub provider_order_id: String,
    pub client_order_id: String,
    pub symbol: Symbol,
    pub asset_class: String,
    pub side: OrderSide,
    pub quantity: Option<WholeQuantity>,
    pub notional: Option<Money>,
    pub filled_quantity: WholeQuantity,
    pub average_fill_price: Option<Price>,
    pub limit_price: Option<Price>,
    pub order_class: String,
    pub order_type: String,
    pub time_in_force: String,
    /// Preserved verbatim. `OrderLifecycle` owns the exhaustive known-state
    /// mapping and fails closed for anything new.
    pub status: String,
    pub extended_hours: bool,
    pub submitted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AlpacaOrder {
    pub fn to_broker_event(&self, evidence: &ResponseEvidence) -> BrokerEvent {
        BrokerEvent {
            provider_order_id: Some(self.provider_order_id.clone()),
            client_order_id: self.client_order_id.clone(),
            status: self.status.clone(),
            filled_quantity: self.filled_quantity,
            fill_price: self.average_fill_price,
            provider_timestamp: self.updated_at,
            received_at: evidence.received_at,
            raw_payload_hash: evidence.raw_payload_hash,
            request_id: evidence.request_id.clone(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct FillActivity {
    pub activity_id: String,
    pub activity_type: String,
    pub fill_type: String,
    pub provider_order_id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub quantity: WholeQuantity,
    pub cumulative_quantity: WholeQuantity,
    pub leaves_quantity: WholeQuantity,
    pub price: Price,
    pub transaction_at: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct FillActivityQuery {
    pub after: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub page_size: u16,
    pub page_token: Option<String>,
}

impl Default for FillActivityQuery {
    fn default() -> Self {
        Self {
            after: None,
            until: None,
            page_size: 100,
            page_token: None,
        }
    }
}

impl FillActivityQuery {
    fn validate(&self) -> Result<(), ExecutionError> {
        if !(1..=100).contains(&self.page_size) {
            return Err(ExecutionError::UnsafeConfiguration(
                "fill activity page_size must be between 1 and 100".into(),
            ));
        }
        if self
            .after
            .zip(self.until)
            .is_some_and(|(after, until)| after >= until)
        {
            return Err(ExecutionError::UnsafeConfiguration(
                "fill activity after must precede until".into(),
            ));
        }
        if let Some(token) = &self.page_token {
            validate_bounded_text("page_token", token, MAX_PAGE_TOKEN_BYTES)?;
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaMarketIdentity {
    pub acronym: String,
    pub name: String,
    pub timezone: String,
    pub mic: Option<String>,
    pub bic: Option<String>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum AlpacaMarketPhase {
    Closed,
    Pre,
    Core,
    Lunch,
    Post,
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaMarketClock {
    pub market: AlpacaMarketIdentity,
    pub timestamp: DateTime<Utc>,
    pub is_market_day: bool,
    pub next_market_open: DateTime<Utc>,
    pub next_market_close: DateTime<Utc>,
    pub phase: AlpacaMarketPhase,
    pub phase_until: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaCalendarDay {
    pub date: NaiveDate,
    pub core_start: DateTime<Utc>,
    pub core_end: DateTime<Utc>,
    pub pre_start: Option<DateTime<Utc>>,
    pub pre_end: Option<DateTime<Utc>>,
    pub post_start: Option<DateTime<Utc>>,
    pub post_end: Option<DateTime<Utc>>,
    pub lunch_start: Option<DateTime<Utc>>,
    pub lunch_end: Option<DateTime<Utc>>,
    pub settlement_date: Option<NaiveDate>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaMarketCalendar {
    pub market: AlpacaMarketIdentity,
    pub days: Vec<AlpacaCalendarDay>,
}

pub struct AlpacaPaperAdapter<T> {
    transport: T,
    environment: Environment,
}

/// Narrow broker port for startup reconciliation. It owns the full adapter but
/// deliberately exposes no submit, replace, cancel, transport, or salt access.
/// This type intentionally implements neither `Debug` nor `Serialize`.
pub struct AlpacaReadOnlyBroker<T> {
    adapter: AlpacaPaperAdapter<T>,
    fingerprint_salt: FingerprintSalt,
}

/// Secret account-identity material. No formatting or serialization trait is
/// implemented, and the bytes are overwritten before ordinary deallocation.
struct FingerprintSalt(Vec<u8>);

impl Drop for FingerprintSalt {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

impl<T> AlpacaReadOnlyBroker<T> {
    pub fn new(
        adapter: AlpacaPaperAdapter<T>,
        mut fingerprint_salt: Vec<u8>,
    ) -> Result<Self, CoordinatorPortError> {
        if !(MIN_ACCOUNT_FINGERPRINT_SALT_BYTES..=MAX_ACCOUNT_FINGERPRINT_SALT_BYTES)
            .contains(&fingerprint_salt.len())
        {
            fingerprint_salt.fill(0);
            return Err(read_only_configuration_error());
        }
        Ok(Self {
            adapter,
            fingerprint_salt: FingerprintSalt(fingerprint_salt),
        })
    }
}

impl<T> AlpacaPaperAdapter<T> {
    pub fn new(
        environment: Environment,
        trading_api_base_url: &str,
        market_data_base_url: &str,
        transport: T,
    ) -> Result<Self, ExecutionError> {
        if environment != Environment::Paper {
            return Err(ExecutionError::UnsafeConfiguration(
                "Alpaca adapter construction is paper-only".into(),
            ));
        }
        if trading_api_base_url != PAPER_TRADING_API {
            return Err(ExecutionError::UnsafeConfiguration(
                "paper adapter requires the exact pinned paper trading host".into(),
            ));
        }
        if market_data_base_url != MARKET_DATA_API {
            return Err(ExecutionError::UnsafeConfiguration(
                "paper adapter requires the exact pinned Alpaca data host".into(),
            ));
        }
        Ok(Self {
            transport,
            environment,
        })
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn ensure_paper(&self) -> Result<(), ExecutionError> {
        if self.environment != Environment::Paper {
            return Err(ExecutionError::UnsafeConfiguration(
                "non-paper adapter state cannot perform transport I/O".into(),
            ));
        }
        Ok(())
    }
}

impl<T: HttpTransport> AlpacaPaperAdapter<T> {
    /// Reads Alpaca's current v3 multi-market clock for the NYSE regular U.S.
    /// equity session. The response schema and `phase=core` vocabulary come
    /// from Alpaca's current Trading API OpenAPI definition linked above.
    pub async fn get_market_clock(&self) -> Result<Observed<AlpacaMarketClock>, ExecutionError> {
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Routine,
                None,
                format!("{PAPER_TRADING_API}/v3/clock?markets={US_EQUITY_MARKET}"),
                Vec::new(),
            )
            .await?;
        require_status("get market clock", &response, 200)?;
        let evidence = evidence(&response)?;
        let raw: RawClockEnvelope = parse_json("market clock", &response.body)?;
        if raw.clocks.len() != 1 {
            return Err(ExecutionError::Broker(
                "market clock did not return exactly one requested market".into(),
            ));
        }
        let clock: AlpacaMarketClock = raw
            .clocks
            .into_iter()
            .next()
            .expect("length checked")
            .try_into()?;
        validate_us_equity_market(&clock.market)?;
        let source_age = evidence.received_at.signed_duration_since(clock.timestamp);
        if source_age < Duration::zero()
            || source_age > Duration::seconds(MAX_CLOCK_SOURCE_AGE_SECONDS)
        {
            return Err(ExecutionError::Broker(
                "market clock is future-dated or stale at local receipt".into(),
            ));
        }
        Ok(Observed {
            value: clock,
            evidence,
        })
    }

    /// Reads the exact NYSE calendar day with UTC timestamps. The current v3
    /// schema exposes `core_start` and `core_end`, including early closes.
    pub async fn get_market_calendar(
        &self,
        date: NaiveDate,
    ) -> Result<Observed<AlpacaMarketCalendar>, ExecutionError> {
        let date = date.to_string();
        let url = with_query(
            &format!("{PAPER_TRADING_API}/v3/calendar/{US_EQUITY_MARKET}"),
            &[
                ("start", date.clone()),
                ("end", date),
                ("timezone", "UTC".into()),
            ],
        );
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Routine,
                None,
                url,
                Vec::new(),
            )
            .await?;
        require_status("get market calendar", &response, 200)?;
        let evidence = evidence(&response)?;
        let raw: RawCalendarEnvelope = parse_json("market calendar", &response.body)?;
        let calendar: AlpacaMarketCalendar = raw.try_into()?;
        validate_us_equity_market(&calendar.market)?;
        Ok(Observed {
            value: calendar,
            evidence,
        })
    }

    /// Obtains a short-lived, hash-evidenced regular-session permit. Clock and
    /// calendar must independently agree on the same exact core-session close.
    pub async fn regular_trading_session_permit(
        &self,
    ) -> Result<RegularTradingSessionPermit, ExecutionError> {
        let clock = self.get_market_clock().await?;
        if !clock.value.is_market_day || clock.value.phase != AlpacaMarketPhase::Core {
            return Err(ExecutionError::AuthorityDenied(
                "Alpaca clock is not in the regular core trading phase".into(),
            ));
        }
        let session_date = clock.value.timestamp.date_naive();
        let calendar = self.get_market_calendar(session_date).await?;
        if calendar.value.days.len() != 1 || calendar.value.days[0].date != session_date {
            return Err(ExecutionError::AuthorityDenied(
                "NYSE calendar did not return the exact current market day".into(),
            ));
        }
        let day = &calendar.value.days[0];
        if clock.value.timestamp < day.core_start
            || clock.value.timestamp >= day.core_end
            || clock.value.phase_until != day.core_end
            || clock.value.next_market_close != day.core_end
            || clock.value.next_market_open <= day.core_end
            || calendar.evidence.received_at < clock.evidence.received_at
            || calendar.evidence.received_at >= day.core_end
        {
            return Err(ExecutionError::AuthorityDenied(
                "clock and calendar do not prove the same open regular session".into(),
            ));
        }
        RegularTradingSessionPermit::verified(
            US_EQUITY_MARKET.into(),
            session_date,
            day.core_start,
            day.core_end,
            clock.value.timestamp,
            calendar.evidence.received_at,
            clock.evidence.raw_payload_hash,
            calendar.evidence.raw_payload_hash,
            clock.evidence.request_id,
            calendar.evidence.request_id,
        )
    }

    pub async fn get_account(&self) -> Result<Observed<AlpacaAccount>, ExecutionError> {
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Reconciliation,
                None,
                format!("{PAPER_TRADING_API}/v2/account"),
                Vec::new(),
            )
            .await?;
        require_status("get account", &response, 200)?;
        let evidence = evidence(&response)?;
        let raw: RawAccount = parse_json("account", &response.body)?;
        let account = raw.try_into()?;
        Ok(Observed {
            value: account,
            evidence,
        })
    }

    pub async fn get_positions(&self) -> Result<Observed<Vec<AlpacaPosition>>, ExecutionError> {
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Reconciliation,
                None,
                format!("{PAPER_TRADING_API}/v2/positions"),
                Vec::new(),
            )
            .await?;
        require_status("get positions", &response, 200)?;
        let evidence = evidence(&response)?;
        let raw: Vec<RawPosition> = parse_json("positions", &response.body)?;
        let positions = raw
            .into_iter()
            .map(AlpacaPosition::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let mut provider_asset_ids = BTreeSet::new();
        let mut symbols = BTreeSet::new();
        for position in &positions {
            if !provider_asset_ids.insert(position.provider_asset_id.clone())
                || !symbols.insert(position.symbol.clone())
            {
                return Err(ExecutionError::Broker(
                    "positions response repeated a provider asset or symbol".into(),
                ));
            }
        }
        Ok(Observed {
            value: positions,
            evidence,
        })
    }

    pub async fn latest_sip_quote(
        &self,
        symbol: &Symbol,
    ) -> Result<Observed<AlpacaLatestQuote>, ExecutionError> {
        let url = format!(
            "{MARKET_DATA_API}/v2/stocks/{}/quotes/latest?feed=sip",
            percent_encode_component(symbol.as_str())
        );
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Routine,
                None,
                url,
                Vec::new(),
            )
            .await?;
        require_status("latest SIP quote", &response, 200)?;
        let evidence = evidence(&response)?;
        let raw: RawLatestQuoteEnvelope = parse_json("latest quote", &response.body)?;
        let quote: AlpacaLatestQuote = raw.try_into()?;
        if quote.symbol != *symbol {
            return Err(ExecutionError::Broker(
                "latest quote symbol does not match request".into(),
            ));
        }
        if quote.bid_price > quote.ask_price {
            return Err(ExecutionError::Broker(
                "latest quote has a crossed bid/ask book".into(),
            ));
        }
        let source_age = evidence
            .received_at
            .signed_duration_since(quote.provider_at);
        if source_age < Duration::zero()
            || source_age > Duration::seconds(MAX_QUOTE_SOURCE_AGE_SECONDS)
        {
            return Err(ExecutionError::Broker(
                "latest quote is future-dated or stale at local receipt".into(),
            ));
        }
        Ok(Observed {
            value: quote,
            evidence,
        })
    }

    /// Materializes raw bid/ask evidence for the same Rust intent pipeline.
    /// Buys use the ask and sells use the bid; no midpoint or float is used.
    pub async fn fresh_quote(
        &self,
        symbol: &Symbol,
        side: OrderSide,
        ttl: Duration,
    ) -> Result<FreshExecutionQuote, ExecutionError> {
        if ttl <= Duration::zero() || ttl > Duration::seconds(MAX_QUOTE_TTL_SECONDS) {
            return Err(ExecutionError::UnsafeConfiguration(
                "fresh quote TTL must be positive and at most 15 seconds".into(),
            ));
        }
        let observed = self.latest_sip_quote(symbol).await?;
        let (raw_price, displayed_size) = match side {
            OrderSide::Buy => (observed.value.ask_price, observed.value.ask_size),
            OrderSide::Sell => (observed.value.bid_price, observed.value.bid_size),
        };
        if displayed_size == WholeQuantity::ZERO {
            return Err(ExecutionError::Broker(
                "selected quote side has zero displayed liquidity".into(),
            ));
        }
        let valid_until = observed
            .evidence
            .received_at
            .checked_add_signed(ttl)
            .ok_or_else(|| ExecutionError::Broker("fresh quote expiry overflow".into()))?;
        Ok(FreshExecutionQuote {
            symbol: observed.value.symbol,
            raw_price,
            provider_at: observed.value.provider_at,
            received_at: observed.evidence.received_at,
            valid_until,
            payload_hash: observed.evidence.raw_payload_hash,
        })
    }

    /// Enumerates every currently open order across all Alpaca asset classes.
    /// Any out-of-scope asset/order shape fails the entire reconciliation closed
    /// rather than disappearing behind an equity-only provider filter.
    pub async fn list_open_orders(
        &self,
    ) -> Result<PagedObserved<Vec<AlpacaOrder>>, ExecutionError> {
        self.collect_order_pages("open", None).await
    }

    /// Enumerates the complete closed-order horizon after `after` (exclusive).
    /// The first page uses the timestamp filter; subsequent pages use Alpaca's
    /// order-ID cursor and stop as soon as the horizon boundary is crossed.
    pub async fn list_recent_closed_orders(
        &self,
        after: DateTime<Utc>,
    ) -> Result<PagedObserved<Vec<AlpacaOrder>>, ExecutionError> {
        self.collect_order_pages("closed", Some(after)).await
    }

    async fn collect_order_pages(
        &self,
        status: &'static str,
        after: Option<DateTime<Utc>>,
    ) -> Result<PagedObserved<Vec<AlpacaOrder>>, ExecutionError> {
        let mut orders = Vec::new();
        let mut page_evidence = Vec::new();
        let mut seen_provider_ids = BTreeSet::new();
        let mut before_order_id: Option<String> = None;
        let mut previous_page_tail: Option<DateTime<Utc>> = None;

        for page_index in 0..MAX_ORDER_PAGES {
            let initial_timestamp_horizon = before_order_id.is_none() && after.is_some();
            let mut pairs = vec![
                ("status", status.to_owned()),
                ("limit", ORDER_PAGE_SIZE.to_string()),
                ("direction", "desc".to_owned()),
                ("nested", "false".to_owned()),
            ];
            if let Some(cursor) = &before_order_id {
                pairs.push(("before_order_id", cursor.clone()));
            } else if let Some(after) = after {
                pairs.push(("after", after.to_rfc3339_opts(SecondsFormat::AutoSi, true)));
            }
            let request_parameters_hash =
                HashDigest::of_json(&("wasp2/alpaca-order-page-request/v1", status, &pairs))
                    .map_err(|_| {
                        ExecutionError::Broker(
                            "order-page request evidence could not be hashed".into(),
                        )
                    })?;
            let response = self
                .send(
                    HttpMethod::Get,
                    RequestClass::Reconciliation,
                    None,
                    with_query(&format!("{PAPER_TRADING_API}/v2/orders"), &pairs),
                    Vec::new(),
                )
                .await?;
            require_status("list orders", &response, 200)?;
            let evidence = evidence(&response)?;
            let raw: Vec<RawOrder> = parse_json("orders", &response.body)?;
            if raw.len() > ORDER_PAGE_SIZE {
                return Err(ExecutionError::Broker(
                    "orders response exceeded the requested page bound".into(),
                ));
            }
            let page = raw
                .into_iter()
                .map(AlpacaOrder::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            if page.iter().any(|order| order.asset_class != "us_equity") {
                return Err(ExecutionError::Broker(
                    "open/recent order reconciliation found an out-of-scope asset class".into(),
                ));
            }
            validate_descending_order_page(&page, previous_page_tail)?;
            if initial_timestamp_horizon
                && after.is_some_and(|cutoff| page.iter().any(|order| order.submitted_at <= cutoff))
            {
                return Err(ExecutionError::Broker(
                    "orders response violated the exclusive after boundary".into(),
                ));
            }
            for order in &page {
                if !seen_provider_ids.insert(order.provider_order_id.clone()) {
                    return Err(ExecutionError::Broker(
                        "orders pagination repeated a provider order ID".into(),
                    ));
                }
            }

            let page_is_full = page.len() == ORDER_PAGE_SIZE;
            let crossed_horizon = after.is_some_and(|cutoff| {
                page.last()
                    .is_some_and(|oldest| oldest.submitted_at <= cutoff)
            });
            let next_cursor = page.last().map(|order| order.provider_order_id.clone());
            if page_is_full && next_cursor == before_order_id {
                return Err(ExecutionError::Broker(
                    "orders pagination cursor did not advance".into(),
                ));
            }
            previous_page_tail = page.last().map(|order| order.submitted_at);
            before_order_id = next_cursor;
            let item_count = u32::try_from(page.len()).map_err(|_| {
                ExecutionError::Broker("order-page item count exceeded its bound".into())
            })?;
            orders.extend(
                page.into_iter()
                    .filter(|order| after.is_none_or(|cutoff| order.submitted_at > cutoff)),
            );
            let completion = if crossed_horizon {
                Some(PaginationCompletionWitness::TimestampHorizonCrossed)
            } else if !page_is_full {
                Some(PaginationCompletionWitness::ShortPage)
            } else {
                None
            };
            page_evidence.push(PageResponseEvidence {
                response: evidence,
                request_parameters_hash,
                item_count,
                completion_witness: completion,
            });

            if let Some(completion) = completion {
                return Ok(PagedObserved {
                    value: orders,
                    page_evidence,
                    completion,
                });
            }
            if page_index + 1 == MAX_ORDER_PAGES {
                return Err(ExecutionError::Broker(
                    "orders pagination exceeded the fail-closed page ceiling".into(),
                ));
            }
        }
        unreachable!("bounded order-pagination loop always returns")
    }

    pub async fn list_fill_activities(
        &self,
        query: &FillActivityQuery,
    ) -> Result<PagedObserved<Vec<FillActivity>>, ExecutionError> {
        query.validate()?;
        let page_size = usize::from(query.page_size);
        let mut fills = Vec::new();
        let mut page_evidence = Vec::new();
        let mut page_token = query.page_token.clone();
        let mut seen_activity_ids = BTreeSet::new();
        let mut seen_cursors = BTreeSet::new();
        let mut previous_page_tail: Option<DateTime<Utc>> = None;
        if let Some(initial_token) = &page_token {
            seen_cursors.insert(initial_token.clone());
        }

        for page_index in 0..MAX_FILL_ACTIVITY_PAGES {
            let mut pairs = vec![
                ("direction", "asc".to_owned()),
                ("page_size", query.page_size.to_string()),
            ];
            if let Some(after) = query.after {
                pairs.push(("after", after.to_rfc3339_opts(SecondsFormat::AutoSi, true)));
            }
            if let Some(until) = query.until {
                pairs.push(("until", until.to_rfc3339_opts(SecondsFormat::AutoSi, true)));
            }
            if let Some(token) = &page_token {
                pairs.push(("page_token", token.clone()));
            }
            let request_parameters_hash =
                HashDigest::of_json(&("wasp2/alpaca-fill-activity-page-request/v1", &pairs))
                    .map_err(|_| {
                        ExecutionError::Broker(
                            "FILL activity request evidence could not be hashed".into(),
                        )
                    })?;
            let url = with_query(
                &format!("{PAPER_TRADING_API}/v2/account/activities/FILL"),
                &pairs,
            );
            let response = self
                .send(
                    HttpMethod::Get,
                    RequestClass::Reconciliation,
                    None,
                    url,
                    Vec::new(),
                )
                .await?;
            require_status("list FILL activities", &response, 200)?;
            let evidence = evidence(&response)?;
            let raw: Vec<RawFillActivity> = parse_json("FILL activities", &response.body)?;
            if raw.len() > page_size {
                return Err(ExecutionError::Broker(
                    "FILL activity response exceeded the requested page bound".into(),
                ));
            }
            let page = raw
                .into_iter()
                .map(FillActivity::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            // Alpaca defines `after`/`until` against activity creation time,
            // while FILL rows expose `transaction_time`. Do not claim that a
            // transaction timestamp proves or violates the server-side filter.
            validate_ascending_fill_page(&page, previous_page_tail)?;
            if page_token
                .as_ref()
                .is_some_and(|cursor| page.iter().any(|fill| fill.activity_id == *cursor))
            {
                return Err(ExecutionError::Broker(
                    "FILL activity response included its exclusive page_token".into(),
                ));
            }
            for fill in &page {
                if !seen_activity_ids.insert(fill.activity_id.clone()) {
                    return Err(ExecutionError::Broker(
                        "FILL activity pagination repeated an activity ID".into(),
                    ));
                }
            }

            let page_is_full = page.len() == page_size;
            let next_cursor = page.last().map(|fill| fill.activity_id.clone());
            if page_is_full {
                let cursor = next_cursor.as_ref().ok_or_else(|| {
                    ExecutionError::Broker(
                        "full FILL activity page did not produce a cursor".into(),
                    )
                })?;
                if !seen_cursors.insert(cursor.clone()) {
                    return Err(ExecutionError::Broker(
                        "FILL activity pagination cursor did not advance".into(),
                    ));
                }
            }
            previous_page_tail = page.last().map(|fill| fill.transaction_at);
            let item_count = u32::try_from(page.len()).map_err(|_| {
                ExecutionError::Broker("FILL activity page item count exceeded its bound".into())
            })?;
            fills.extend(page);
            let completion = (!page_is_full).then_some(PaginationCompletionWitness::ShortPage);
            page_evidence.push(PageResponseEvidence {
                response: evidence,
                request_parameters_hash,
                item_count,
                completion_witness: completion,
            });

            // Alpaca exposes no next-page flag. A short page is the provider's
            // only completeness witness; full pages always require one more
            // request using the exact last activity ID as page_token.
            if let Some(completion) = completion {
                return Ok(PagedObserved {
                    value: fills,
                    page_evidence,
                    completion,
                });
            }
            page_token = next_cursor;
            if page_index + 1 == MAX_FILL_ACTIVITY_PAGES {
                return Err(ExecutionError::Broker(
                    "FILL activity pagination exceeded the fail-closed page ceiling".into(),
                ));
            }
        }
        unreachable!("bounded FILL-pagination loop always returns")
    }

    pub async fn submit_order(
        &self,
        intent: &OrderIntent,
        session_permit: &RegularTradingSessionPermit,
        not_after: DateTime<Utc>,
    ) -> Result<SubmissionOutcome, ExecutionError> {
        validate_intent(intent)?;
        session_permit.validate_submission_deadline(intent, not_after)?;
        let body = serde_json::to_vec(&CreateOrderRequest {
            symbol: intent.symbol.as_str(),
            quantity: intent.quantity.get().to_string(),
            side: match intent.side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            order_type: "limit",
            time_in_force: "day",
            limit_price: canonical_price(intent.limit_price),
            extended_hours: false,
            client_order_id: &intent.client_order_id,
            order_class: "simple",
        })
        .map_err(|error| ExecutionError::Broker(format!("serialize create order: {error}")))?;

        let response = match self
            .send(
                HttpMethod::Post,
                RequestClass::Routine,
                Some(not_after),
                format!("{PAPER_TRADING_API}/v2/orders"),
                body,
            )
            .await
        {
            Ok(response) => response,
            Err(ExecutionError::SubmissionUnknown(detail)) => {
                return Ok(SubmissionOutcome::Unknown { detail });
            }
            Err(error) => return Err(error),
        };
        if response.status != 200 {
            return Ok(SubmissionOutcome::Unknown {
                detail: status_detail("submit order", &response),
            });
        }
        let evidence = match mutation_evidence(&response) {
            Ok(evidence) => evidence,
            Err(_) => {
                return Ok(SubmissionOutcome::Unknown {
                    detail: format!(
                        "successful POST omitted trustworthy request evidence; payload_hash={}",
                        HashDigest::sha256(&response.body)
                    ),
                });
            }
        };
        let raw: RawOrder = match parse_json("submitted order", &response.body) {
            Ok(raw) => raw,
            Err(error) => {
                return Ok(SubmissionOutcome::Unknown {
                    detail: format!("submitted order response was not trustworthy: {error}"),
                });
            }
        };
        let order: AlpacaOrder = match raw.try_into() {
            Ok(order) => order,
            Err(error) => {
                return Ok(SubmissionOutcome::Unknown {
                    detail: format!("submitted order response was not trustworthy: {error}"),
                });
            }
        };
        if !order_matches_intent(&order, intent) {
            return Ok(SubmissionOutcome::Unknown {
                detail: format!(
                    "submitted order identity/contract mismatch; request_id={:?} payload_hash={}",
                    evidence.request_id, evidence.raw_payload_hash
                ),
            });
        }
        Ok(SubmissionOutcome::Observed(
            order.to_broker_event(&evidence),
        ))
    }

    pub async fn get_order_by_client_order_id(
        &self,
        client_order_id: &str,
    ) -> Result<Observed<Option<AlpacaOrder>>, ExecutionError> {
        validate_bounded_text("client_order_id", client_order_id, MAX_IDENTIFIER_BYTES)?;
        let url = format!(
            "{PAPER_TRADING_API}/v2/orders:by_client_order_id?client_order_id={}",
            percent_encode_component(client_order_id)
        );
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Reconciliation,
                None,
                url,
                Vec::new(),
            )
            .await?;
        let evidence = evidence(&response)?;
        if response.status == 404 {
            return Ok(Observed {
                value: None,
                evidence,
            });
        }
        require_status("get order by client_order_id", &response, 200)?;
        let raw: RawOrder = parse_json("order lookup", &response.body)?;
        let order: AlpacaOrder = raw.try_into()?;
        if order.client_order_id != client_order_id {
            return Err(ExecutionError::Broker(
                "order lookup returned a different client_order_id".into(),
            ));
        }
        Ok(Observed {
            value: Some(order),
            evidence,
        })
    }

    /// Read-only recovery lookup for a cancellation whose DELETE may already
    /// have reached Alpaca. The provider order ID is both the request key and a
    /// response invariant; a mismatch fails closed.
    pub async fn get_order_by_provider_order_id(
        &self,
        provider_order_id: &str,
    ) -> Result<Observed<Option<AlpacaOrder>>, ExecutionError> {
        validate_bounded_text("provider_order_id", provider_order_id, MAX_IDENTIFIER_BYTES)?;
        let url = format!(
            "{PAPER_TRADING_API}/v2/orders/{}",
            percent_encode_component(provider_order_id)
        );
        let response = self
            .send(
                HttpMethod::Get,
                RequestClass::Reconciliation,
                None,
                url,
                Vec::new(),
            )
            .await?;
        let evidence = evidence(&response)?;
        if response.status == 404 {
            return Ok(Observed {
                value: None,
                evidence,
            });
        }
        require_status("get order by provider_order_id", &response, 200)?;
        let raw: RawOrder = parse_json("provider order lookup", &response.body)?;
        let order: AlpacaOrder = raw.try_into()?;
        if order.provider_order_id != provider_order_id {
            return Err(ExecutionError::Broker(
                "provider order lookup returned a different provider_order_id".into(),
            ));
        }
        Ok(Observed {
            value: Some(order),
            evidence,
        })
    }

    pub async fn cancel_order(
        &self,
        provider_order_id: &str,
    ) -> Result<CancellationOutcome, ExecutionError> {
        validate_bounded_text("provider_order_id", provider_order_id, MAX_IDENTIFIER_BYTES)?;
        self.ensure_paper()?;
        let url = format!(
            "{PAPER_TRADING_API}/v2/orders/{}",
            percent_encode_component(provider_order_id)
        );
        let request = HttpRequest {
            method: HttpMethod::Delete,
            request_class: RequestClass::Cancel,
            not_after: None,
            url,
            headers: BTreeMap::from([("Accept".into(), "application/json".into())]),
            body: Vec::new(),
        };
        let response = match self.transport.send(request).await {
            Ok(response) => response,
            Err(TransportError::BeforeSend { detail }) => {
                let observed_at = postgres_microsecond_timestamp(Utc::now());
                let detail = bounded_message(&detail);
                let reason_code = "TRANSPORT_BEFORE_SEND".to_owned();
                let evidence_hash = HashDigest::of_json(&serde_json::json!({
                    "provider_order_id": provider_order_id,
                    "observed_at": observed_at,
                    "reason_code": &reason_code,
                    "detail": &detail,
                }))?;
                return Ok(CancellationOutcome::NotDispatched(
                    CancellationNotDispatched {
                        provider_order_id: provider_order_id.into(),
                        observed_at,
                        reason_code,
                        detail,
                        evidence_hash,
                    },
                ));
            }
            Err(TransportError::Timeout { detail }) => {
                return Ok(CancellationOutcome::Unknown {
                    detail: format!(
                        "broker mutation timed out; reconcile by stable identity: {}",
                        bounded_message(&detail)
                    ),
                });
            }
            Err(TransportError::ConnectionLost { detail }) => {
                return Ok(CancellationOutcome::Unknown {
                    detail: format!(
                        "broker mutation connection lost; reconcile by stable identity: {}",
                        bounded_message(&detail)
                    ),
                });
            }
        };
        if response.status != 204 {
            return Ok(CancellationOutcome::Unknown {
                detail: status_detail("cancel order", &response),
            });
        }
        let evidence = match mutation_evidence(&response) {
            Ok(evidence) => evidence,
            Err(_) => {
                return Ok(CancellationOutcome::Unknown {
                    detail: format!(
                        "successful DELETE omitted trustworthy request evidence; payload_hash={}",
                        HashDigest::sha256(&response.body)
                    ),
                });
            }
        };
        Ok(CancellationOutcome::RequestAccepted(
            CancellationRequestAccepted {
                provider_order_id: provider_order_id.into(),
                accepted_at: evidence.received_at,
                request_id: evidence
                    .request_id
                    .expect("mutation evidence always has a request ID"),
                raw_payload_hash: evidence.raw_payload_hash,
            },
        ))
    }

    async fn send(
        &self,
        method: HttpMethod,
        request_class: RequestClass,
        not_after: Option<DateTime<Utc>>,
        url: String,
        body: Vec<u8>,
    ) -> Result<HttpResponse, ExecutionError> {
        self.ensure_paper()?;
        let mut headers = BTreeMap::from([("Accept".into(), "application/json".into())]);
        if method == HttpMethod::Post {
            headers.insert("Content-Type".into(), "application/json".into());
        }
        let request = HttpRequest {
            method,
            request_class,
            not_after,
            url,
            headers,
            body,
        };
        match self.transport.send(request).await {
            Ok(response) => Ok(response),
            Err(TransportError::Timeout { detail }) => {
                let detail = bounded_message(&detail);
                if matches!(method, HttpMethod::Post | HttpMethod::Delete) {
                    Err(ExecutionError::SubmissionUnknown(format!(
                        "broker mutation timed out; reconcile by stable identity: {detail}"
                    )))
                } else {
                    Err(ExecutionError::Broker(format!(
                        "transport timeout: {detail}"
                    )))
                }
            }
            Err(TransportError::ConnectionLost { detail }) => {
                let detail = bounded_message(&detail);
                if matches!(method, HttpMethod::Post | HttpMethod::Delete) {
                    Err(ExecutionError::SubmissionUnknown(format!(
                        "broker mutation connection lost; reconcile by stable identity: {detail}"
                    )))
                } else {
                    Err(ExecutionError::Broker(format!(
                        "transport connection lost: {detail}"
                    )))
                }
            }
            Err(TransportError::BeforeSend { detail }) => {
                let detail = bounded_message(&detail);
                Err(ExecutionError::Broker(format!(
                    "transport rejected before send: {detail}"
                )))
            }
        }
    }
}

impl<T: HttpTransport> AlpacaReadOnlyBroker<T> {
    async fn collect_snapshot_with_evidence(
        &self,
        snapshot_round: u8,
    ) -> Result<ObservedBrokerSnapshot, ExecutionError> {
        if !matches!(snapshot_round, 1 | 2) {
            return Err(ExecutionError::UnsafeConfiguration(
                "read-only snapshot round must be one or two".into(),
            ));
        }
        let account = self.adapter.get_account().await?;
        let account_fingerprint = account
            .value
            .account_fingerprint(&self.fingerprint_salt.0)?;
        let account_created_at = account.value.created_at;

        let positions = self.adapter.get_positions().await?;
        let open_orders = self.adapter.list_open_orders().await?;
        let closed_orders = self
            .adapter
            .list_recent_closed_orders(account_created_at)
            .await?;
        let fills = self
            .adapter
            .list_fill_activities(&FillActivityQuery {
                after: Some(account_created_at),
                ..FillActivityQuery::default()
            })
            .await?;

        if !open_orders.completeness_proven()
            || !closed_orders.completeness_proven()
            || !fills.completeness_proven()
        {
            return Err(ExecutionError::Broker(
                "read-only broker pagination was not complete".into(),
            ));
        }

        let mut pages = Vec::with_capacity(
            2 + open_orders.page_evidence.len()
                + closed_orders.page_evidence.len()
                + fills.page_evidence.len(),
        );
        pages.push(single_source_page(
            snapshot_round,
            SourcePageKind::Account,
            "wasp2/alpaca-account-request/v1",
            &account.evidence,
            1,
        )?);
        pages.push(single_source_page(
            snapshot_round,
            SourcePageKind::Positions,
            "wasp2/alpaca-positions-request/v1",
            &positions.evidence,
            u32::try_from(positions.value.len()).map_err(|_| {
                ExecutionError::Broker("position response item count exceeded its bound".into())
            })?,
        )?);
        append_paged_source_evidence(
            &mut pages,
            snapshot_round,
            SourcePageKind::OpenOrders,
            &open_orders.page_evidence,
        )?;
        append_paged_source_evidence(
            &mut pages,
            snapshot_round,
            SourcePageKind::ClosedOrders,
            &closed_orders.page_evidence,
        )?;
        append_paged_source_evidence(
            &mut pages,
            snapshot_round,
            SourcePageKind::FillActivities,
            &fills.page_evidence,
        )?;

        let mut normalized_positions = BTreeMap::new();
        let mut position_asset_ids = BTreeMap::new();
        let mut position_available_quantities = BTreeMap::new();
        for position in positions.value {
            let symbol = position.symbol;
            if position_asset_ids
                .insert(symbol.clone(), position.provider_asset_id)
                .is_some()
            {
                return Err(ExecutionError::Broker(
                    "read-only broker snapshot repeated a position asset identity".into(),
                ));
            }
            if normalized_positions
                .insert(symbol.clone(), position.quantity)
                .is_some()
            {
                return Err(ExecutionError::Broker(
                    "read-only broker snapshot repeated a position symbol".into(),
                ));
            }
            if position_available_quantities
                .insert(symbol, position.quantity_available)
                .is_some()
            {
                return Err(ExecutionError::Broker(
                    "read-only broker snapshot repeated position availability".into(),
                ));
            }
        }

        let mut provider_order_ids = BTreeSet::new();
        let mut client_order_ids = BTreeSet::new();
        let mut orders = BTreeMap::new();
        normalize_snapshot_orders(
            open_orders.value,
            &mut provider_order_ids,
            &mut client_order_ids,
            &mut orders,
        )?;
        normalize_snapshot_orders(
            closed_orders.value,
            &mut provider_order_ids,
            &mut client_order_ids,
            &mut orders,
        )?;

        let mut fill_fingerprints = fills
            .value
            .iter()
            .map(fill_fingerprint)
            .collect::<Result<Vec<_>, _>>()?;
        fill_fingerprints.sort_unstable();

        let source_evidence_hashes = pages
            .iter()
            .map(|evidence| evidence.raw_payload_hash)
            .collect();

        Ok(ObservedBrokerSnapshot {
            snapshot: BrokerSnapshot {
                account_fingerprint,
                account_status: map_account_status(&account.value.status),
                trading_blocked: account.value.trading_blocked,
                account_blocked: account.value.account_blocked,
                transfers_blocked: account.value.transfers_blocked,
                trade_suspended_by_user: account.value.trade_suspended_by_user,
                usd_currency: account.value.currency == "USD",
                cash: account.value.cash,
                buying_power: account.value.buying_power,
                non_marginable_buying_power: account.value.non_marginable_buying_power,
                equity: account.value.equity,
                last_equity: account.value.last_equity,
                portfolio_value: account.value.portfolio_value,
                long_market_value: account.value.long_market_value,
                short_market_value: account.value.short_market_value,
                accrued_fees: account.value.accrued_fees,
                pending_transfer_in: account.value.pending_transfer_in,
                pending_transfer_out: account.value.pending_transfer_out,
                initial_margin: account.value.initial_margin,
                maintenance_margin: account.value.maintenance_margin,
                last_maintenance_margin: account.value.last_maintenance_margin,
                regt_buying_power: account.value.regt_buying_power,
                multiplier: account.value.multiplier,
                shorting_enabled: account.value.shorting_enabled,
                positions: normalized_positions,
                position_asset_ids,
                position_available_quantities,
                orders,
                fill_fingerprints,
                source_evidence_hashes,
            },
            pages,
        })
    }

    async fn collect_snapshot(&self) -> Result<BrokerSnapshot, ExecutionError> {
        self.collect_snapshot_with_evidence(1)
            .await
            .map(|observed| observed.snapshot)
    }
}

#[async_trait]
impl<T: HttpTransport> ReadOnlyBroker for AlpacaReadOnlyBroker<T> {
    async fn read_snapshot(&mut self) -> Result<BrokerSnapshot, CoordinatorPortError> {
        self.collect_snapshot()
            .await
            .map_err(|_| read_only_snapshot_error())
    }

    async fn read_snapshot_with_evidence(
        &mut self,
        snapshot_round: u8,
    ) -> Result<ObservedBrokerSnapshot, CoordinatorPortError> {
        self.collect_snapshot_with_evidence(snapshot_round)
            .await
            .map_err(|_| read_only_snapshot_error())
    }
}

/// The order operations satisfy the existing executor port. Account snapshots
/// intentionally fail closed because Alpaca's account endpoint does not expose
/// the certified high-water drawdown required by `AccountSnapshot`; a local
/// risk-state combiner must construct that object from the direct typed reads.
#[async_trait]
impl<T: HttpTransport> BrokerPort for AlpacaPaperAdapter<T> {
    async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
        Err(ExecutionError::Broker(
            "account_snapshot requires local certified drawdown state; use get_account and get_positions"
                .into(),
        ))
    }

    fn validate_submission_window(
        &self,
        broker_arrival_by: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        self.transport
            .validate_broker_arrival_window(broker_arrival_by, now)
            .map_err(|_| {
                ExecutionError::AuthorityDenied(
                    "certified broker-arrival allowance is insufficient for dispatch".into(),
                )
            })
    }

    async fn find_order_by_client_id(
        &self,
        expected_intent: &OrderIntent,
    ) -> Result<Option<BrokerEvent>, ExecutionError> {
        let observed = self
            .get_order_by_client_order_id(&expected_intent.client_order_id)
            .await?;
        observed
            .value
            .map(|order| {
                if !order_matches_intent(&order, expected_intent) {
                    return Err(ExecutionError::Broker(
                        "client-ID recovery returned an order that differs from the committed intent"
                            .into(),
                    ));
                }
                Ok(order.to_broker_event(&observed.evidence))
            })
            .transpose()
    }

    async fn find_order_by_provider_id(
        &self,
        provider_order_id: &str,
        expected_client_order_id: &str,
    ) -> Result<Option<BrokerEvent>, ExecutionError> {
        validate_bounded_text(
            "expected_client_order_id",
            expected_client_order_id,
            MAX_IDENTIFIER_BYTES,
        )?;
        let observed = self
            .get_order_by_provider_order_id(provider_order_id)
            .await?;
        let Some(order) = observed.value else {
            return Ok(None);
        };
        if order.client_order_id != expected_client_order_id {
            return Err(ExecutionError::Broker(
                "provider order lookup returned a different client_order_id".into(),
            ));
        }
        Ok(Some(order.to_broker_event(&observed.evidence)))
    }

    async fn submit_committed_intent(
        &self,
        intent: &OrderIntent,
        session_permit: &RegularTradingSessionPermit,
        not_after: DateTime<Utc>,
    ) -> Result<SubmissionOutcome, ExecutionError> {
        self.submit_order(intent, session_permit, not_after).await
    }

    async fn cancel_order(
        &self,
        provider_order_id: &str,
    ) -> Result<CancellationOutcome, ExecutionError> {
        AlpacaPaperAdapter::cancel_order(self, provider_order_id).await
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireNumber {
    Text(String),
    Number(serde_json::Number),
}

impl WireNumber {
    fn text(&self) -> Cow<'_, str> {
        match self {
            Self::Text(value) => Cow::Borrowed(value),
            Self::Number(value) => Cow::Owned(value.to_string()),
        }
    }
}

#[derive(Deserialize)]
struct RawMarketIdentity {
    acronym: String,
    name: String,
    timezone: String,
    mic: Option<String>,
    bic: Option<String>,
}

impl TryFrom<RawMarketIdentity> for AlpacaMarketIdentity {
    type Error = ExecutionError;

    fn try_from(raw: RawMarketIdentity) -> Result<Self, Self::Error> {
        validate_bounded_text("market acronym", &raw.acronym, 32)?;
        validate_bounded_text("market name", &raw.name, 128)?;
        validate_bounded_text("market timezone", &raw.timezone, 64)?;
        if let Some(mic) = &raw.mic {
            validate_bounded_text("market MIC", mic, 11)?;
        }
        if let Some(bic) = &raw.bic {
            validate_bounded_text("market BIC", bic, 11)?;
        }
        Ok(Self {
            acronym: raw.acronym,
            name: raw.name,
            timezone: raw.timezone,
            mic: raw.mic,
            bic: raw.bic,
        })
    }
}

#[derive(Deserialize)]
struct RawClockEnvelope {
    clocks: Vec<RawMarketClock>,
}

#[derive(Deserialize)]
struct RawMarketClock {
    market: RawMarketIdentity,
    timestamp: String,
    is_market_day: bool,
    next_market_open: String,
    next_market_close: String,
    phase: String,
    phase_until: String,
}

impl TryFrom<RawMarketClock> for AlpacaMarketClock {
    type Error = ExecutionError;

    fn try_from(raw: RawMarketClock) -> Result<Self, Self::Error> {
        let timestamp = parse_timestamp("clock timestamp", &raw.timestamp)?;
        let next_market_open = parse_timestamp("next market open", &raw.next_market_open)?;
        let next_market_close = parse_timestamp("next market close", &raw.next_market_close)?;
        let phase_until = parse_timestamp("clock phase end", &raw.phase_until)?;
        let phase = match raw.phase.as_str() {
            "closed" => AlpacaMarketPhase::Closed,
            "pre" => AlpacaMarketPhase::Pre,
            "core" => AlpacaMarketPhase::Core,
            "lunch" => AlpacaMarketPhase::Lunch,
            "post" => AlpacaMarketPhase::Post,
            _ => {
                return Err(ExecutionError::Broker(
                    "market clock returned an unknown phase".into(),
                ));
            }
        };
        if next_market_open <= timestamp
            || next_market_close <= timestamp
            || phase_until <= timestamp
        {
            return Err(ExecutionError::Broker(
                "market clock timestamps are not future ordered".into(),
            ));
        }
        Ok(Self {
            market: raw.market.try_into()?,
            timestamp,
            is_market_day: raw.is_market_day,
            next_market_open,
            next_market_close,
            phase,
            phase_until,
        })
    }
}

#[derive(Deserialize)]
struct RawCalendarEnvelope {
    market: RawMarketIdentity,
    calendar: Vec<RawCalendarDay>,
}

#[derive(Deserialize)]
struct RawCalendarDay {
    date: String,
    core_start: String,
    core_end: String,
    pre_start: Option<String>,
    pre_end: Option<String>,
    post_start: Option<String>,
    post_end: Option<String>,
    lunch_start: Option<String>,
    lunch_end: Option<String>,
    settlement_date: Option<String>,
}

impl TryFrom<RawCalendarDay> for AlpacaCalendarDay {
    type Error = ExecutionError;

    fn try_from(raw: RawCalendarDay) -> Result<Self, Self::Error> {
        let date = parse_date("calendar date", &raw.date)?;
        let core_start = parse_timestamp("calendar core_start", &raw.core_start)?;
        let core_end = parse_timestamp("calendar core_end", &raw.core_end)?;
        if core_start >= core_end
            || core_start.date_naive() != date
            || core_end.date_naive() != date
        {
            return Err(ExecutionError::Broker(
                "calendar core session is empty or does not match its date".into(),
            ));
        }
        let (pre_start, pre_end) =
            parse_optional_interval("calendar pre session", raw.pre_start, raw.pre_end)?;
        let (post_start, post_end) =
            parse_optional_interval("calendar post session", raw.post_start, raw.post_end)?;
        let (lunch_start, lunch_end) =
            parse_optional_interval("calendar lunch session", raw.lunch_start, raw.lunch_end)?;
        if pre_end.is_some_and(|end| end > core_start)
            || post_start.is_some_and(|start| start < core_end)
        {
            return Err(ExecutionError::Broker(
                "calendar optional sessions conflict with the core session".into(),
            ));
        }
        Ok(Self {
            date,
            core_start,
            core_end,
            pre_start,
            pre_end,
            post_start,
            post_end,
            lunch_start,
            lunch_end,
            settlement_date: raw
                .settlement_date
                .as_deref()
                .map(|value| parse_date("settlement date", value))
                .transpose()?,
        })
    }
}

impl TryFrom<RawCalendarEnvelope> for AlpacaMarketCalendar {
    type Error = ExecutionError;

    fn try_from(raw: RawCalendarEnvelope) -> Result<Self, Self::Error> {
        if raw.calendar.len() > 1 {
            return Err(ExecutionError::Broker(
                "single-day calendar response exceeded its requested bound".into(),
            ));
        }
        Ok(Self {
            market: raw.market.try_into()?,
            days: raw
                .calendar
                .into_iter()
                .map(AlpacaCalendarDay::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Deserialize)]
struct RawAccount {
    id: String,
    account_number: String,
    status: String,
    currency: String,
    cash: WireNumber,
    buying_power: WireNumber,
    non_marginable_buying_power: WireNumber,
    equity: WireNumber,
    last_equity: WireNumber,
    portfolio_value: WireNumber,
    long_market_value: WireNumber,
    short_market_value: WireNumber,
    accrued_fees: WireNumber,
    pending_transfer_in: WireNumber,
    pending_transfer_out: WireNumber,
    initial_margin: WireNumber,
    maintenance_margin: WireNumber,
    last_maintenance_margin: WireNumber,
    regt_buying_power: WireNumber,
    multiplier: WireNumber,
    trading_blocked: bool,
    transfers_blocked: bool,
    account_blocked: bool,
    trade_suspended_by_user: bool,
    shorting_enabled: bool,
    created_at: String,
}

impl TryFrom<RawAccount> for AlpacaAccount {
    type Error = ExecutionError;

    fn try_from(raw: RawAccount) -> Result<Self, Self::Error> {
        validate_bounded_text("account id", &raw.id, MAX_IDENTIFIER_BYTES)?;
        validate_bounded_text("account number", &raw.account_number, MAX_IDENTIFIER_BYTES)?;
        validate_bounded_text("account status", &raw.status, MAX_IDENTIFIER_BYTES)?;
        validate_bounded_text("account currency", &raw.currency, 16)?;
        Ok(Self {
            provider_account_id: raw.id,
            account_number: raw.account_number,
            status: raw.status,
            currency: raw.currency,
            cash: parse_number("cash", &raw.cash)?,
            buying_power: parse_number("buying_power", &raw.buying_power)?,
            non_marginable_buying_power: parse_number(
                "non_marginable_buying_power",
                &raw.non_marginable_buying_power,
            )?,
            equity: parse_number("equity", &raw.equity)?,
            last_equity: parse_number("last_equity", &raw.last_equity)?,
            portfolio_value: parse_number("portfolio_value", &raw.portfolio_value)?,
            long_market_value: parse_number("long_market_value", &raw.long_market_value)?,
            short_market_value: parse_number("short_market_value", &raw.short_market_value)?,
            accrued_fees: parse_number("accrued_fees", &raw.accrued_fees)?,
            pending_transfer_in: parse_number("pending_transfer_in", &raw.pending_transfer_in)?,
            pending_transfer_out: parse_number("pending_transfer_out", &raw.pending_transfer_out)?,
            initial_margin: parse_number("initial_margin", &raw.initial_margin)?,
            maintenance_margin: parse_number("maintenance_margin", &raw.maintenance_margin)?,
            last_maintenance_margin: parse_number(
                "last_maintenance_margin",
                &raw.last_maintenance_margin,
            )?,
            regt_buying_power: parse_number("regt_buying_power", &raw.regt_buying_power)?,
            multiplier: parse_number("multiplier", &raw.multiplier)?,
            trading_blocked: raw.trading_blocked,
            transfers_blocked: raw.transfers_blocked,
            account_blocked: raw.account_blocked,
            trade_suspended_by_user: raw.trade_suspended_by_user,
            shorting_enabled: raw.shorting_enabled,
            created_at: parse_timestamp("created_at", &raw.created_at)?,
        })
    }
}

#[derive(Deserialize)]
struct RawPosition {
    asset_id: String,
    symbol: String,
    exchange: String,
    asset_class: String,
    avg_entry_price: WireNumber,
    qty: WireNumber,
    qty_available: WireNumber,
    side: String,
    market_value: WireNumber,
    cost_basis: WireNumber,
    unrealized_pl: WireNumber,
    unrealized_intraday_pl: WireNumber,
    current_price: WireNumber,
    lastday_price: WireNumber,
}

impl TryFrom<RawPosition> for AlpacaPosition {
    type Error = ExecutionError;

    fn try_from(raw: RawPosition) -> Result<Self, Self::Error> {
        validate_bounded_text("asset id", &raw.asset_id, MAX_IDENTIFIER_BYTES)?;
        validate_bounded_text("exchange", &raw.exchange, 32)?;
        if raw.asset_class != "us_equity" || raw.side != "long" {
            return Err(ExecutionError::Broker(
                "v1 position is not a long U.S. equity".into(),
            ));
        }
        let quantity = parse_whole("position qty", &raw.qty)?;
        let quantity_available = parse_whole("position qty_available", &raw.qty_available)?;
        if quantity == WholeQuantity::ZERO || quantity_available.get() > quantity.get() {
            return Err(ExecutionError::Broker(
                "position quantity is zero or available quantity exceeds held quantity".into(),
            ));
        }
        Ok(Self {
            provider_asset_id: raw.asset_id,
            symbol: parse_symbol(raw.symbol)?,
            exchange: raw.exchange,
            asset_class: raw.asset_class,
            side: raw.side,
            quantity,
            quantity_available,
            average_entry_price: parse_positive_price("avg_entry_price", &raw.avg_entry_price)?,
            current_price: parse_positive_price("current_price", &raw.current_price)?,
            last_day_price: parse_positive_price("lastday_price", &raw.lastday_price)?,
            market_value: parse_number("market_value", &raw.market_value)?,
            cost_basis: parse_number("cost_basis", &raw.cost_basis)?,
            unrealized_pnl: parse_number("unrealized_pl", &raw.unrealized_pl)?,
            unrealized_intraday_pnl: parse_number(
                "unrealized_intraday_pl",
                &raw.unrealized_intraday_pl,
            )?,
        })
    }
}

#[derive(Deserialize)]
struct RawLatestQuoteEnvelope {
    symbol: String,
    quote: RawLatestQuote,
}

#[derive(Deserialize)]
struct RawLatestQuote {
    #[serde(rename = "bp")]
    bid_price: WireNumber,
    #[serde(rename = "ap")]
    ask_price: WireNumber,
    #[serde(rename = "bs")]
    bid_size: WireNumber,
    #[serde(rename = "as")]
    ask_size: WireNumber,
    #[serde(rename = "t")]
    timestamp: String,
}

impl TryFrom<RawLatestQuoteEnvelope> for AlpacaLatestQuote {
    type Error = ExecutionError;

    fn try_from(raw: RawLatestQuoteEnvelope) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: parse_symbol(raw.symbol)?,
            bid_price: parse_positive_price("bid price", &raw.quote.bid_price)?,
            ask_price: parse_positive_price("ask price", &raw.quote.ask_price)?,
            bid_size: parse_whole("bid size", &raw.quote.bid_size)?,
            ask_size: parse_whole("ask size", &raw.quote.ask_size)?,
            provider_at: parse_timestamp("quote timestamp", &raw.quote.timestamp)?,
        })
    }
}

#[derive(Deserialize)]
struct RawOrder {
    id: String,
    client_order_id: String,
    symbol: String,
    asset_class: String,
    side: String,
    qty: Option<WireNumber>,
    notional: Option<WireNumber>,
    filled_qty: WireNumber,
    filled_avg_price: Option<WireNumber>,
    limit_price: Option<WireNumber>,
    order_class: String,
    order_type: String,
    /// Alpaca currently returns both `order_type` and its legacy `type` alias.
    #[serde(rename = "type")]
    legacy_order_type: String,
    time_in_force: String,
    status: String,
    extended_hours: bool,
    submitted_at: String,
    updated_at: String,
}

impl TryFrom<RawOrder> for AlpacaOrder {
    type Error = ExecutionError;

    fn try_from(raw: RawOrder) -> Result<Self, Self::Error> {
        validate_bounded_text("order id", &raw.id, MAX_IDENTIFIER_BYTES)?;
        validate_bounded_text(
            "client_order_id",
            &raw.client_order_id,
            MAX_IDENTIFIER_BYTES,
        )?;
        validate_bounded_text("order status", &raw.status, MAX_IDENTIFIER_BYTES)?;
        validate_bounded_text("order type", &raw.order_type, 64)?;
        if raw.legacy_order_type != raw.order_type {
            return Err(ExecutionError::Broker(
                "provider order_type and type fields disagree".into(),
            ));
        }
        validate_bounded_text("time_in_force", &raw.time_in_force, 32)?;
        validate_bounded_text("order class", &raw.order_class, 32).or_else(|error| {
            if raw.order_class.is_empty() {
                Ok(())
            } else {
                Err(error)
            }
        })?;
        let quantity = raw
            .qty
            .as_ref()
            .map(|value| parse_whole("order qty", value))
            .transpose()?;
        let notional = raw
            .notional
            .as_ref()
            .map(|value| parse_number("order notional", value))
            .transpose()?;
        if quantity.is_some() == notional.is_some() {
            return Err(ExecutionError::Broker(
                "order must contain exactly one of qty or notional".into(),
            ));
        }
        let filled_quantity = parse_whole("filled_qty", &raw.filled_qty)?;
        if quantity.is_some_and(|quantity| filled_quantity.get() > quantity.get()) {
            return Err(ExecutionError::Broker(
                "filled quantity exceeds order quantity".into(),
            ));
        }
        Ok(Self {
            provider_order_id: raw.id,
            client_order_id: raw.client_order_id,
            symbol: parse_symbol(raw.symbol)?,
            asset_class: raw.asset_class,
            side: parse_side(&raw.side)?,
            quantity,
            notional,
            filled_quantity,
            average_fill_price: raw
                .filled_avg_price
                .as_ref()
                .map(|value| parse_positive_price("filled_avg_price", value))
                .transpose()?,
            limit_price: raw
                .limit_price
                .as_ref()
                .map(|value| parse_positive_price("limit_price", value))
                .transpose()?,
            order_class: raw.order_class,
            order_type: raw.order_type,
            time_in_force: raw.time_in_force,
            status: raw.status,
            extended_hours: raw.extended_hours,
            submitted_at: parse_timestamp("submitted_at", &raw.submitted_at)?,
            updated_at: parse_timestamp("updated_at", &raw.updated_at)?,
        })
    }
}

#[derive(Deserialize)]
struct RawFillActivity {
    id: String,
    activity_type: String,
    #[serde(rename = "type")]
    fill_type: String,
    order_id: String,
    symbol: String,
    side: String,
    qty: WireNumber,
    cum_qty: WireNumber,
    leaves_qty: WireNumber,
    price: WireNumber,
    transaction_time: String,
}

impl TryFrom<RawFillActivity> for FillActivity {
    type Error = ExecutionError;

    fn try_from(raw: RawFillActivity) -> Result<Self, Self::Error> {
        validate_bounded_text("activity id", &raw.id, MAX_PAGE_TOKEN_BYTES)?;
        validate_bounded_text("fill order id", &raw.order_id, MAX_IDENTIFIER_BYTES)?;
        if raw.activity_type != "FILL" || !matches!(raw.fill_type.as_str(), "fill" | "partial_fill")
        {
            return Err(ExecutionError::Broker(
                "FILL endpoint returned an unexpected activity type".into(),
            ));
        }
        let quantity = parse_whole("fill qty", &raw.qty)?;
        let cumulative_quantity = parse_whole("fill cum_qty", &raw.cum_qty)?;
        let leaves_quantity = parse_whole("fill leaves_qty", &raw.leaves_qty)?;
        if quantity == WholeQuantity::ZERO || cumulative_quantity < quantity {
            return Err(ExecutionError::Broker(
                "fill quantities are not positive and cumulative".into(),
            ));
        }
        Ok(Self {
            activity_id: raw.id,
            activity_type: raw.activity_type,
            fill_type: raw.fill_type,
            provider_order_id: raw.order_id,
            symbol: parse_symbol(raw.symbol)?,
            side: parse_side(&raw.side)?,
            quantity,
            cumulative_quantity,
            leaves_quantity,
            price: parse_positive_price("fill price", &raw.price)?,
            transaction_at: parse_timestamp("transaction_time", &raw.transaction_time)?,
        })
    }
}

#[derive(Serialize)]
struct CreateOrderRequest<'a> {
    symbol: &'a str,
    #[serde(rename = "qty")]
    quantity: String,
    side: &'static str,
    #[serde(rename = "type")]
    order_type: &'static str,
    time_in_force: &'static str,
    limit_price: String,
    extended_hours: bool,
    client_order_id: &'a str,
    order_class: &'static str,
}

#[derive(Serialize)]
struct FillFingerprintMaterial<'a> {
    schema: &'static str,
    activity_id: &'a str,
    activity_type: &'a str,
    fill_type: &'a str,
    provider_order_id: &'a str,
    symbol: &'a Symbol,
    side: OrderSide,
    quantity: WholeQuantity,
    cumulative_quantity: WholeQuantity,
    leaves_quantity: WholeQuantity,
    price: Price,
    transaction_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct SourcePageHashMaterial<'a> {
    schema: &'static str,
    snapshot_round: u8,
    kind: SourcePageKind,
    page_ordinal: u32,
    request_parameters_hash: HashDigest,
    request_id: &'a Option<String>,
    raw_payload_hash: HashDigest,
    received_at: DateTime<Utc>,
    item_count: u32,
    completion_witness: Option<PageCompletionWitness>,
}

fn single_source_page(
    snapshot_round: u8,
    kind: SourcePageKind,
    request_schema: &'static str,
    response: &ResponseEvidence,
    item_count: u32,
) -> Result<SourcePageEvidence, ExecutionError> {
    let request_parameters_hash = HashDigest::of_json(&(request_schema, "no-query-parameters"))
        .map_err(|_| {
            ExecutionError::Broker("single-page request evidence could not be hashed".into())
        })?;
    source_page(
        snapshot_round,
        kind,
        0,
        request_parameters_hash,
        response,
        item_count,
        Some(PageCompletionWitness::Single),
    )
}

fn append_paged_source_evidence(
    output: &mut Vec<SourcePageEvidence>,
    snapshot_round: u8,
    kind: SourcePageKind,
    pages: &[PageResponseEvidence],
) -> Result<(), ExecutionError> {
    for (ordinal, page) in pages.iter().enumerate() {
        let page_ordinal = u32::try_from(ordinal)
            .map_err(|_| ExecutionError::Broker("broker page ordinal exceeded its bound".into()))?;
        let completion_witness = page.completion_witness.map(|witness| match witness {
            PaginationCompletionWitness::ShortPage => PageCompletionWitness::ShortPage,
            PaginationCompletionWitness::TimestampHorizonCrossed => {
                PageCompletionWitness::TimestampHorizonCrossed
            }
        });
        output.push(source_page(
            snapshot_round,
            kind,
            page_ordinal,
            page.request_parameters_hash,
            &page.response,
            page.item_count,
            completion_witness,
        )?);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn source_page(
    snapshot_round: u8,
    kind: SourcePageKind,
    page_ordinal: u32,
    request_parameters_hash: HashDigest,
    response: &ResponseEvidence,
    item_count: u32,
    completion_witness: Option<PageCompletionWitness>,
) -> Result<SourcePageEvidence, ExecutionError> {
    let material = SourcePageHashMaterial {
        schema: "wasp2/alpaca-source-page-evidence/v1",
        snapshot_round,
        kind,
        page_ordinal,
        request_parameters_hash,
        request_id: &response.request_id,
        raw_payload_hash: response.raw_payload_hash,
        received_at: response.received_at,
        item_count,
        completion_witness,
    };
    let evidence_hash = HashDigest::of_json(&material).map_err(|_| {
        ExecutionError::Broker("broker source-page evidence could not be hashed".into())
    })?;
    Ok(SourcePageEvidence {
        snapshot_round,
        kind,
        page_ordinal,
        request_parameters_hash,
        request_id: response.request_id.clone(),
        raw_payload_hash: response.raw_payload_hash,
        received_at: response.received_at,
        item_count,
        completion_witness,
        evidence_hash,
    })
}

fn normalize_snapshot_orders(
    orders: Vec<AlpacaOrder>,
    provider_order_ids: &mut BTreeSet<String>,
    client_order_ids: &mut BTreeSet<String>,
    normalized_orders: &mut BTreeMap<String, OrderTruth>,
) -> Result<(), ExecutionError> {
    for order in orders {
        if matches!(
            BrokerOrderStatus::from_provider(&order.status),
            BrokerOrderStatus::Unknown(_)
        ) {
            return Err(ExecutionError::Broker(
                "read-only broker snapshot contained an unknown order status".into(),
            ));
        }
        if !provider_order_ids.insert(order.provider_order_id.clone())
            || !client_order_ids.insert(order.client_order_id.clone())
        {
            return Err(ExecutionError::Broker(
                "read-only broker snapshot repeated an order identity".into(),
            ));
        }
        let client_order_id = order.client_order_id.clone();
        let truth = OrderTruth {
            provider_order_id: order.provider_order_id,
            client_order_id: order.client_order_id,
            symbol: order.symbol,
            asset_class: order.asset_class,
            side: order.side,
            quantity: order.quantity,
            notional: order.notional,
            filled_quantity: order.filled_quantity,
            average_fill_price: order.average_fill_price,
            limit_price: order.limit_price,
            order_class: order.order_class,
            order_type: order.order_type,
            time_in_force: order.time_in_force,
            status: order.status,
            extended_hours: order.extended_hours,
            submitted_at: order.submitted_at,
            updated_at: order.updated_at,
        };
        if normalized_orders.insert(client_order_id, truth).is_some() {
            return Err(ExecutionError::Broker(
                "read-only broker snapshot repeated an order identity".into(),
            ));
        }
    }
    Ok(())
}

fn fill_fingerprint(fill: &FillActivity) -> Result<HashDigest, ExecutionError> {
    HashDigest::of_json(&FillFingerprintMaterial {
        schema: "wasp2/alpaca-fill-fingerprint/v1",
        activity_id: &fill.activity_id,
        activity_type: &fill.activity_type,
        fill_type: &fill.fill_type,
        provider_order_id: &fill.provider_order_id,
        symbol: &fill.symbol,
        side: fill.side,
        quantity: fill.quantity,
        cumulative_quantity: fill.cumulative_quantity,
        leaves_quantity: fill.leaves_quantity,
        price: fill.price,
        transaction_at: fill.transaction_at,
    })
    .map_err(|_| ExecutionError::Broker("fill fingerprint could not be computed".into()))
}

fn map_account_status(status: &str) -> AccountStatus {
    match status {
        "ACTIVE" => AccountStatus::Active,
        "ACCOUNT_CLOSED" | "CLOSED" | "REJECTED" => AccountStatus::Closed,
        "ACCOUNT_UPDATED" | "ACTION_REQUIRED" | "APPROVAL_PENDING" | "APPROVED" | "INACTIVE"
        | "ONBOARDING" | "RESTRICTED" | "SUBMISSION_FAILED" | "SUBMITTED" => {
            AccountStatus::Restricted
        }
        _ => AccountStatus::Unknown,
    }
}

fn read_only_configuration_error() -> CoordinatorPortError {
    CoordinatorPortError::new("read-only Alpaca broker configuration rejected")
}

fn read_only_snapshot_error() -> CoordinatorPortError {
    CoordinatorPortError::new("read-only Alpaca broker snapshot unavailable")
}

fn validate_intent(intent: &OrderIntent) -> Result<(), ExecutionError> {
    validate_bounded_text(
        "client_order_id",
        &intent.client_order_id,
        MAX_IDENTIFIER_BYTES,
    )?;
    if intent.quantity == WholeQuantity::ZERO
        || !intent.limit_price.fixed().is_positive()
        || !intent.arrival_quote.fixed().is_positive()
        || !is_valid_equity_tick(intent.limit_price)
        || matches!(
            intent.side,
            OrderSide::Buy if intent.limit_price < intent.arrival_quote
        )
        || matches!(
            intent.side,
            OrderSide::Sell if intent.limit_price > intent.arrival_quote
        )
        || intent.time_in_force != TimeInForce::Day
    {
        return Err(ExecutionError::UnsafeConfiguration(
            "Alpaca v1 accepts only positive tick-aligned whole-share DAY limit intents".into(),
        ));
    }
    Ok(())
}

fn validate_descending_order_page(
    page: &[AlpacaOrder],
    previous_page_tail: Option<DateTime<Utc>>,
) -> Result<(), ExecutionError> {
    if page
        .windows(2)
        .any(|pair| pair[0].submitted_at < pair[1].submitted_at)
        || previous_page_tail
            .zip(page.first())
            .is_some_and(|(previous_tail, current_head)| current_head.submitted_at > previous_tail)
    {
        return Err(ExecutionError::Broker(
            "orders pagination was not globally descending by submitted_at".into(),
        ));
    }
    Ok(())
}

fn validate_ascending_fill_page(
    page: &[FillActivity],
    previous_page_tail: Option<DateTime<Utc>>,
) -> Result<(), ExecutionError> {
    if page
        .windows(2)
        .any(|pair| pair[0].transaction_at > pair[1].transaction_at)
        || previous_page_tail
            .zip(page.first())
            .is_some_and(|(previous_tail, current_head)| {
                current_head.transaction_at < previous_tail
            })
    {
        return Err(ExecutionError::Broker(
            "FILL activity pagination was not globally ascending by transaction_time".into(),
        ));
    }
    Ok(())
}

fn is_valid_equity_tick(price: Price) -> bool {
    let scaled = price.scaled();
    if scaled <= 0 {
        return false;
    }
    let tick = if scaled >= Fixed::SCALE { 10_000 } else { 100 };
    scaled % tick == 0
}

fn canonical_price(price: Price) -> String {
    let scaled = price.scaled();
    let whole = scaled / Fixed::SCALE;
    let remainder = scaled % Fixed::SCALE;
    if scaled >= Fixed::SCALE {
        format!("{whole}.{:02}", remainder / 10_000)
    } else {
        format!("{whole}.{:04}", remainder / 100)
    }
}

fn order_matches_intent(order: &AlpacaOrder, intent: &OrderIntent) -> bool {
    order.client_order_id == intent.client_order_id
        && order.symbol == intent.symbol
        && order.asset_class == "us_equity"
        && order.side == intent.side
        && order.quantity == Some(intent.quantity)
        && order.notional.is_none()
        && order.limit_price == Some(intent.limit_price)
        && order.order_type == "limit"
        && order.time_in_force == "day"
        && matches!(order.order_class.as_str(), "" | "simple")
        && !order.extended_hours
}

fn parse_side(value: &str) -> Result<OrderSide, ExecutionError> {
    match value {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(ExecutionError::Broker("unknown provider order side".into())),
    }
}

fn parse_whole(field: &str, value: &WireNumber) -> Result<WholeQuantity, ExecutionError> {
    let text = value.text();
    let quantity = u64::from_str(text.trim()).map_err(|_| {
        ExecutionError::Broker(format!("{field} is not a checked whole-share quantity"))
    })?;
    Ok(WholeQuantity::new(quantity))
}

fn parse_positive_price(field: &str, value: &WireNumber) -> Result<Price, ExecutionError> {
    let price: Price = parse_number(field, value)?;
    if !price.fixed().is_positive() {
        return Err(ExecutionError::Broker(format!("{field} must be positive")));
    }
    Ok(price)
}

fn parse_number<T>(field: &str, value: &WireNumber) -> Result<T, ExecutionError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .text()
        .parse::<T>()
        .map_err(|_| ExecutionError::Broker(format!("{field} is not checked fixed-point")))
}

fn parse_timestamp(field: &str, value: &str) -> Result<DateTime<Utc>, ExecutionError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| ExecutionError::Broker(format!("invalid {field}")))
}

fn parse_date(field: &str, value: &str) -> Result<NaiveDate, ExecutionError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| ExecutionError::Broker(format!("invalid {field}")))
}

fn parse_optional_interval(
    field: &str,
    start: Option<String>,
    end: Option<String>,
) -> Result<OptionalTimeInterval, ExecutionError> {
    match (start, end) {
        (None, None) => Ok((None, None)),
        (Some(start), Some(end)) => {
            let start = parse_timestamp(field, &start)?;
            let end = parse_timestamp(field, &end)?;
            if start >= end {
                return Err(ExecutionError::Broker(format!(
                    "{field} is empty or reversed"
                )));
            }
            Ok((Some(start), Some(end)))
        }
        _ => Err(ExecutionError::Broker(format!(
            "{field} has only one interval boundary"
        ))),
    }
}

fn parse_symbol(value: String) -> Result<Symbol, ExecutionError> {
    Symbol::new(value).map_err(|_| ExecutionError::Broker("provider symbol is invalid".into()))
}

fn validate_us_equity_market(market: &AlpacaMarketIdentity) -> Result<(), ExecutionError> {
    if market.acronym != US_EQUITY_MARKET || market.timezone != US_EQUITY_TIMEZONE {
        return Err(ExecutionError::Broker(
            "market response identity is not the pinned NYSE calendar".into(),
        ));
    }
    Ok(())
}

fn parse_json<T: for<'de> Deserialize<'de>>(
    subject: &str,
    body: &[u8],
) -> Result<T, ExecutionError> {
    serde_json::from_slice(body)
        .map_err(|_| ExecutionError::Broker(format!("invalid {subject} JSON")))
}

fn evidence(response: &HttpResponse) -> Result<ResponseEvidence, ExecutionError> {
    Ok(ResponseEvidence {
        request_id: validated_request_id(response)?,
        raw_payload_hash: HashDigest::sha256(&response.body),
        received_at: response.received_at,
    })
}

fn mutation_evidence(response: &HttpResponse) -> Result<ResponseEvidence, ExecutionError> {
    let evidence = evidence(response)?;
    if evidence.request_id.is_none() {
        return Err(ExecutionError::Broker(
            "successful mutation response omitted X-Request-ID".into(),
        ));
    }
    Ok(evidence)
}

fn validated_request_id(response: &HttpResponse) -> Result<Option<String>, ExecutionError> {
    let mut values = response
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-request-id"))
        .map(|(_, value)| value);
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some()
        || value.is_empty()
        || value.len() > MAX_REQUEST_ID_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(ExecutionError::Broker(
            "response X-Request-ID is missing, duplicate, or malformed".into(),
        ));
    }
    Ok(Some(value.clone()))
}

fn require_status(
    operation: &str,
    response: &HttpResponse,
    expected: u16,
) -> Result<(), ExecutionError> {
    if response.status == expected {
        Ok(())
    } else {
        Err(ExecutionError::Broker(status_detail(operation, response)))
    }
}

fn status_detail(operation: &str, response: &HttpResponse) -> String {
    let request_id = validated_request_id(response).ok().flatten();
    let payload_hash = HashDigest::sha256(&response.body);
    let provider_message = serde_json::from_slice::<serde_json::Value>(&response.body)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "no provider message".into());
    let provider_message = bounded_message(&provider_message);
    format!(
        "{operation} returned HTTP {}; request_id={:?} payload_hash={} message={provider_message}",
        response.status, request_id, payload_hash
    )
}

fn bounded_message(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() || character == ' ' {
                character
            } else {
                '?'
            }
        })
        .take(MAX_ERROR_MESSAGE_BYTES)
        .collect()
}

fn postgres_microsecond_timestamp(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_nanosecond((value.nanosecond() / 1_000) * 1_000)
        .expect("a truncated nanosecond value is always valid")
}

fn validate_bounded_text(field: &str, value: &str, max_bytes: usize) -> Result<(), ExecutionError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(ExecutionError::UnsafeConfiguration(format!(
            "{field} is empty, oversized, or contains control characters"
        )));
    }
    Ok(())
}

fn with_query(base: &str, pairs: &[(&str, String)]) -> String {
    let query = pairs
        .iter()
        .map(|(name, value)| format!("{name}={}", percent_encode_component(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{query}")
}

fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use serde_json::{json, Value};

    use super::*;

    #[derive(Clone, Default)]
    struct FakeTransport {
        state: Arc<Mutex<FakeState>>,
    }

    #[derive(Default)]
    struct FakeState {
        outcomes: VecDeque<Result<HttpResponse, TransportError>>,
        requests: Vec<HttpRequest>,
        io_attempts: u32,
    }

    impl FakeTransport {
        fn with_outcomes(outcomes: Vec<Result<HttpResponse, TransportError>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeState {
                    outcomes: outcomes.into(),
                    requests: Vec::new(),
                    io_attempts: 0,
                })),
            }
        }

        fn requests(&self) -> Vec<HttpRequest> {
            self.state.lock().unwrap().requests.clone()
        }

        fn io_attempts(&self) -> u32 {
            self.state.lock().unwrap().io_attempts
        }
    }

    #[async_trait]
    impl HttpTransport for FakeTransport {
        fn validate_broker_arrival_window(
            &self,
            broker_arrival_by: DateTime<Utc>,
            now: DateTime<Utc>,
        ) -> Result<(), TransportError> {
            if now < broker_arrival_by {
                Ok(())
            } else {
                Err(TransportError::BeforeSend {
                    detail: "fake broker-arrival window expired".into(),
                })
            }
        }

        async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            let mut state = self.state.lock().unwrap();
            state.requests.push(request);
            let outcome = state.outcomes.pop_front().expect("planned response");
            if !matches!(outcome, Err(TransportError::BeforeSend { .. })) {
                state.io_attempts += 1;
            }
            outcome
        }
    }

    fn adapter(transport: FakeTransport) -> AlpacaPaperAdapter<FakeTransport> {
        AlpacaPaperAdapter::new(
            Environment::Paper,
            PAPER_TRADING_API,
            MARKET_DATA_API,
            transport,
        )
        .unwrap()
    }

    fn response(status: u16, body: Value) -> HttpResponse {
        HttpResponse {
            status,
            headers: BTreeMap::from([("x-ReQuEsT-iD".into(), "request-123".into())]),
            body: serde_json::to_vec(&body).unwrap(),
            received_at: "2026-07-20T13:30:01Z".parse().unwrap(),
        }
    }

    fn empty_response(status: u16) -> HttpResponse {
        HttpResponse {
            status,
            headers: BTreeMap::from([("X-Request-ID".into(), "request-empty".into())]),
            body: Vec::new(),
            received_at: "2026-07-20T13:30:01Z".parse().unwrap(),
        }
    }

    fn response_without_request_id(status: u16, body: Value) -> HttpResponse {
        HttpResponse {
            status,
            headers: BTreeMap::new(),
            body: serde_json::to_vec(&body).unwrap(),
            received_at: "2026-07-20T13:30:01Z".parse().unwrap(),
        }
    }

    fn market_json() -> Value {
        json!({
            "acronym": "NYSE",
            "name": "New York Stock Exchange",
            "timezone": "America/New_York",
            "mic": "XNYS"
        })
    }

    fn clock_json(phase: &str) -> Value {
        json!({
            "clocks": [{
                "market": market_json(),
                "timestamp": "2026-07-20T13:30:00Z",
                "is_market_day": true,
                "next_market_open": "2026-07-21T13:30:00Z",
                "next_market_close": "2026-07-20T20:00:00Z",
                "phase": phase,
                "phase_until": "2026-07-20T20:00:00Z"
            }]
        })
    }

    fn calendar_json(core_end: &str) -> Value {
        json!({
            "market": market_json(),
            "calendar": [{
                "date": "2026-07-20",
                "core_start": "2026-07-20T13:30:00Z",
                "core_end": core_end,
                "pre_start": "2026-07-20T08:00:00Z",
                "pre_end": "2026-07-20T13:30:00Z",
                "post_start": core_end,
                "post_end": "2026-07-21T00:00:00Z",
                "settlement_date": "2026-07-21"
            }]
        })
    }

    fn order_json(status: &str) -> Value {
        json!({
            "id": "order-1",
            "client_order_id": "client-1",
            "symbol": "SPY",
            "asset_class": "us_equity",
            "side": "buy",
            "qty": "2",
            "notional": null,
            "filled_qty": "0",
            "filled_avg_price": null,
            "limit_price": "500.250000",
            "order_class": "simple",
            "order_type": "limit",
            "type": "limit",
            "time_in_force": "day",
            "status": status,
            "extended_hours": false,
            "submitted_at": "2026-07-20T13:30:00Z",
            "updated_at": "2026-07-20T13:30:00.5Z"
        })
    }

    fn order_json_with_id_and_time(id: &str, submitted_at: &str) -> Value {
        let mut order = order_json("new");
        order["id"] = json!(id);
        order["client_order_id"] = json!(format!("client-{id}"));
        order["submitted_at"] = json!(submitted_at);
        order["updated_at"] = json!(submitted_at);
        order
    }

    fn full_order_page(prefix: &str, submitted_at: &str) -> Value {
        Value::Array(
            (0..ORDER_PAGE_SIZE)
                .map(|index| {
                    order_json_with_id_and_time(&format!("{prefix}-{index:03}"), submitted_at)
                })
                .collect(),
        )
    }

    fn fill_json(id: &str, transaction_time: &str) -> Value {
        json!({
            "id": id,
            "activity_type": "FILL",
            "type": "partial_fill",
            "order_id": "order-1",
            "symbol": "SPY",
            "side": "buy",
            "qty": "1",
            "cum_qty": "1",
            "leaves_qty": "1",
            "price": "500.10",
            "transaction_time": transaction_time
        })
    }

    fn account_json(status: &str, currency: &str) -> Value {
        json!({
            "id": "account-1",
            "account_number": "PA123",
            "status": status,
            "currency": currency,
            "cash": "1000.25",
            "buying_power": "1000.25",
            "non_marginable_buying_power": "900",
            "equity": "1100.25",
            "last_equity": "1090.25",
            "portfolio_value": "1100.25",
            "long_market_value": "100",
            "short_market_value": "0",
            "accrued_fees": "0",
            "pending_transfer_in": "0",
            "pending_transfer_out": "0",
            "initial_margin": "0",
            "maintenance_margin": "0",
            "last_maintenance_margin": "0",
            "regt_buying_power": "1000.25",
            "multiplier": "1",
            "trading_blocked": false,
            "transfers_blocked": false,
            "account_blocked": false,
            "trade_suspended_by_user": false,
            "shorting_enabled": false,
            "created_at": "2026-07-01T00:00:00Z"
        })
    }

    fn position_json() -> Value {
        json!({
            "asset_id": "asset-1", "symbol": "SPY", "exchange": "ARCA",
            "asset_class": "us_equity", "avg_entry_price": "500", "qty": "2",
            "qty_available": "2", "side": "long", "market_value": "1000",
            "cost_basis": "1000", "unrealized_pl": "0", "unrealized_intraday_pl": "0",
            "current_price": "500", "lastday_price": "499"
        })
    }

    fn full_fill_page(prefix: &str, transaction_time: &str, page_size: usize) -> Value {
        Value::Array(
            (0..page_size)
                .map(|index| fill_json(&format!("{prefix}-{index:03}::fill"), transaction_time))
                .collect(),
        )
    }

    fn intent() -> OrderIntent {
        OrderIntent {
            intent_id: "intent-1".into(),
            client_order_id: "client-1".into(),
            release_id: "release-1".into(),
            decision_id: "decision-1".into(),
            symbol: Symbol::new("SPY").unwrap(),
            side: OrderSide::Buy,
            quantity: WholeQuantity::new(2),
            limit_price: "500.25".parse().unwrap(),
            decision_at: "2026-07-20T13:29:55Z".parse().unwrap(),
            arrival_quote: "500.00".parse().unwrap(),
            quote_provider_at: "2026-07-20T13:29:59Z".parse().unwrap(),
            quote_received_at: "2026-07-20T13:30:00Z".parse().unwrap(),
            quote_valid_until: "2026-07-20T13:30:10Z".parse().unwrap(),
            quote_payload_hash: HashDigest::sha256("quote"),
            time_in_force: TimeInForce::Day,
            decision_evidence_hash: HashDigest::sha256("decision"),
            materialization_evidence_hash: HashDigest::sha256("materialization"),
            created_at: "2026-07-20T13:30:00Z".parse().unwrap(),
        }
    }

    fn session_permit() -> RegularTradingSessionPermit {
        RegularTradingSessionPermit::verified(
            US_EQUITY_MARKET.into(),
            NaiveDate::from_ymd_opt(2026, 7, 20).unwrap(),
            "2026-07-20T13:30:00Z".parse().unwrap(),
            "2026-07-20T20:00:00Z".parse().unwrap(),
            "2026-07-20T13:30:00Z".parse().unwrap(),
            "2026-07-20T13:30:01Z".parse().unwrap(),
            HashDigest::sha256("clock"),
            HashDigest::sha256("calendar"),
            Some("clock-request".into()),
            Some("calendar-request".into()),
        )
        .unwrap()
    }

    #[test]
    fn construction_rejects_live_shadow_and_unpinned_hosts_before_io() {
        let transport = FakeTransport::default();
        for environment in [Environment::Live, Environment::Shadow] {
            assert!(AlpacaPaperAdapter::new(
                environment,
                PAPER_TRADING_API,
                MARKET_DATA_API,
                transport.clone()
            )
            .is_err());
        }
        assert!(AlpacaPaperAdapter::new(
            Environment::Paper,
            "https://api.alpaca.markets",
            MARKET_DATA_API,
            transport.clone()
        )
        .is_err());
        assert!(AlpacaPaperAdapter::new(
            Environment::Paper,
            PAPER_TRADING_API,
            "https://data.sandbox.alpaca.markets",
            transport.clone()
        )
        .is_err());
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn clock_and_calendar_mint_hash_evidenced_core_session_permit() {
        let transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, clock_json("core"))),
            Ok(response(200, calendar_json("2026-07-20T20:00:00Z"))),
        ]);
        let permit = adapter(transport.clone())
            .regular_trading_session_permit()
            .await
            .unwrap();

        assert_eq!(
            permit.session_close(),
            "2026-07-20T20:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            permit.verified_at(),
            "2026-07-20T13:30:01Z".parse::<DateTime<Utc>>().unwrap()
        );
        let requests = transport.requests();
        assert_eq!(
            requests[0].url,
            "https://paper-api.alpaca.markets/v3/clock?markets=NYSE"
        );
        assert_eq!(
            requests[1].url,
            "https://paper-api.alpaca.markets/v3/calendar/NYSE?start=2026-07-20&end=2026-07-20&timezone=UTC"
        );
    }

    #[tokio::test]
    async fn session_permit_fails_closed_when_clock_is_not_core_or_calendar_disagrees() {
        let pre_transport =
            FakeTransport::with_outcomes(vec![Ok(response(200, clock_json("pre")))]);
        assert!(adapter(pre_transport.clone())
            .regular_trading_session_permit()
            .await
            .is_err());
        assert_eq!(pre_transport.requests().len(), 1);

        let mismatch_transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, clock_json("core"))),
            Ok(response(200, calendar_json("2026-07-20T19:00:00Z"))),
        ]);
        assert!(adapter(mismatch_transport)
            .regular_trading_session_permit()
            .await
            .is_err());
    }

    #[tokio::test]
    async fn unknown_clock_phase_fails_closed_without_guessing() {
        let transport =
            FakeTransport::with_outcomes(vec![Ok(response(200, clock_json("auction")))]);
        assert!(adapter(transport).get_market_clock().await.is_err());
    }

    #[tokio::test]
    async fn account_parses_numeric_strings_and_captures_evidence() {
        let body = json!({
            "id": "account-1",
            "account_number": "PA123",
            "status": "ACTIVE",
            "currency": "USD",
            "cash": "1000.25",
            "buying_power": "1000.25",
            "non_marginable_buying_power": "900",
            "equity": "1100.25",
            "last_equity": "1090.25",
            "portfolio_value": "1100.25",
            "long_market_value": "100",
            "short_market_value": "0",
            "accrued_fees": "0",
            "pending_transfer_in": "0",
            "pending_transfer_out": "0",
            "initial_margin": "0",
            "maintenance_margin": "0",
            "last_maintenance_margin": "0",
            "regt_buying_power": "1000.25",
            "multiplier": "1",
            "trading_blocked": false,
            "transfers_blocked": false,
            "account_blocked": false,
            "trade_suspended_by_user": false,
            "shorting_enabled": false,
            "created_at": "2026-07-01T00:00:00Z"
        });
        let expected_hash = HashDigest::sha256(serde_json::to_vec(&body).unwrap());
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, body))]);
        let observed = adapter(transport.clone()).get_account().await.unwrap();

        assert_eq!(observed.value.cash, "1000.25".parse().unwrap());
        assert_eq!(observed.value.status, "ACTIVE");
        assert!(observed.value.is_trade_eligible());
        assert!(observed.value.account_fingerprint(b"too-short").is_err());
        let salt = [7_u8; 32];
        assert_ne!(
            observed.value.account_fingerprint(&salt).unwrap(),
            HashDigest::sha256("account-1")
        );
        assert_eq!(observed.evidence.request_id.as_deref(), Some("request-123"));
        assert_eq!(observed.evidence.raw_payload_hash, expected_hash);
        let request = &transport.requests()[0];
        assert_eq!(request.method, HttpMethod::Get);
        assert_eq!(request.request_class, RequestClass::Reconciliation);
        assert_eq!(request.url, "https://paper-api.alpaca.markets/v2/account");
        assert!(!request
            .headers
            .keys()
            .any(|name| name.to_ascii_lowercase().contains("api-key")));
    }

    #[tokio::test]
    async fn read_only_broker_builds_complete_normalized_snapshot() {
        let account_body = account_json("ACTIVE", "USD");
        let positions_body = json!([position_json()]);
        let open_orders_body = json!([order_json("new")]);
        let mut closed_order = order_json("filled");
        closed_order["id"] = json!("order-2");
        closed_order["client_order_id"] = json!("client-2");
        closed_order["filled_qty"] = json!("2");
        closed_order["filled_avg_price"] = json!("500.10");
        closed_order["submitted_at"] = json!("2026-07-19T13:30:00Z");
        closed_order["updated_at"] = json!("2026-07-19T13:31:00Z");
        let closed_orders_body = json!([closed_order]);
        let fills_body = json!([fill_json("fill-1", "2026-07-19T13:31:00Z")]);
        let response_bodies = [
            account_body.clone(),
            positions_body.clone(),
            open_orders_body.clone(),
            closed_orders_body.clone(),
            fills_body.clone(),
        ];
        let expected_evidence = response_bodies
            .iter()
            .map(|body| HashDigest::sha256(serde_json::to_vec(body).unwrap()))
            .collect::<Vec<_>>();
        let transport = FakeTransport::with_outcomes(
            response_bodies
                .into_iter()
                .map(|body| Ok(response(200, body)))
                .collect(),
        );
        let mut broker = AlpacaReadOnlyBroker::new(adapter(transport.clone()), vec![9_u8; 32])
            .expect("valid secret salt should construct the narrow port");

        let observed = broker.read_snapshot_with_evidence(2).await.unwrap();
        assert_eq!(observed.pages.len(), 5);
        assert!(observed
            .pages
            .iter()
            .all(|page| page.snapshot_round == 2
                && page.request_id.as_deref() == Some("request-123")));
        assert_eq!(observed.pages[0].kind, SourcePageKind::Account);
        assert_eq!(observed.pages[0].item_count, 1);
        assert_eq!(
            observed.pages[0].completion_witness,
            Some(PageCompletionWitness::Single)
        );
        assert_eq!(observed.pages[1].kind, SourcePageKind::Positions);
        assert_eq!(observed.pages[1].item_count, 1);
        assert_eq!(observed.pages[2].kind, SourcePageKind::OpenOrders);
        assert_eq!(
            observed.pages[2].completion_witness,
            Some(PageCompletionWitness::ShortPage)
        );
        assert!(observed
            .pages
            .iter()
            .all(|page| page.evidence_hash != page.raw_payload_hash));
        let snapshot = observed.snapshot;

        assert_eq!(snapshot.account_status, AccountStatus::Active);
        assert!(snapshot.usd_currency);
        assert_eq!(snapshot.cash, "1000.25".parse().unwrap());
        assert_eq!(
            snapshot.positions,
            BTreeMap::from([(Symbol::new("SPY").unwrap(), WholeQuantity::new(2))])
        );
        assert_eq!(
            snapshot.position_asset_ids,
            BTreeMap::from([(Symbol::new("SPY").unwrap(), "asset-1".into())])
        );
        assert_eq!(snapshot.accrued_fees, Money::ZERO);
        assert_eq!(snapshot.pending_transfer_in, Money::ZERO);
        assert_eq!(snapshot.pending_transfer_out, Money::ZERO);
        assert_eq!(snapshot.orders.len(), 2);
        assert_eq!(snapshot.orders["client-1"].status, "new");
        assert_eq!(snapshot.orders["client-1"].symbol.as_str(), "SPY");
        assert_eq!(
            snapshot.orders["client-1"].quantity,
            Some(WholeQuantity::new(2))
        );
        assert_eq!(snapshot.orders["client-2"].status, "filled");
        assert_eq!(snapshot.fill_fingerprints.len(), 1);
        assert_eq!(snapshot.source_evidence_hashes, expected_evidence);
        assert_ne!(
            snapshot.account_fingerprint,
            HashDigest::sha256("account-1")
        );

        let requests = transport.requests();
        assert_eq!(requests.len(), 5);
        assert!(requests
            .iter()
            .all(|request| request.method == HttpMethod::Get));
        assert!(requests[2].url.contains("status=open"));
        assert!(requests[3].url.contains("status=closed"));
        assert!(requests[3].url.contains("after=2026-07-01T00%3A00%3A00Z"));
        assert!(requests[4].url.contains("/activities/FILL"));
        assert!(requests[4].url.contains("after=2026-07-01T00%3A00%3A00Z"));
    }

    #[tokio::test]
    async fn read_only_broker_preserves_account_restrictions_and_non_usd_state() {
        let mut account = account_json("APPROVAL_PENDING", "EUR");
        account["trading_blocked"] = json!(true);
        account["transfers_blocked"] = json!(true);
        account["account_blocked"] = json!(true);
        account["trade_suspended_by_user"] = json!(true);
        account["accrued_fees"] = json!("0.01");
        account["pending_transfer_in"] = json!("25.00");
        account["pending_transfer_out"] = json!("5.00");
        let transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, account)),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([]))),
        ]);
        let mut broker = AlpacaReadOnlyBroker::new(adapter(transport), vec![6_u8; 32]).unwrap();

        let snapshot = broker.read_snapshot().await.unwrap();

        assert_eq!(snapshot.account_status, AccountStatus::Restricted);
        assert!(snapshot.trading_blocked);
        assert!(snapshot.transfers_blocked);
        assert!(snapshot.account_blocked);
        assert!(snapshot.trade_suspended_by_user);
        assert!(!snapshot.usd_currency);
        assert_eq!(snapshot.accrued_fees, "0.01".parse().unwrap());
        assert_eq!(snapshot.pending_transfer_in, "25.00".parse().unwrap());
        assert_eq!(snapshot.pending_transfer_out, "5.00".parse().unwrap());
    }

    #[tokio::test]
    async fn read_only_broker_rejects_unknown_and_duplicate_order_identities_redacted() {
        let mut unknown = order_json("future_provider_state");
        unknown["id"] = json!("unknown-order");
        let unknown_transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, account_json("ACTIVE", "USD"))),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([unknown]))),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([]))),
        ]);
        let mut unknown_broker =
            AlpacaReadOnlyBroker::new(adapter(unknown_transport), vec![7_u8; 32]).unwrap();

        let unknown_error = unknown_broker.read_snapshot().await.unwrap_err();

        assert_eq!(
            unknown_error.to_string(),
            "read-only Alpaca broker snapshot unavailable"
        );
        assert!(!unknown_error.to_string().contains("future_provider_state"));

        let first = order_json("new");
        let mut duplicate_client = order_json("accepted");
        duplicate_client["id"] = json!("order-2");
        let duplicate_transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, account_json("ACTIVE", "USD"))),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([first, duplicate_client]))),
            Ok(response(200, json!([]))),
            Ok(response(200, json!([]))),
        ]);
        let mut duplicate_broker =
            AlpacaReadOnlyBroker::new(adapter(duplicate_transport), vec![8_u8; 32]).unwrap();

        let duplicate_error = duplicate_broker.read_snapshot().await.unwrap_err();

        assert_eq!(
            duplicate_error.to_string(),
            "read-only Alpaca broker snapshot unavailable"
        );
    }

    #[test]
    fn read_only_broker_rejects_invalid_salt_with_fixed_error() {
        let result =
            AlpacaReadOnlyBroker::new(adapter(FakeTransport::default()), b"short".to_vec());
        let error = match result {
            Ok(_) => panic!("short salt must not construct the broker port"),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "read-only Alpaca broker configuration rejected"
        );
        assert!(!error.to_string().contains("short"));
    }

    #[tokio::test]
    async fn positions_reject_fractional_or_short_provider_state() {
        let fractional = json!([{
            "asset_id": "asset-1", "symbol": "SPY", "exchange": "ARCA",
            "asset_class": "us_equity", "avg_entry_price": "500", "qty": "1.5",
            "qty_available": "1.5", "side": "long", "market_value": "750",
            "cost_basis": "750", "unrealized_pl": "0", "unrealized_intraday_pl": "0",
            "current_price": "500", "lastday_price": "499"
        }]);
        let short = json!([{
            "asset_id": "asset-1", "symbol": "SPY", "exchange": "ARCA",
            "asset_class": "us_equity", "avg_entry_price": "500", "qty": "1",
            "qty_available": "1", "side": "short", "market_value": "-500",
            "cost_basis": "500", "unrealized_pl": "0", "unrealized_intraday_pl": "0",
            "current_price": "500", "lastday_price": "499"
        }]);
        let transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, fractional)),
            Ok(response(200, short)),
        ]);
        let adapter = adapter(transport);
        assert!(adapter.get_positions().await.is_err());
        assert!(adapter.get_positions().await.is_err());
    }

    #[tokio::test]
    async fn positions_reject_duplicate_provider_assets_or_symbols() {
        let first = json!({
            "asset_id": "asset-1", "symbol": "SPY", "exchange": "ARCA",
            "asset_class": "us_equity", "avg_entry_price": "500", "qty": "1",
            "qty_available": "1", "side": "long", "market_value": "500",
            "cost_basis": "500", "unrealized_pl": "0", "unrealized_intraday_pl": "0",
            "current_price": "500", "lastday_price": "499"
        });
        let same_symbol = json!({
            "asset_id": "asset-2", "symbol": "SPY", "exchange": "ARCA",
            "asset_class": "us_equity", "avg_entry_price": "500", "qty": "1",
            "qty_available": "1", "side": "long", "market_value": "500",
            "cost_basis": "500", "unrealized_pl": "0", "unrealized_intraday_pl": "0",
            "current_price": "500", "lastday_price": "499"
        });
        let transport =
            FakeTransport::with_outcomes(vec![Ok(response(200, json!([first, same_symbol])))]);

        assert!(adapter(transport).get_positions().await.is_err());
    }

    #[tokio::test]
    async fn sip_quote_uses_exact_feed_and_fresh_quote_uses_side_of_book() {
        let body = json!({
            "symbol": "SPY",
            "quote": {
                "bp": 500.10, "ap": 500.12, "bs": 20, "as": 25,
                "t": "2026-07-20T13:30:00.123456Z"
            }
        });
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, body))]);
        let quote = adapter(transport.clone())
            .fresh_quote(
                &Symbol::new("SPY").unwrap(),
                OrderSide::Buy,
                Duration::seconds(10),
            )
            .await
            .unwrap();
        assert_eq!(quote.raw_price, "500.12".parse().unwrap());
        assert_eq!(
            quote.valid_until,
            "2026-07-20T13:30:11Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            transport.requests()[0].url,
            "https://data.alpaca.markets/v2/stocks/SPY/quotes/latest?feed=sip"
        );
        assert_eq!(transport.requests()[0].request_class, RequestClass::Routine);
    }

    #[tokio::test]
    async fn stale_latest_quote_is_rejected_before_materialization() {
        let body = json!({
            "symbol": "SPY",
            "quote": {
                "bp": "500.10", "ap": "500.12", "bs": "20", "as": "25",
                "t": "2026-07-20T13:29:40Z"
            }
        });
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, body))]);
        assert!(adapter(transport)
            .fresh_quote(
                &Symbol::new("SPY").unwrap(),
                OrderSide::Buy,
                Duration::seconds(10),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn crossed_or_empty_selected_quote_side_is_rejected() {
        let crossed = json!({
            "symbol": "SPY",
            "quote": {
                "bp": "500.13", "ap": "500.12", "bs": "20", "as": "25",
                "t": "2026-07-20T13:30:00Z"
            }
        });
        let empty_ask = json!({
            "symbol": "SPY",
            "quote": {
                "bp": "500.10", "ap": "500.12", "bs": "20", "as": "0",
                "t": "2026-07-20T13:30:00Z"
            }
        });
        let transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, crossed)),
            Ok(response(200, empty_ask)),
        ]);
        let adapter = adapter(transport);
        let symbol = Symbol::new("SPY").unwrap();
        assert!(adapter.latest_sip_quote(&symbol).await.is_err());
        assert!(adapter
            .fresh_quote(&symbol, OrderSide::Buy, Duration::seconds(10))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn unknown_order_status_is_preserved_for_lifecycle_fail_closed() {
        let transport = FakeTransport::with_outcomes(vec![Ok(response(
            200,
            json!([order_json("provider_future_state")]),
        ))]);
        let observed = adapter(transport.clone()).list_open_orders().await.unwrap();
        assert_eq!(observed.value[0].status, "provider_future_state");
        assert_eq!(observed.page_evidence.len(), 1);
        assert_eq!(
            transport.requests()[0].url,
            "https://paper-api.alpaca.markets/v2/orders?status=open&limit=500&direction=desc&nested=false"
        );
    }

    #[tokio::test]
    async fn order_reconciliation_does_not_hide_out_of_scope_asset_classes() {
        let mut crypto = order_json("new");
        crypto["asset_class"] = json!("crypto");
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, json!([crypto])))]);
        let adapter = adapter(transport.clone());

        assert!(adapter.list_open_orders().await.is_err());
        assert!(!transport.requests()[0].url.contains("asset_class="));
    }

    #[tokio::test]
    async fn order_pagination_accepts_full_equal_timestamp_page_and_proves_terminal_short_page() {
        let first = full_order_page("order", "2026-07-20T13:30:00Z");
        let transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, first)),
            Ok(response(200, json!([]))),
        ]);
        let observed = adapter(transport.clone()).list_open_orders().await.unwrap();

        assert_eq!(observed.value.len(), ORDER_PAGE_SIZE);
        assert_eq!(observed.page_evidence.len(), 2);
        assert!(observed.completeness_proven());
        assert_eq!(
            transport.requests()[1].url,
            "https://paper-api.alpaca.markets/v2/orders?status=open&limit=500&direction=desc&nested=false&before_order_id=order-499"
        );
    }

    #[tokio::test]
    async fn order_pagination_rejects_unsorted_repeated_cursor_and_cross_page_regression() {
        let unsorted = json!([
            order_json_with_id_and_time("order-a", "2026-07-20T13:30:00Z"),
            order_json_with_id_and_time("order-b", "2026-07-20T13:30:01Z")
        ]);
        assert!(adapter(FakeTransport::with_outcomes(vec![Ok(response(
            200, unsorted
        ))]))
        .list_open_orders()
        .await
        .is_err());

        let full = full_order_page("repeat", "2026-07-20T13:30:00Z");
        let repeated_cursor = json!([order_json_with_id_and_time(
            "repeat-499",
            "2026-07-20T13:30:00Z"
        )]);
        assert!(adapter(FakeTransport::with_outcomes(vec![
            Ok(response(200, full)),
            Ok(response(200, repeated_cursor)),
        ]))
        .list_open_orders()
        .await
        .is_err());

        let full = full_order_page("cross", "2026-07-20T13:30:00Z");
        let newer_next_page = json!([order_json_with_id_and_time(
            "cross-next",
            "2026-07-20T13:30:01Z"
        )]);
        assert!(adapter(FakeTransport::with_outcomes(vec![
            Ok(response(200, full)),
            Ok(response(200, newer_next_page)),
        ]))
        .list_open_orders()
        .await
        .is_err());
    }

    #[tokio::test]
    async fn closed_order_pagination_validates_exclusive_timestamp_horizon() {
        let cutoff: DateTime<Utc> = "2026-07-20T13:30:00Z".parse().unwrap();
        let at_boundary = json!([order_json_with_id_and_time(
            "order-boundary",
            "2026-07-20T13:30:00Z"
        )]);
        assert!(adapter(FakeTransport::with_outcomes(vec![Ok(response(
            200,
            at_boundary,
        ))]))
        .list_recent_closed_orders(cutoff)
        .await
        .is_err());
    }

    #[tokio::test]
    async fn fill_activity_query_is_bounded_typed_and_percent_encoded() {
        let body = json!([fill_json(
            "20260720000000000::fill-1",
            "2026-07-20T13:30:00Z"
        )]);
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, body))]);
        let query = FillActivityQuery {
            page_token: Some("prior::activity".into()),
            ..FillActivityQuery::default()
        };
        let observed = adapter(transport.clone())
            .list_fill_activities(&query)
            .await
            .unwrap();
        assert_eq!(observed.value[0].quantity, WholeQuantity::new(1));
        assert_eq!(
            transport.requests()[0].url,
            "https://paper-api.alpaca.markets/v2/account/activities/FILL?direction=asc&page_size=100&page_token=prior%3A%3Aactivity"
        );
    }

    #[tokio::test]
    async fn fill_activity_creation_time_filters_are_not_inferred_from_transaction_time() {
        let boundary = "2026-07-20T13:30:00Z".parse().unwrap();
        let body = json!([fill_json(
            "20260720000000000::fill-1",
            "2026-07-20T13:30:00Z"
        )]);
        let transport = FakeTransport::with_outcomes(vec![
            Ok(response(200, body.clone())),
            Ok(response(200, body)),
        ]);
        let adapter = adapter(transport);

        assert!(adapter
            .list_fill_activities(&FillActivityQuery {
                after: Some(boundary),
                ..FillActivityQuery::default()
            })
            .await
            .is_ok());
        assert!(adapter
            .list_fill_activities(&FillActivityQuery {
                until: Some(boundary),
                ..FillActivityQuery::default()
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn fill_activity_pagination_uses_last_id_until_a_short_page_proves_completion() {
        let first = full_fill_page("activity", "2026-07-20T13:30:00Z", 100);
        let second = json!([fill_json("activity-next::fill", "2026-07-20T13:30:01Z")]);
        let transport =
            FakeTransport::with_outcomes(vec![Ok(response(200, first)), Ok(response(200, second))]);
        let observed = adapter(transport.clone())
            .list_fill_activities(&FillActivityQuery::default())
            .await
            .unwrap();

        assert_eq!(observed.value.len(), 101);
        assert_eq!(observed.page_evidence.len(), 2);
        assert!(observed.completeness_proven());
        assert_eq!(
            transport.requests()[1].url,
            "https://paper-api.alpaca.markets/v2/account/activities/FILL?direction=asc&page_size=100&page_token=activity-099%3A%3Afill"
        );
    }

    #[tokio::test]
    async fn fill_activity_pagination_rejects_unsorted_and_repeated_cursor() {
        let unsorted = json!([
            fill_json("activity-a::fill", "2026-07-20T13:30:01Z"),
            fill_json("activity-b::fill", "2026-07-20T13:30:00Z")
        ]);
        assert!(adapter(FakeTransport::with_outcomes(vec![Ok(response(
            200, unsorted
        ))]))
        .list_fill_activities(&FillActivityQuery::default())
        .await
        .is_err());

        let first = full_fill_page("repeat", "2026-07-20T13:30:00Z", 100);
        let repeated = json!([fill_json("repeat-099::fill", "2026-07-20T13:30:00Z")]);
        assert!(adapter(FakeTransport::with_outcomes(vec![
            Ok(response(200, first)),
            Ok(response(200, repeated)),
        ]))
        .list_fill_activities(&FillActivityQuery::default())
        .await
        .is_err());

        let first = full_fill_page("cross", "2026-07-20T13:30:01Z", 100);
        let older_next_page = json!([fill_json("cross-next::fill", "2026-07-20T13:30:00Z")]);
        assert!(adapter(FakeTransport::with_outcomes(vec![
            Ok(response(200, first)),
            Ok(response(200, older_next_page)),
        ]))
        .list_fill_activities(&FillActivityQuery::default())
        .await
        .is_err());
    }

    #[tokio::test]
    async fn fill_activity_pagination_fails_closed_at_page_ceiling() {
        let outcomes = (0..MAX_FILL_ACTIVITY_PAGES)
            .map(|index| {
                Ok(response(
                    200,
                    json!([fill_json(
                        &format!("ceiling-{index:03}::fill"),
                        "2026-07-20T13:30:00Z",
                    )]),
                ))
            })
            .collect();
        let query = FillActivityQuery {
            page_size: 1,
            ..FillActivityQuery::default()
        };
        assert!(adapter(FakeTransport::with_outcomes(outcomes))
            .list_fill_activities(&query)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn submit_is_exact_whole_share_day_limit_simple_contract() {
        let transport =
            FakeTransport::with_outcomes(vec![Ok(response(200, order_json("accepted")))]);
        let outcome = adapter(transport.clone())
            .submit_order(
                &intent(),
                &session_permit(),
                "2026-07-20T13:30:10Z".parse().unwrap(),
            )
            .await
            .unwrap();
        let SubmissionOutcome::Observed(event) = outcome else {
            panic!("expected observed order");
        };
        assert_eq!(event.status, "accepted");
        assert_eq!(event.request_id.as_deref(), Some("request-123"));

        let request = &transport.requests()[0];
        assert_eq!(request.method, HttpMethod::Post);
        assert_eq!(request.request_class, RequestClass::Routine);
        assert_eq!(
            request.not_after,
            Some("2026-07-20T13:30:10Z".parse().unwrap())
        );
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["qty"], "2");
        assert_eq!(body["type"], "limit");
        assert_eq!(body["time_in_force"], "day");
        assert_eq!(body["order_class"], "simple");
        assert_eq!(body["limit_price"], "500.25");
        assert_eq!(body["extended_hours"], false);
        assert!(body.get("notional").is_none());
    }

    #[tokio::test]
    async fn submit_serializes_exact_equity_tick_precision_at_one_dollar_boundary() {
        assert_eq!(canonical_price("0.9999".parse().unwrap()), "0.9999");
        assert_eq!(canonical_price("1".parse().unwrap()), "1.00");
        assert_eq!(canonical_price("500.25".parse().unwrap()), "500.25");

        let mut broker_order = order_json("accepted");
        broker_order["limit_price"] = json!("1.000000");
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, broker_order))]);
        let mut boundary_intent = intent();
        boundary_intent.arrival_quote = "0.9999".parse().unwrap();
        boundary_intent.limit_price = "1".parse().unwrap();
        let outcome = adapter(transport.clone())
            .submit_order(
                &boundary_intent,
                &session_permit(),
                "2026-07-20T13:30:10Z".parse().unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, SubmissionOutcome::Observed(_)));
        let body: Value = serde_json::from_slice(&transport.requests()[0].body).unwrap();
        assert_eq!(body["limit_price"], "1.00");
    }

    #[tokio::test]
    async fn successful_post_without_sanitized_request_id_is_submission_unknown() {
        for headers in [
            BTreeMap::new(),
            BTreeMap::from([("X-Request-ID".into(), "unsafe\nrequest".into())]),
            BTreeMap::from([("X-Request-ID".into(), "x".repeat(MAX_REQUEST_ID_BYTES + 1))]),
            BTreeMap::from([
                ("X-Request-ID".into(), "request-one".into()),
                ("x-request-id".into(), "request-two".into()),
            ]),
        ] {
            let mut response = response_without_request_id(200, order_json("accepted"));
            response.headers = headers;
            let transport = FakeTransport::with_outcomes(vec![Ok(response)]);
            let outcome = adapter(transport)
                .submit_order(
                    &intent(),
                    &session_permit(),
                    "2026-07-20T13:30:10Z".parse().unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(outcome, SubmissionOutcome::Unknown { .. }));
        }
    }

    #[tokio::test]
    async fn submit_rejects_nonconforming_equity_tick_before_transport() {
        let transport = FakeTransport::default();
        let mut invalid = intent();
        invalid.limit_price = "500.250001".parse().unwrap();

        assert!(adapter(transport.clone())
            .submit_order(
                &invalid,
                &session_permit(),
                "2026-07-20T13:30:10Z".parse().unwrap(),
            )
            .await
            .is_err());
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn submit_rejects_non_marketable_limit_before_transport() {
        let transport = FakeTransport::default();
        let mut invalid = intent();
        invalid.limit_price = "499.99".parse().unwrap();

        assert!(adapter(transport.clone())
            .submit_order(
                &invalid,
                &session_permit(),
                "2026-07-20T13:30:10Z".parse().unwrap(),
            )
            .await
            .is_err());
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn submit_rejects_deadline_not_bound_to_quote_and_session_before_transport() {
        let transport = FakeTransport::default();
        assert!(adapter(transport.clone())
            .submit_order(
                &intent(),
                &session_permit(),
                "2026-07-20T13:30:09Z".parse().unwrap(),
            )
            .await
            .is_err());
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn post_timeout_and_connection_loss_are_unknown_and_never_retried() {
        for error in [
            TransportError::Timeout {
                detail: "deadline".into(),
            },
            TransportError::ConnectionLost {
                detail: "reset".into(),
            },
        ] {
            let transport = FakeTransport::with_outcomes(vec![Err(error)]);
            let outcome = adapter(transport.clone())
                .submit_order(
                    &intent(),
                    &session_permit(),
                    "2026-07-20T13:30:10Z".parse().unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(outcome, SubmissionOutcome::Unknown { .. }));
            assert_eq!(transport.requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn transport_detail_is_sanitized_and_bounded_before_embedding() {
        let unsafe_detail = format!("first\nsecond{}", "x".repeat(1_000));
        let transport = FakeTransport::with_outcomes(vec![Err(TransportError::Timeout {
            detail: unsafe_detail,
        })]);
        let outcome = adapter(transport)
            .submit_order(
                &intent(),
                &session_permit(),
                "2026-07-20T13:30:10Z".parse().unwrap(),
            )
            .await
            .unwrap();
        let SubmissionOutcome::Unknown { detail } = outcome else {
            panic!("expected unknown submission");
        };
        assert!(!detail.contains('\n'));
        assert!(detail.len() < 400);
    }

    #[test]
    fn transport_error_debug_is_redacted_and_display_is_bounded() {
        let secret = format!("credential-like\n{}", "x".repeat(1_000));
        let error = TransportError::ConnectionLost {
            detail: secret.clone(),
        };
        let debug = format!("{error:?}");
        assert!(!debug.contains("credential-like"));
        let display = error.to_string();
        assert!(!display.contains('\n'));
        assert!(!display.contains(&secret));
        assert!(display.len() < 400);
    }

    #[tokio::test]
    async fn client_id_404_is_observed_none_with_request_evidence() {
        let transport =
            FakeTransport::with_outcomes(vec![Ok(response(404, json!({"message": "not found"})))]);
        let observed = adapter(transport.clone())
            .get_order_by_client_order_id("client:1")
            .await
            .unwrap();
        assert!(observed.value.is_none());
        assert_eq!(observed.evidence.request_id.as_deref(), Some("request-123"));
        assert_eq!(
            transport.requests()[0].url,
            "https://paper-api.alpaca.markets/v2/orders:by_client_order_id?client_order_id=client%3A1"
        );
    }

    #[tokio::test]
    async fn read_response_may_omit_request_id() {
        let transport = FakeTransport::with_outcomes(vec![Ok(response_without_request_id(
            404,
            json!({"message": "not found"}),
        ))]);
        let observed = adapter(transport)
            .get_order_by_client_order_id("client-1")
            .await
            .unwrap();
        assert!(observed.value.is_none());
        assert_eq!(observed.evidence.request_id, None);
    }

    #[tokio::test]
    async fn provider_id_recovery_is_read_only_and_validates_both_identities() {
        let transport =
            FakeTransport::with_outcomes(vec![Ok(response(200, order_json("pending_cancel")))]);
        let paper_adapter = adapter(transport.clone());
        let event = BrokerPort::find_order_by_provider_id(&paper_adapter, "order-1", "client-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.provider_order_id.as_deref(), Some("order-1"));
        assert_eq!(event.client_order_id, "client-1");
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, HttpMethod::Get);
        assert_eq!(
            requests[0].url,
            "https://paper-api.alpaca.markets/v2/orders/order-1"
        );

        let mut wrong_client = order_json("pending_cancel");
        wrong_client["client_order_id"] = json!("other-client");
        let wrong_client_adapter = adapter(FakeTransport::with_outcomes(vec![Ok(response(
            200,
            wrong_client,
        ))]));
        assert!(BrokerPort::find_order_by_provider_id(
            &wrong_client_adapter,
            "order-1",
            "client-1"
        )
        .await
        .is_err());

        let mut wrong_provider = order_json("pending_cancel");
        wrong_provider["id"] = json!("other-order");
        let wrong_provider_adapter = adapter(FakeTransport::with_outcomes(vec![Ok(response(
            200,
            wrong_provider,
        ))]));
        assert!(wrong_provider_adapter
            .get_order_by_provider_order_id("order-1")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn broker_port_recovery_rejects_same_client_id_with_different_order_contract() {
        let mut mismatched = order_json("accepted");
        mismatched["symbol"] = json!("QQQ");
        let transport = FakeTransport::with_outcomes(vec![Ok(response(200, mismatched))]);
        let adapter = adapter(transport);

        assert!(BrokerPort::find_order_by_client_id(&adapter, &intent())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn cancel_requires_204_and_captures_request_id() {
        let transport = FakeTransport::with_outcomes(vec![Ok(empty_response(204))]);
        let outcome = adapter(transport.clone())
            .cancel_order("order-1")
            .await
            .unwrap();
        let CancellationOutcome::RequestAccepted(accepted) = outcome else {
            panic!("expected cancellation request acknowledgement")
        };
        assert_eq!(accepted.request_id, "request-empty");
        assert_eq!(accepted.provider_order_id, "order-1");
        assert_eq!(transport.requests()[0].method, HttpMethod::Delete);
        assert_eq!(transport.requests()[0].request_class, RequestClass::Cancel);
        assert_eq!(
            transport.requests()[0].url,
            "https://paper-api.alpaca.markets/v2/orders/order-1"
        );
    }

    #[tokio::test]
    async fn cancel_before_send_is_distinct_and_proves_zero_transport_io() {
        let transport = FakeTransport::with_outcomes(vec![Err(TransportError::BeforeSend {
            detail: "HTTP request budget denied dispatch".into(),
        })]);
        let outcome = adapter(transport.clone())
            .cancel_order("order-1")
            .await
            .unwrap();
        let CancellationOutcome::NotDispatched(proof) = outcome else {
            panic!("expected a provable not-dispatched outcome")
        };
        assert_eq!(proof.provider_order_id, "order-1");
        assert_eq!(proof.reason_code, "TRANSPORT_BEFORE_SEND");
        assert!(proof.detail.contains("request budget denied"));
        assert_eq!(transport.requests().len(), 1);
        assert_eq!(transport.io_attempts(), 0);
        assert_eq!(
            proof.evidence_hash,
            HashDigest::of_json(&json!({
                "provider_order_id": proof.provider_order_id,
                "observed_at": proof.observed_at,
                "reason_code": proof.reason_code,
                "detail": proof.detail,
            }))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn cancel_204_without_sanitized_request_id_is_unknown_not_terminal_canceled() {
        for headers in [
            BTreeMap::new(),
            BTreeMap::from([("X-Request-ID".into(), "unsafe request".into())]),
        ] {
            let mut response = empty_response(204);
            response.headers = headers;
            let transport = FakeTransport::with_outcomes(vec![Ok(response)]);
            let outcome = adapter(transport).cancel_order("order-1").await.unwrap();
            assert!(matches!(outcome, CancellationOutcome::Unknown { .. }));
        }
    }

    #[tokio::test]
    async fn cancel_connection_loss_is_unknown_and_never_terminal_state() {
        let transport = FakeTransport::with_outcomes(vec![Err(TransportError::ConnectionLost {
            detail: "connection reset".into(),
        })]);
        let outcome = adapter(transport.clone())
            .cancel_order("order-1")
            .await
            .unwrap();
        assert!(matches!(outcome, CancellationOutcome::Unknown { .. }));
        assert_eq!(transport.requests().len(), 1);
        assert_eq!(transport.io_attempts(), 1);
    }

    #[tokio::test]
    async fn cancel_non_204_is_unknown_and_requires_reconciliation() {
        let transport = FakeTransport::with_outcomes(vec![Ok(response(
            404,
            json!({"message": "order not found"}),
        ))]);
        let outcome = adapter(transport.clone())
            .cancel_order("order-1")
            .await
            .unwrap();
        assert!(matches!(outcome, CancellationOutcome::Unknown { .. }));
        assert_eq!(transport.requests().len(), 1);
    }

    #[tokio::test]
    async fn broker_port_account_snapshot_fails_before_transport_io() {
        let transport = FakeTransport::default();
        let adapter = adapter(transport.clone());
        assert!(BrokerPort::account_snapshot(&adapter).await.is_err());
        assert!(transport.requests().is_empty());
    }
}
