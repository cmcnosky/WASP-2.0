//! Production HTTP transport for the paper-only Alpaca adapter.
//!
//! This module is deliberately narrower than a general HTTP client. It injects
//! credentials only after validating the complete destination URL, disables
//! redirects, proxies, and automatic retries, and bounds both request and
//! response bodies. The live trading host is not in the allowlist.

use std::{
    collections::BTreeMap,
    env,
    sync::{Arc, LazyLock, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    redirect::Policy,
    Client, Method, Request, Url,
};
use trader_core::HashDigest;

use crate::{
    alpaca::{HttpMethod, HttpRequest, HttpResponse, HttpTransport, TransportError},
    rate_limit::{RequestBudget, RequestClass},
};

const API_KEY_ID_ENV: &str = "ALPACA_API_KEY_ID";
const API_SECRET_KEY_ENV: &str = "ALPACA_API_SECRET_KEY";
const API_KEY_ID_HEADER: &str = "apca-api-key-id";
const API_SECRET_KEY_HEADER: &str = "apca-api-secret-key";

const PAPER_API_HOST: &str = "paper-api.alpaca.markets";
const MARKET_DATA_HOST: &str = "data.alpaca.markets";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const REQUEST_LIMIT_PER_MINUTE: u16 = 180;
const REQUEST_SAFETY_RESERVE: u16 = 20;
const MAX_CREDENTIAL_BYTES: usize = 512;
const MAX_URL_BYTES: usize = 8 * 1024;
const MAX_REQUEST_HEADERS: usize = 64;
const MAX_REQUEST_HEADER_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_HEADERS: usize = 128;
const MAX_RESPONSE_HEADER_BYTES: usize = 16 * 1024;
const MAX_REQUEST_ID_BYTES: usize = 256;
const MAX_CERTIFIED_ARRIVAL_GUARD: Duration = Duration::from_secs(10);
pub const MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;
pub const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Reqwest-backed transport restricted to Alpaca paper trading and market data.
///
/// This type intentionally implements neither `Debug` nor `Clone`: its header
/// values contain credentials, and there must be one synchronized request
/// budget for every transport instance.
pub struct ReqwestTransport {
    client: Client,
    credentials: Credentials,
    request_budget: Arc<Mutex<RequestBudget>>,
    arrival_guard: CertifiedBrokerArrivalGuard,
}

/// A bounded broker-arrival allowance derived from an observed p99 plus an
/// explicit safety margin.
///
/// A POST's `not_after` value is the latest acceptable broker-arrival time,
/// not the last instant at which bytes may leave this process. The concrete
/// transport subtracts this certified guard and refuses to start dispatch at
/// or after that earlier boundary. The measurement evidence is deliberately
/// opaque here; release governance owns certification and expiry.
#[derive(Clone, Eq, PartialEq)]
pub struct CertifiedBrokerArrivalGuard {
    measured_p99: Duration,
    safety_margin: Duration,
    measured_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
    evidence_hash: HashDigest,
}

impl CertifiedBrokerArrivalGuard {
    pub fn from_measurement(
        measured_p99: Duration,
        safety_margin: Duration,
        measured_at: DateTime<Utc>,
        valid_until: DateTime<Utc>,
        evidence_hash: HashDigest,
    ) -> Result<Self, TransportError> {
        let total = measured_p99
            .checked_add(safety_margin)
            .ok_or_else(|| before_send("broker-arrival guard overflowed"))?;
        if measured_p99.is_zero()
            || safety_margin.is_zero()
            || total > MAX_CERTIFIED_ARRIVAL_GUARD
            || measured_at >= valid_until
        {
            return Err(before_send(
                "broker-arrival guard measurement is invalid or unbounded",
            ));
        }
        Ok(Self {
            measured_p99,
            safety_margin,
            measured_at,
            valid_until,
            evidence_hash,
        })
    }

    fn total(&self) -> Result<chrono::Duration, TransportError> {
        chrono::Duration::from_std(
            self.measured_p99
                .checked_add(self.safety_margin)
                .ok_or_else(|| before_send("broker-arrival guard overflowed"))?,
        )
        .map_err(|_| before_send("broker-arrival guard could not be represented"))
    }

    fn dispatch_by(
        &self,
        broker_arrival_by: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, TransportError> {
        if now < self.measured_at || now >= self.valid_until {
            return Err(before_send(
                "broker-arrival guard certificate is not currently valid",
            ));
        }
        broker_arrival_by
            .checked_sub_signed(self.total()?)
            .ok_or_else(|| before_send("broker-arrival dispatch boundary overflowed"))
    }

    pub fn evidence_hash(&self) -> HashDigest {
        self.evidence_hash
    }
}

/// Every transport created in this process shares one account budget. The
/// restart-seeded window prevents a crash/restart from manufacturing capacity.
static ACCOUNT_REQUEST_BUDGET: LazyLock<Arc<Mutex<RequestBudget>>> = LazyLock::new(|| {
    Arc::new(Mutex::new(
        RequestBudget::new_after_restart(
            REQUEST_LIMIT_PER_MINUTE,
            REQUEST_SAFETY_RESERVE,
            Instant::now(),
        )
        .expect("compile-time request-budget constants must be valid"),
    ))
});

/// Sensitive header values with no `Debug` or `Clone` implementation.
struct Credentials {
    key_id: HeaderValue,
    secret_key: HeaderValue,
}

impl ReqwestTransport {
    /// Builds a production client from credentials injected into the task by
    /// ECS. Errors never include an environment value or credential.
    pub fn from_env(arrival_guard: CertifiedBrokerArrivalGuard) -> Result<Self, TransportError> {
        let credentials = Credentials::from_env()?;
        let client = build_client()?;
        Ok(Self {
            client,
            credentials,
            request_budget: Arc::clone(&ACCOUNT_REQUEST_BUDGET),
            arrival_guard,
        })
    }

    /// Converts and validates a request without performing network I/O.
    fn prepare_request(&self, request: HttpRequest) -> Result<Request, TransportError> {
        if request.body.len() > MAX_REQUEST_BODY_BYTES {
            return Err(before_send("request body exceeds configured limit"));
        }

        let url = validate_url(&request.url)?;
        let headers = self.prepare_headers(request.headers)?;
        let method = match request.method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Delete => Method::DELETE,
        };

        self.client
            .request(method, url)
            .headers(headers)
            .body(request.body)
            .build()
            .map_err(|_| before_send("HTTP request could not be built"))
    }

    fn prepare_headers(
        &self,
        input: BTreeMap<String, String>,
    ) -> Result<HeaderMap, TransportError> {
        if input.len() > MAX_REQUEST_HEADERS {
            return Err(before_send("request has too many headers"));
        }

        let total_bytes = input.iter().try_fold(0usize, |total, (name, value)| {
            total
                .checked_add(name.len())
                .and_then(|next| next.checked_add(value.len()))
        });
        if total_bytes.is_none_or(|total| total > MAX_REQUEST_HEADER_BYTES) {
            return Err(before_send("request headers exceed configured limit"));
        }

        let mut headers = HeaderMap::new();
        for (name, value) in input {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| before_send("adapter supplied an invalid header name"))?;
            let value = HeaderValue::from_str(&value)
                .map_err(|_| before_send("adapter supplied an invalid header value"))?;
            validate_protocol_header(&name, &value)?;
            headers.insert(name, value);
        }

        // HeaderValue::clone preserves the sensitive marker. The raw values are
        // never cloned as Strings and this type has no Clone implementation.
        headers.insert(
            HeaderName::from_static(API_KEY_ID_HEADER),
            self.credentials.key_id.clone(),
        );
        headers.insert(
            HeaderName::from_static(API_SECRET_KEY_HEADER),
            self.credentials.secret_key.clone(),
        );
        Ok(headers)
    }

    fn acquire_budget(&self, class: RequestClass) -> Result<(), TransportError> {
        let mut budget = lock_budget(&self.request_budget)?;
        budget
            .try_acquire(class, Instant::now())
            .map_err(|_| before_send("HTTP request budget denied dispatch"))
    }
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    fn validate_broker_arrival_window(
        &self,
        broker_arrival_by: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), TransportError> {
        validate_dispatch_deadline(
            HttpMethod::Post,
            Some(broker_arrival_by),
            &self.arrival_guard,
            now,
        )
    }

    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        // The class is explicit contract data. Never infer cancellation or
        // reconciliation priority from a URL or HTTP method.
        let request_class = request.request_class;
        let method = request.method;
        let not_after = request.not_after;
        validate_dispatch_deadline(method, not_after, &self.arrival_guard, Utc::now())?;
        let mut request = self.prepare_request(request)?;
        self.acquire_budget(request_class)?;
        // Recheck after request construction and budget acquisition, at the
        // last application-controlled point before reqwest may write bytes.
        let response_timeout =
            response_timeout(method, not_after, &self.arrival_guard, Utc::now())?;
        *request.timeout_mut() = response_timeout;

        let mut response = self
            .client
            .execute(request)
            .await
            .map_err(|error| after_dispatch(error, "HTTP request"))?;

        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES as u64)
        {
            return Err(connection_lost("response body exceeds configured limit"));
        }

        let status = response.status().as_u16();
        let headers = response_headers(response.headers())?;
        let initial_capacity = response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(MAX_RESPONSE_BODY_BYTES);
        let mut body = Vec::with_capacity(initial_capacity);

        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| after_dispatch(error, "HTTP response body"))?
        {
            if !response_chunk_fits(body.len(), chunk.len()) {
                return Err(connection_lost("response body exceeds configured limit"));
            }
            body.extend_from_slice(&chunk);
        }

        // Capture receive time only after the complete, bounded body has
        // arrived. Acknowledgement time is not substituted for this value.
        let received_at = Utc::now();
        Ok(HttpResponse {
            status,
            headers,
            body,
            received_at,
        })
    }
}

impl Credentials {
    fn from_env() -> Result<Self, TransportError> {
        let key_id = credential_from_env(API_KEY_ID_ENV)?;
        let secret_key = credential_from_env(API_SECRET_KEY_ENV)?;
        Ok(Self { key_id, secret_key })
    }

    #[cfg(test)]
    fn from_values(key_id: String, secret_key: String) -> Result<Self, TransportError> {
        Ok(Self {
            key_id: sensitive_credential(key_id, API_KEY_ID_ENV)?,
            secret_key: sensitive_credential(secret_key, API_SECRET_KEY_ENV)?,
        })
    }
}

fn build_client() -> Result<Client, TransportError> {
    Client::builder()
        .https_only(true)
        .tls_backend_rustls()
        .redirect(Policy::none())
        .referer(false)
        .no_proxy()
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .map_err(|_| before_send("HTTP client could not be initialized"))
}

fn validate_url(raw: &str) -> Result<Url, TransportError> {
    if raw.len() > MAX_URL_BYTES {
        return Err(before_send("request URL exceeds configured limit"));
    }
    let url = Url::parse(raw).map_err(|_| before_send("request URL is invalid"))?;
    if url.scheme() != "https" {
        return Err(before_send("request URL must use HTTPS"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(before_send("request URL must not contain user information"));
    }
    if url.fragment().is_some() {
        return Err(before_send("request URL must not contain a fragment"));
    }
    if url.port_or_known_default() != Some(443) {
        return Err(before_send("request URL must not use a non-default port"));
    }
    match url.host_str() {
        Some(PAPER_API_HOST | MARKET_DATA_HOST) => Ok(url),
        _ => Err(before_send("request URL host is not allowlisted")),
    }
}

fn validate_dispatch_deadline(
    method: HttpMethod,
    not_after: Option<chrono::DateTime<Utc>>,
    arrival_guard: &CertifiedBrokerArrivalGuard,
    now: chrono::DateTime<Utc>,
) -> Result<(), TransportError> {
    match (method, not_after) {
        (HttpMethod::Post, Some(deadline))
            if now < deadline && deadline - now <= chrono::Duration::seconds(15) =>
        {
            let dispatch_by = arrival_guard.dispatch_by(deadline, now)?;
            if now < dispatch_by {
                Ok(())
            } else {
                Err(before_send(
                    "certified broker-arrival dispatch boundary has expired",
                ))
            }
        }
        (HttpMethod::Post, _) => Err(before_send(
            "POST requires a current execution deadline no more than 15 seconds away",
        )),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(before_send(
            "only POST requests may carry an execution deadline",
        )),
    }
}

fn response_timeout(
    method: HttpMethod,
    not_after: Option<chrono::DateTime<Utc>>,
    arrival_guard: &CertifiedBrokerArrivalGuard,
    now: chrono::DateTime<Utc>,
) -> Result<Option<Duration>, TransportError> {
    validate_dispatch_deadline(method, not_after, arrival_guard, now)?;
    not_after
        .map(|deadline| {
            (deadline - now)
                .to_std()
                .map_err(|_| before_send("execution deadline expired before dispatch"))
        })
        .transpose()
}

fn validate_protocol_header(name: &HeaderName, value: &HeaderValue) -> Result<(), TransportError> {
    let valid = match name.as_str() {
        "accept" | "content-type" => value.as_bytes() == b"application/json",
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(before_send(
            "adapter supplied a prohibited or invalid protocol header",
        ))
    }
}

fn credential_from_env(name: &'static str) -> Result<HeaderValue, TransportError> {
    let value = env::var(name)
        .map_err(|_| before_send("required Alpaca credential is missing or invalid"))?;
    sensitive_credential(value, name)
}

fn sensitive_credential(
    value: String,
    _environment_name: &'static str,
) -> Result<HeaderValue, TransportError> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_BYTES {
        return Err(before_send(
            "required Alpaca credential is missing or invalid",
        ));
    }
    let mut value = HeaderValue::from_str(&value)
        .map_err(|_| before_send("required Alpaca credential is missing or invalid"))?;
    value.set_sensitive(true);
    Ok(value)
}

fn response_headers(headers: &HeaderMap) -> Result<BTreeMap<String, String>, TransportError> {
    if headers.len() > MAX_RESPONSE_HEADERS {
        return Err(connection_lost(
            "provider response contained too many headers",
        ));
    }
    let aggregate_bytes = headers.iter().try_fold(0usize, |total, (name, value)| {
        total
            .checked_add(name.as_str().len())
            .and_then(|next| next.checked_add(value.as_bytes().len()))
    });
    if aggregate_bytes.is_none_or(|total| total > MAX_RESPONSE_HEADER_BYTES) {
        return Err(connection_lost(
            "provider response headers exceeded the configured limit",
        ));
    }
    let mut output = BTreeMap::new();
    for name in headers.keys() {
        if is_auth_header(name.as_str()) {
            return Err(connection_lost(
                "provider response contained a prohibited authentication header",
            ));
        }
    }
    let mut request_ids = headers.get_all("x-request-id").iter();
    if let Some(value) = request_ids.next() {
        let value = value
            .to_str()
            .map_err(|_| connection_lost("provider request ID was not text"))?;
        if value.is_empty()
            || value.len() > MAX_REQUEST_ID_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(connection_lost("provider request ID was invalid"));
        }
        if request_ids.next().is_some() {
            return Err(connection_lost(
                "provider response contained multiple request IDs",
            ));
        }
        output.insert("x-request-id".into(), value.to_owned());
    }
    Ok(output)
}

fn response_chunk_fits(current_body_bytes: usize, next_chunk_bytes: usize) -> bool {
    next_chunk_bytes <= MAX_RESPONSE_BODY_BYTES.saturating_sub(current_body_bytes)
}

fn is_auth_header(name: &str) -> bool {
    name.eq_ignore_ascii_case(API_KEY_ID_HEADER) || name.eq_ignore_ascii_case(API_SECRET_KEY_HEADER)
}

fn lock_budget(
    budget: &Mutex<RequestBudget>,
) -> Result<MutexGuard<'_, RequestBudget>, TransportError> {
    budget
        .lock()
        .map_err(|_| before_send("HTTP request budget is unavailable"))
}

fn before_send(detail: &'static str) -> TransportError {
    TransportError::BeforeSend {
        detail: detail.into(),
    }
}

fn connection_lost(detail: &'static str) -> TransportError {
    TransportError::ConnectionLost {
        detail: detail.into(),
    }
}

fn after_dispatch(error: reqwest::Error, phase: &'static str) -> TransportError {
    if error.is_timeout() {
        TransportError::Timeout {
            detail: format!("{phase} timed out after dispatch may have begun"),
        }
    } else {
        TransportError::ConnectionLost {
            detail: format!("{phase} failed after dispatch may have begun"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_arrival_guard(now: DateTime<Utc>) -> CertifiedBrokerArrivalGuard {
        CertifiedBrokerArrivalGuard::from_measurement(
            Duration::from_millis(750),
            Duration::from_millis(250),
            now - chrono::Duration::hours(1),
            now + chrono::Duration::hours(1),
            HashDigest::sha256("arrival-measurement"),
        )
        .unwrap()
    }

    fn test_transport() -> ReqwestTransport {
        let now = Utc::now();
        ReqwestTransport {
            client: build_client().expect("test client should build"),
            credentials: Credentials::from_values("test-key-id".into(), "test-secret".into())
                .expect("test credentials should be valid headers"),
            request_budget: Mutex::new(
                RequestBudget::new(REQUEST_LIMIT_PER_MINUTE, REQUEST_SAFETY_RESERVE)
                    .expect("test request budget should be valid"),
            )
            .into(),
            arrival_guard: test_arrival_guard(now),
        }
    }

    fn request(url: &str) -> HttpRequest {
        HttpRequest {
            method: HttpMethod::Get,
            not_after: None,
            url: url.into(),
            headers: BTreeMap::new(),
            body: Vec::new(),
            request_class: RequestClass::Routine,
        }
    }

    #[test]
    fn live_and_foreign_hosts_are_rejected_during_request_building() {
        let transport = test_transport();
        for url in [
            "https://api.alpaca.markets/v2/account",
            "https://example.com/v2/account",
            "https://paper-api.alpaca.markets.example.com/v2/account",
        ] {
            assert!(matches!(
                transport.prepare_request(request(url)),
                Err(TransportError::BeforeSend { .. })
            ));
        }
    }

    #[test]
    fn malformed_authority_variants_fail_before_send() {
        let transport = test_transport();
        for url in [
            "http://paper-api.alpaca.markets/v2/account",
            "https://user@paper-api.alpaca.markets/v2/account",
            "https://paper-api.alpaca.markets:8443/v2/account",
            "https://paper-api.alpaca.markets/v2/account#fragment",
        ] {
            assert!(matches!(
                transport.prepare_request(request(url)),
                Err(TransportError::BeforeSend { .. })
            ));
        }
    }

    #[test]
    fn only_paper_and_data_hosts_build() {
        let transport = test_transport();
        for url in [
            "https://paper-api.alpaca.markets/v2/account",
            "https://data.alpaca.markets/v2/stocks/SPY/quotes/latest",
        ] {
            assert!(transport.prepare_request(request(url)).is_ok());
        }
    }

    #[tokio::test]
    async fn exhausted_cancel_budget_fails_before_any_network_dispatch() {
        let mut transport = test_transport();
        transport.request_budget = Arc::new(Mutex::new(
            RequestBudget::new_after_restart(
                REQUEST_LIMIT_PER_MINUTE,
                REQUEST_SAFETY_RESERVE,
                Instant::now(),
            )
            .expect("restart-seeded test budget should be valid"),
        ));
        let mut cancel = request("https://paper-api.alpaca.markets/v2/orders/order-1");
        cancel.method = HttpMethod::Delete;
        cancel.request_class = RequestClass::Cancel;

        let result = transport.send(cancel).await;

        assert!(matches!(
            result,
            Err(TransportError::BeforeSend { detail })
                if detail == "HTTP request budget denied dispatch"
        ));
        assert_eq!(
            lock_budget(&transport.request_budget).unwrap().in_window(),
            usize::from(REQUEST_LIMIT_PER_MINUTE),
            "a denied dispatch must not consume another request slot"
        );
    }

    #[test]
    fn injected_auth_headers_are_sensitive() {
        let transport = test_transport();
        let request = transport
            .prepare_request(request("https://paper-api.alpaca.markets/v2/account"))
            .expect("allowlisted request should build");

        let key_id = request
            .headers()
            .get(API_KEY_ID_HEADER)
            .expect("key ID header should be injected");
        let secret_key = request
            .headers()
            .get(API_SECRET_KEY_HEADER)
            .expect("secret key header should be injected");
        assert!(key_id.is_sensitive());
        assert!(secret_key.is_sensitive());
    }

    #[test]
    fn adapter_cannot_override_auth_and_oversized_body_is_rejected() {
        let transport = test_transport();
        let mut auth_override = request("https://paper-api.alpaca.markets/v2/account");
        auth_override
            .headers
            .insert("APCA-API-KEY-ID".into(), "caller-value".into());
        assert!(matches!(
            transport.prepare_request(auth_override),
            Err(TransportError::BeforeSend { .. })
        ));

        let mut host_override = request("https://paper-api.alpaca.markets/v2/account");
        host_override
            .headers
            .insert("Host".into(), "api.alpaca.markets".into());
        assert!(matches!(
            transport.prepare_request(host_override),
            Err(TransportError::BeforeSend { .. })
        ));

        let mut oversized = request("https://paper-api.alpaca.markets/v2/account");
        oversized.body = vec![0; MAX_REQUEST_BODY_BYTES + 1];
        assert!(matches!(
            transport.prepare_request(oversized),
            Err(TransportError::BeforeSend { .. })
        ));
    }

    #[test]
    fn post_requires_a_near_future_arrival_deadline_and_dispatches_before_guard() {
        let transport = test_transport();
        let mut post = request("https://paper-api.alpaca.markets/v2/orders");
        post.method = HttpMethod::Post;
        assert!(transport.prepare_request(post.clone()).is_ok());
        let now = Utc::now();
        let guard = test_arrival_guard(now);
        assert!(validate_dispatch_deadline(post.method, None, &guard, now).is_err());
        assert!(validate_dispatch_deadline(
            post.method,
            Some(now - chrono::Duration::nanoseconds(1)),
            &guard,
            now
        )
        .is_err());

        let remaining = response_timeout(
            post.method,
            Some(now + chrono::Duration::seconds(2)),
            &guard,
            now,
        )
        .unwrap()
        .unwrap();
        assert_eq!(remaining, Duration::from_secs(2));
        assert!(validate_dispatch_deadline(
            post.method,
            Some(now + chrono::Duration::seconds(15)),
            &guard,
            now
        )
        .is_ok());
        assert!(validate_dispatch_deadline(
            post.method,
            Some(now + chrono::Duration::seconds(16)),
            &guard,
            now
        )
        .is_err());

        let arrival_by = now + chrono::Duration::seconds(2);
        let dispatch_by = arrival_by - chrono::Duration::seconds(1);
        assert!(validate_dispatch_deadline(
            post.method,
            Some(arrival_by),
            &guard,
            dispatch_by - chrono::Duration::nanoseconds(1),
        )
        .is_ok());
        assert!(
            validate_dispatch_deadline(post.method, Some(arrival_by), &guard, dispatch_by,)
                .is_err()
        );
    }

    #[test]
    fn arrival_guard_requires_positive_bounded_measurement_and_current_certificate() {
        let now = Utc::now();
        assert!(CertifiedBrokerArrivalGuard::from_measurement(
            Duration::ZERO,
            Duration::from_millis(1),
            now,
            now + chrono::Duration::hours(1),
            HashDigest::sha256("invalid"),
        )
        .is_err());
        assert!(CertifiedBrokerArrivalGuard::from_measurement(
            Duration::from_secs(9),
            Duration::from_secs(2),
            now,
            now + chrono::Duration::hours(1),
            HashDigest::sha256("invalid"),
        )
        .is_err());

        let expired = CertifiedBrokerArrivalGuard::from_measurement(
            Duration::from_millis(750),
            Duration::from_millis(250),
            now - chrono::Duration::hours(2),
            now - chrono::Duration::hours(1),
            HashDigest::sha256("expired"),
        )
        .unwrap();
        assert!(validate_dispatch_deadline(
            HttpMethod::Post,
            Some(now + chrono::Duration::seconds(5)),
            &expired,
            now,
        )
        .is_err());
    }

    #[test]
    fn response_body_bound_is_checked_before_extending_the_buffer() {
        assert!(response_chunk_fits(MAX_RESPONSE_BODY_BYTES - 1, 1));
        assert!(!response_chunk_fits(MAX_RESPONSE_BODY_BYTES, 1));
        assert!(!response_chunk_fits(usize::MAX, 1));
    }

    #[test]
    fn response_evidence_retains_only_one_bounded_request_id() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("request-1"));
        headers.insert("set-cookie", HeaderValue::from_static("private=value"));
        assert_eq!(
            response_headers(&headers).unwrap(),
            BTreeMap::from([("x-request-id".into(), "request-1".into())])
        );

        headers.append("x-request-id", HeaderValue::from_static("request-2"));
        assert!(response_headers(&headers).is_err());
    }
}
