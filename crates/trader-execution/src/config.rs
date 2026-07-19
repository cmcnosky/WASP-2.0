use serde::{Deserialize, Serialize};
use trader_core::{Environment, HashDigest};

use crate::ExecutionError;

pub const PAPER_TRADING_API: &str = "https://paper-api.alpaca.markets";
pub const LIVE_TRADING_API: &str = "https://api.alpaca.markets";
pub const MARKET_DATA_API: &str = "https://data.alpaca.markets";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub environment: Environment,
    pub trading_api_base_url: String,
    pub market_data_base_url: String,
    pub database_isolation_tag: String,
    pub credentials_secret_arn: Option<String>,
    pub account_fingerprint: HashDigest,
    pub submission_enabled: bool,
    pub request_limit_per_minute: u16,
}

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), ExecutionError> {
        let expected_host = match self.environment {
            Environment::Shadow | Environment::Paper => PAPER_TRADING_API,
            Environment::Live => LIVE_TRADING_API,
        };
        if self.trading_api_base_url != expected_host {
            return Err(ExecutionError::UnsafeConfiguration(format!(
                "{:?} must use its exact pinned trading host",
                self.environment
            )));
        }
        if self.market_data_base_url != MARKET_DATA_API {
            return Err(ExecutionError::UnsafeConfiguration(
                "market data host is not the pinned Alpaca endpoint".into(),
            ));
        }
        let required_tag = match self.environment {
            Environment::Shadow => "shadow",
            Environment::Paper => "paper",
            Environment::Live => "live",
        };
        if self.database_isolation_tag != required_tag {
            return Err(ExecutionError::UnsafeConfiguration(
                "database isolation tag does not exactly match environment".into(),
            ));
        }
        if self.environment == Environment::Shadow && self.submission_enabled {
            return Err(ExecutionError::UnsafeConfiguration(
                "shadow mode can never enable submission".into(),
            ));
        }
        if self.submission_enabled
            && self
                .credentials_secret_arn
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(ExecutionError::UnsafeConfiguration(
                "submission requires a secret reference, never raw credentials".into(),
            ));
        }
        // Reserve at least 20 requests/minute for cancels and reconciliation.
        if self.request_limit_per_minute == 0 || self.request_limit_per_minute > 180 {
            return Err(ExecutionError::UnsafeConfiguration(
                "request limit must be between 1 and 180 per minute".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(environment: Environment) -> RuntimeConfig {
        RuntimeConfig {
            environment,
            trading_api_base_url: match environment {
                Environment::Live => LIVE_TRADING_API,
                _ => PAPER_TRADING_API,
            }
            .into(),
            market_data_base_url: MARKET_DATA_API.into(),
            database_isolation_tag: match environment {
                Environment::Shadow => "shadow",
                Environment::Paper => "paper",
                Environment::Live => "live",
            }
            .into(),
            credentials_secret_arn: Some("arn:aws:secretsmanager:example".into()),
            account_fingerprint: HashDigest::sha256("account"),
            submission_enabled: environment != Environment::Shadow,
            request_limit_per_minute: 180,
        }
    }

    #[test]
    fn paper_cannot_select_live_host() {
        let mut paper = config(Environment::Paper);
        paper.trading_api_base_url = LIVE_TRADING_API.into();
        assert!(paper.validate().is_err());
    }

    #[test]
    fn live_cannot_select_paper_database() {
        let mut live = config(Environment::Live);
        live.database_isolation_tag = "paper".into();
        assert!(live.validate().is_err());
    }
}
