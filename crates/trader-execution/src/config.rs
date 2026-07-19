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

    /// Validate the only runtime configuration currently permitted to start
    /// the paper reconciliation coordinator.
    ///
    /// This is intentionally narrower than [`Self::validate`]: the general
    /// validator continues to describe future shadow, paper-mutation, and live
    /// configurations, while startup must remain exactly paper/read-only until
    /// those later modes earn separate authority.
    pub fn validate_paper_read_only_startup(&self) -> Result<(), ExecutionError> {
        self.validate()?;

        if self.environment != Environment::Paper {
            return Err(ExecutionError::UnsafeConfiguration(
                "startup runtime is restricted to the paper environment".into(),
            ));
        }
        if self.submission_enabled {
            return Err(ExecutionError::UnsafeConfiguration(
                "paper startup runtime must remain read-only".into(),
            ));
        }
        if self
            .credentials_secret_arn
            .as_deref()
            .is_none_or(|secret_arn| secret_arn.trim().is_empty())
        {
            return Err(ExecutionError::UnsafeConfiguration(
                "paper startup reconciliation requires a secret reference".into(),
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

    #[test]
    fn exact_paper_read_only_startup_configuration_is_accepted() {
        let mut paper = config(Environment::Paper);
        paper.submission_enabled = false;

        assert_eq!(paper.validate_paper_read_only_startup(), Ok(()));
    }

    #[test]
    fn startup_configuration_rejects_non_paper_environments() {
        for environment in [Environment::Shadow, Environment::Live] {
            let mut candidate = config(environment);
            candidate.submission_enabled = false;

            assert_eq!(
                candidate.validate_paper_read_only_startup(),
                Err(ExecutionError::UnsafeConfiguration(
                    "startup runtime is restricted to the paper environment".into()
                ))
            );
        }
    }

    #[test]
    fn startup_configuration_rejects_paper_submission_authority() {
        let paper = config(Environment::Paper);

        assert_eq!(
            paper.validate_paper_read_only_startup(),
            Err(ExecutionError::UnsafeConfiguration(
                "paper startup runtime must remain read-only".into()
            ))
        );
    }

    #[test]
    fn startup_configuration_requires_non_blank_secret_reference() {
        for secret_reference in [None, Some(String::new()), Some("   ".into())] {
            let mut paper = config(Environment::Paper);
            paper.submission_enabled = false;
            paper.credentials_secret_arn = secret_reference;

            assert_eq!(
                paper.validate_paper_read_only_startup(),
                Err(ExecutionError::UnsafeConfiguration(
                    "paper startup reconciliation requires a secret reference".into()
                ))
            );
        }
    }
}
