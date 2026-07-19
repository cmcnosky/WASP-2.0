use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use trader_core::{
    ActivationPermit, ActivationPermitRevocation, Environment, HashDigest, KillSeverity, KillState,
    ReconciliationReport, RiskLimitSnapshot, StrategyRelease,
};

use crate::{config::RuntimeConfig, ExecutionError};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Disabled,
    ReconcileOnly,
    Shadow,
    Enabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuthorityDecision {
    pub mode: ExecutionMode,
    pub reason_codes: Vec<String>,
    #[serde(skip)]
    binding: Option<AuthorityBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorityBinding {
    environment: Environment,
    account_fingerprint: HashDigest,
    release_id: String,
    fencing_token: u64,
    assessed_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryAuthorityBinding {
    environment: Environment,
    account_fingerprint: HashDigest,
    fencing_token: u64,
    assessed_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
}

impl AuthorityDecision {
    fn assessed(mode: ExecutionMode, reason_codes: Vec<String>) -> Self {
        Self {
            mode,
            reason_codes,
            binding: None,
        }
    }

    fn enabled(reason_codes: Vec<String>, binding: AuthorityBinding) -> Self {
        Self {
            mode: ExecutionMode::Enabled,
            reason_codes,
            binding: Some(binding),
        }
    }

    pub(crate) fn permits_submission(
        &self,
        now: DateTime<Utc>,
        environment: Environment,
        account_fingerprint: &HashDigest,
        release_id: &str,
        fencing_token: u64,
    ) -> bool {
        self.mode == ExecutionMode::Enabled
            && self.binding.as_ref().is_some_and(|binding| {
                binding.environment == environment
                    && &binding.account_fingerprint == account_fingerprint
                    && binding.release_id == release_id
                    && binding.fencing_token == fencing_token
                    && now >= binding.assessed_at
                    && now < binding.valid_until
            })
    }

    #[cfg(test)]
    pub(crate) fn test_enabled(
        environment: Environment,
        account_fingerprint: HashDigest,
        release_id: impl Into<String>,
        fencing_token: u64,
        assessed_at: DateTime<Utc>,
        valid_until: DateTime<Utc>,
    ) -> Self {
        Self::enabled(
            vec!["test_authorized".into()],
            AuthorityBinding {
                environment,
                account_fingerprint,
                release_id: release_id.into(),
                fencing_token,
                assessed_at,
                valid_until,
            },
        )
    }
}

/// Separate capability for lookup-only recovery. It can be issued while POST
/// submission is disabled and never authorizes a new order submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryAuthorityDecision {
    binding: RecoveryAuthorityBinding,
}

impl RecoveryAuthorityDecision {
    pub(crate) fn permits_recovery(
        &self,
        now: DateTime<Utc>,
        environment: Environment,
        account_fingerprint: &HashDigest,
        fencing_token: u64,
    ) -> bool {
        self.binding.environment == environment
            && &self.binding.account_fingerprint == account_fingerprint
            && self.binding.fencing_token == fencing_token
            && now >= self.binding.assessed_at
            && now < self.binding.valid_until
    }

    #[cfg(test)]
    pub(crate) fn test_authorized(
        environment: Environment,
        account_fingerprint: HashDigest,
        fencing_token: u64,
        assessed_at: DateTime<Utc>,
        valid_until: DateTime<Utc>,
    ) -> Self {
        Self {
            binding: RecoveryAuthorityBinding {
                environment,
                account_fingerprint,
                fencing_token,
                assessed_at,
                valid_until,
            },
        }
    }
}

pub struct RecoveryAuthorityContext<'a> {
    pub config: &'a RuntimeConfig,
    pub now: DateTime<Utc>,
    pub observed_account_fingerprint: &'a HashDigest,
    pub execution_fencing_token: Option<u64>,
    pub lease_valid_until: Option<DateTime<Utc>>,
}

pub fn assess_recovery_authority(
    context: &RecoveryAuthorityContext<'_>,
) -> Result<RecoveryAuthorityDecision, ExecutionError> {
    context.config.validate()?;
    if context.config.environment == Environment::Shadow
        || context.observed_account_fingerprint != &context.config.account_fingerprint
    {
        return Err(ExecutionError::AuthorityDenied(
            "lookup recovery environment/account is not authorized".into(),
        ));
    }
    let fencing_token = context
        .execution_fencing_token
        .filter(|token| *token > 0)
        .ok_or_else(|| ExecutionError::AuthorityDenied("recovery fence missing".into()))?;
    let lease_valid_until = context
        .lease_valid_until
        .filter(|until| *until > context.now)
        .ok_or_else(|| ExecutionError::AuthorityDenied("recovery lease expired".into()))?;
    Ok(RecoveryAuthorityDecision {
        binding: RecoveryAuthorityBinding {
            environment: context.config.environment,
            account_fingerprint: context.config.account_fingerprint,
            fencing_token,
            assessed_at: context.now,
            valid_until: lease_valid_until,
        },
    })
}

pub struct AuthorityContext<'a> {
    pub config: &'a RuntimeConfig,
    pub now: DateTime<Utc>,
    pub observed_account_fingerprint: &'a HashDigest,
    pub release: &'a StrategyRelease,
    pub risk_limits: &'a RiskLimitSnapshot,
    pub permit: Option<&'a ActivationPermit>,
    pub permit_revocation: Option<&'a ActivationPermitRevocation>,
    pub kill_state: &'a KillState,
    pub reconciliation: &'a ReconciliationReport,
    pub execution_fencing_token: Option<u64>,
    pub lease_valid_until: Option<DateTime<Utc>>,
}

pub fn assess_authority(
    context: &AuthorityContext<'_>,
) -> Result<AuthorityDecision, ExecutionError> {
    context.config.validate()?;
    context.release.validate()?;
    context.risk_limits.validate()?;
    context.kill_state.validate()?;
    context.reconciliation.validate()?;

    if context.now < context.release.valid_from || context.now >= context.release.expires_at {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::Disabled,
            vec!["strategy_release_not_current".into()],
        ));
    }

    if context.config.environment == Environment::Shadow {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::Shadow,
            vec!["shadow_never_submits".into()],
        ));
    }
    if matches!(
        context.kill_state.severity,
        KillSeverity::Hard | KillSeverity::Liquidation
    ) {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::Disabled,
            vec!["hard_halt_operator_clearance_required".into()],
        ));
    }
    if context.kill_state.severity == KillSeverity::Soft {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["soft_halt".into()],
        ));
    }
    if !context.reconciliation.may_resume_execution {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["reconciliation_not_clean".into()],
        ));
    }
    if context.reconciliation.generated_at > context.now
        || context
            .now
            .signed_duration_since(context.reconciliation.generated_at)
            > chrono::Duration::minutes(5)
    {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["reconciliation_stale_or_future".into()],
        ));
    }
    if context.observed_account_fingerprint != &context.config.account_fingerprint
        || context.reconciliation.account_fingerprint != context.config.account_fingerprint
    {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::Disabled,
            vec!["account_fingerprint_mismatch".into()],
        ));
    }
    let Some(fencing_token) = context.execution_fencing_token.filter(|token| *token > 0) else {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["execution_fence_missing".into()],
        ));
    };
    let Some(lease_valid_until) = context
        .lease_valid_until
        .filter(|until| *until > context.now)
    else {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["execution_lease_expired".into()],
        ));
    };
    if context.reconciliation.execution_fencing_token != fencing_token {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["reconciliation_fence_mismatch".into()],
        ));
    }
    if !context.config.submission_enabled {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["submission_disabled_by_config".into()],
        ));
    }
    let Some(permit) = context.permit else {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::ReconcileOnly,
            vec!["activation_permit_missing".into()],
        ));
    };
    if let Some(revocation) = context.permit_revocation {
        revocation.validate(permit)?;
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::Disabled,
            vec!["activation_permit_revoked".into()],
        ));
    }
    if permit.validate(context.now).is_err()
        || permit.environment != context.config.environment
        || permit.account_fingerprint != context.config.account_fingerprint
        || permit.strategy_release_id != context.release.release_id
        || permit.strategy_release_hash != context.release.release_hash()?
        || permit.risk_limits_hash != HashDigest::of_json(context.risk_limits)?
        || permit.max_gross_notional < context.risk_limits.max_gross_exposure
        || permit.max_position_notional < context.risk_limits.max_order_notional
        || permit.max_daily_loss < context.risk_limits.daily_loss_limit
        || permit.max_drawdown < context.risk_limits.hard_drawdown_limit
    {
        return Ok(AuthorityDecision::assessed(
            ExecutionMode::Disabled,
            vec!["activation_permit_invalid_or_mismatched".into()],
        ));
    }
    let reconciliation_valid_until =
        context.reconciliation.generated_at + chrono::Duration::minutes(5);
    let valid_until = context
        .release
        .expires_at
        .min(permit.expires_at)
        .min(reconciliation_valid_until)
        .min(lease_valid_until);
    Ok(AuthorityDecision::enabled(
        vec!["all_execution_gates_passed".into()],
        AuthorityBinding {
            environment: context.config.environment,
            account_fingerprint: context.config.account_fingerprint,
            release_id: context.release.release_id.clone(),
            fencing_token,
            assessed_at: context.now,
            valid_until,
        },
    ))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use trader_core::{Fixed, MomentumTrendSpec, Money, RebalanceCadence, StrategySpec, Symbol};

    use super::*;
    use crate::config::{LIVE_TRADING_API, MARKET_DATA_API};

    struct AuthorityFixture {
        now: DateTime<Utc>,
        config: RuntimeConfig,
        release: StrategyRelease,
        limits: RiskLimitSnapshot,
        permit: ActivationPermit,
        kill: KillState,
        reconciliation: ReconciliationReport,
    }

    fn fixture() -> AuthorityFixture {
        let now = Utc.with_ymd_and_hms(2026, 7, 18, 14, 0, 0).unwrap();
        let strategy = StrategySpec::MomentumTrend(MomentumTrendSpec {
            momentum_lookback_sessions: 63,
            trend_lookback_sessions: 126,
            cadence: RebalanceCadence::Weekly,
        });
        let release = StrategyRelease {
            release_id: "release-1".into(),
            code_hash: HashDigest::sha256("code"),
            parameters_hash: HashDigest::of_json(&strategy).unwrap(),
            universe: ["DIA", "IVV", "IWM", "QQQ", "SCHB", "SPY", "VOO", "VTI"]
                .into_iter()
                .map(|value| Symbol::new(value).unwrap())
                .collect(),
            data_hash: HashDigest::sha256("data"),
            cost_model_hash: HashDigest::sha256("cost"),
            statistical_certificate_hash: HashDigest::sha256("certificate"),
            strategy,
            valid_from: now - chrono::Duration::days(1),
            expires_at: now + chrono::Duration::days(30),
        };
        let limits = RiskLimitSnapshot {
            max_gross_exposure: Money::from_units(1_000).unwrap(),
            max_position_weight: Fixed::ONE,
            max_positions: 1,
            max_order_notional: Money::from_units(1_000).unwrap(),
            max_planned_loss: Money::from_units(25).unwrap(),
            daily_loss_limit: Money::from_units(25).unwrap(),
            hard_drawdown_limit: Money::from_units(100).unwrap(),
            planned_stop_distance_bps: 500,
            marketable_limit_band_bps: 10,
            new_positions_enabled: true,
        };
        let account = HashDigest::sha256("live-account");
        let config = RuntimeConfig {
            environment: Environment::Live,
            trading_api_base_url: LIVE_TRADING_API.into(),
            market_data_base_url: MARKET_DATA_API.into(),
            database_isolation_tag: "live".into(),
            credentials_secret_arn: Some("arn:aws:secretsmanager:live".into()),
            account_fingerprint: account,
            submission_enabled: true,
            request_limit_per_minute: 180,
        };
        let permit = ActivationPermit {
            permit_id: "permit-1".into(),
            environment: Environment::Live,
            account_fingerprint: account,
            strategy_release_id: release.release_id.clone(),
            strategy_release_hash: release.release_hash().unwrap(),
            max_gross_notional: Money::from_units(1_000).unwrap(),
            max_position_notional: Money::from_units(1_000).unwrap(),
            max_daily_loss: Money::from_units(25).unwrap(),
            max_drawdown: Money::from_units(100).unwrap(),
            risk_limits_hash: HashDigest::of_json(&limits).unwrap(),
            issued_at: now - chrono::Duration::hours(1),
            expires_at: now + chrono::Duration::hours(1),
            operator_subject: "operator@example".into(),
            approval_digest: HashDigest::sha256("approval"),
        };
        AuthorityFixture {
            now,
            config,
            release,
            limits,
            permit,
            kill: KillState::clear("operator@example", HashDigest::sha256("clear"), now),
            reconciliation: ReconciliationReport {
                generated_at: now,
                account_fingerprint: account,
                execution_fencing_token: 42,
                differences: Vec::new(),
                may_resume_execution: true,
            },
        }
    }

    #[test]
    fn all_gates_and_operator_permit_are_required() {
        let fixture = fixture();
        let decision = assess_authority(&AuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: None,
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(decision.mode, ExecutionMode::Enabled);
    }

    #[test]
    fn append_only_revocation_disables_a_valid_permit() {
        let fixture = fixture();
        let revocation = ActivationPermitRevocation {
            revocation_id: "revocation-1".into(),
            permit_id: fixture.permit.permit_id.clone(),
            revoked_at: fixture.now,
            operator_subject: "operator@example".into(),
            reason_code: "operator_revoked".into(),
            approval_digest: HashDigest::sha256("revoke-approval"),
        };
        let decision = assess_authority(&AuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: Some(&revocation),
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(decision.mode, ExecutionMode::Disabled);
        assert_eq!(decision.reason_codes, ["activation_permit_revoked"]);
    }

    #[test]
    fn release_validity_is_inclusive_then_exclusive() {
        let mut fixture = fixture();
        fixture.release.valid_from = fixture.now;
        fixture.permit.strategy_release_hash = fixture.release.release_hash().unwrap();
        let at_start = assess_authority(&AuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: None,
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(at_start.mode, ExecutionMode::Enabled);

        let before_start = assess_authority(&AuthorityContext {
            now: fixture.now - chrono::Duration::nanoseconds(1),
            config: &fixture.config,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: None,
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(before_start.reason_codes, ["strategy_release_not_current"]);

        fixture.release.valid_from = fixture.now - chrono::Duration::days(1);
        fixture.release.expires_at = fixture.now;
        fixture.permit.strategy_release_hash = fixture.release.release_hash().unwrap();
        let at_expiry = assess_authority(&AuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: None,
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(at_expiry.reason_codes, ["strategy_release_not_current"]);
    }

    #[test]
    fn stale_future_or_wrong_fence_reconciliation_cannot_enable() {
        let mut fixture = fixture();
        fixture.reconciliation.generated_at =
            fixture.now - chrono::Duration::minutes(5) - chrono::Duration::nanoseconds(1);
        let stale = assess_authority(&AuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: None,
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(stale.reason_codes, ["reconciliation_stale_or_future"]);

        fixture.reconciliation.generated_at = fixture.now;
        fixture.reconciliation.execution_fencing_token = 41;
        let wrong_fence = assess_authority(&AuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            release: &fixture.release,
            risk_limits: &fixture.limits,
            permit: Some(&fixture.permit),
            permit_revocation: None,
            kill_state: &fixture.kill,
            reconciliation: &fixture.reconciliation,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::minutes(1)),
        })
        .unwrap();
        assert_eq!(wrong_fence.reason_codes, ["reconciliation_fence_mismatch"]);
    }

    #[test]
    fn lookup_recovery_has_separate_fenced_authority() {
        let fixture = fixture();
        let recovery = assess_recovery_authority(&RecoveryAuthorityContext {
            config: &fixture.config,
            now: fixture.now,
            observed_account_fingerprint: &fixture.config.account_fingerprint,
            execution_fencing_token: Some(42),
            lease_valid_until: Some(fixture.now + chrono::Duration::seconds(30)),
        })
        .unwrap();
        assert!(recovery.permits_recovery(
            fixture.now,
            Environment::Live,
            &fixture.config.account_fingerprint,
            42
        ));
        assert!(!recovery.permits_recovery(
            fixture.now,
            Environment::Live,
            &fixture.config.account_fingerprint,
            43
        ));
    }
}
