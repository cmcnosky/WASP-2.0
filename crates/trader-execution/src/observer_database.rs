//! Verified PostgreSQL connection boundary for the paper read-only observer.
//!
//! This connector is deliberately separate from the execution database
//! connector. It accepts only observer-prefixed credentials, requires an exact
//! paper database/login identity, verifies hostname and certificate trust, and
//! hands callers only the [`CoordinatorStore`] contract. It cannot construct or
//! expose an execution store.

use std::{env, str::FromStr, sync::Arc, time::Duration};

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{
    pem::{PemObject, SectionKind},
    CertificateDer,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_postgres::{
    config::{ChannelBinding, SslMode},
    Client,
};
use tokio_postgres_rustls::MakeRustlsConnect;
use trader_core::HashDigest;

use crate::{
    coordinator::CoordinatorStore,
    observer_store::{ObserverStoreConfig, PgObserverStore},
};

const APP_ENVIRONMENT_ENV: &str = "APP_ENVIRONMENT";
const AWS_REGION_ENV: &str = "AWS_REGION";
const OBSERVER_DATABASE_HOST_ENV: &str = "OBSERVER_DATABASE_HOST";
const OBSERVER_DATABASE_PORT_ENV: &str = "OBSERVER_DATABASE_PORT";
const OBSERVER_DATABASE_NAME_ENV: &str = "OBSERVER_DATABASE_NAME";
const OBSERVER_DATABASE_USER_ENV: &str = "OBSERVER_DATABASE_USER";
const OBSERVER_DATABASE_PASSWORD_ENV: &str = "OBSERVER_DATABASE_PASSWORD";
const OBSERVER_DATABASE_REQUIRE_TLS_ENV: &str = "OBSERVER_DATABASE_REQUIRE_TLS";
const OBSERVER_DATABASE_CA_BUNDLE_ENV: &str = "OBSERVER_RDS_CA_BUNDLE_PEM";
const EXPECTED_OBSERVER_CA_BUNDLE_SHA256_ENV: &str = "EXPECTED_OBSERVER_RDS_CA_BUNDLE_SHA256";
const EXPECTED_OBSERVER_DATABASE_HOST_SHA256_ENV: &str = "EXPECTED_OBSERVER_DATABASE_HOST_SHA256";

const POSTGRES_PORT: u16 = 5432;
const MAX_PASSWORD_BYTES: usize = 4 * 1024;
const MAX_CA_BUNDLE_BYTES: usize = 64 * 1024;
const MAX_CA_CERTIFICATES: usize = 16;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_VERIFY_TIMEOUT: Duration = Duration::from_secs(5);
const OBSERVER_MEMBERSHIP: &str = "alpaca_trader_observer";

/// Fixed, redacted failures exposed by the observer database boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObserverDatabaseError {
    #[error("paper observer database configuration was rejected")]
    UnsafeConfiguration,
    #[error("paper observer database trust material was rejected")]
    InvalidTrustBundle,
    #[error("verified paper observer database connection could not be established")]
    ConnectionFailed,
    #[error("paper observer database session verification failed closed")]
    SessionVerificationFailed,
    #[error("paper observer database schema or privilege verification failed closed")]
    StoreVerificationFailed,
    #[error("paper observer database connection ended")]
    ConnectionEnded,
}

/// Secret-bearing connection inputs for the paper observer only.
///
/// No `Debug`, `Clone`, or serialization implementation is provided so a
/// password or trust bundle cannot be emitted accidentally.
pub struct PaperObserverDatabaseConfig {
    aws_region: String,
    host: String,
    expected_host_digest: HashDigest,
    port: u16,
    database: String,
    username: String,
    password: String,
    ca_bundle_pem: String,
    expected_ca_bundle_digest: HashDigest,
}

impl PaperObserverDatabaseConfig {
    pub fn from_env() -> Result<Self, ObserverDatabaseError> {
        if required_env(APP_ENVIRONMENT_ENV)? != "paper"
            || required_env(OBSERVER_DATABASE_REQUIRE_TLS_ENV)? != "true"
        {
            return Err(ObserverDatabaseError::UnsafeConfiguration);
        }
        let port = required_env(OBSERVER_DATABASE_PORT_ENV)?
            .parse::<u16>()
            .map_err(|_| ObserverDatabaseError::UnsafeConfiguration)?;
        let expected_host_digest =
            HashDigest::from_str(&required_env(EXPECTED_OBSERVER_DATABASE_HOST_SHA256_ENV)?)
                .map_err(|_| ObserverDatabaseError::UnsafeConfiguration)?;
        let expected_ca_bundle_digest =
            HashDigest::from_str(&required_env(EXPECTED_OBSERVER_CA_BUNDLE_SHA256_ENV)?)
                .map_err(|_| ObserverDatabaseError::UnsafeConfiguration)?;
        let config = Self {
            aws_region: required_env(AWS_REGION_ENV)?,
            host: required_env(OBSERVER_DATABASE_HOST_ENV)?,
            expected_host_digest,
            port,
            database: required_env(OBSERVER_DATABASE_NAME_ENV)?,
            username: required_env(OBSERVER_DATABASE_USER_ENV)?,
            password: required_env(OBSERVER_DATABASE_PASSWORD_ENV)?,
            ca_bundle_pem: required_env(OBSERVER_DATABASE_CA_BUNDLE_ENV)?,
            expected_ca_bundle_digest,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ObserverDatabaseError> {
        if self.port != POSTGRES_PORT
            || !matches!(self.aws_region.as_str(), "us-east-1" | "us-east-2")
        {
            return Err(ObserverDatabaseError::UnsafeConfiguration);
        }
        validate_rds_host(&self.host, &self.aws_region)?;
        if HashDigest::sha256(self.host.as_bytes()) != self.expected_host_digest {
            return Err(ObserverDatabaseError::UnsafeConfiguration);
        }
        validate_identifier(&self.database, 63)?;
        validate_identifier(&self.username, 63)?;
        if !self.database.ends_with("_paper")
            || !self.username.ends_with("_observer_paper")
            || self.username.contains("runtime")
            || self.username.contains("operator")
        {
            return Err(ObserverDatabaseError::UnsafeConfiguration);
        }
        if self.password.is_empty()
            || self.password.len() > MAX_PASSWORD_BYTES
            || self.ca_bundle_pem.is_empty()
            || self.ca_bundle_pem.len() > MAX_CA_BUNDLE_BYTES
        {
            return Err(ObserverDatabaseError::UnsafeConfiguration);
        }
        Ok(())
    }

    pub async fn connect(self) -> Result<VerifiedPaperObserverDatabase, ObserverDatabaseError> {
        self.validate()?;
        let (roots, trust_digest) = parse_root_bundle(self.ca_bundle_pem.as_bytes())?;
        if trust_digest != self.expected_ca_bundle_digest {
            return Err(ObserverDatabaseError::InvalidTrustBundle);
        }

        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let tls_config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|_| ObserverDatabaseError::InvalidTrustBundle)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = MakeRustlsConnect::new(tls_config);

        let mut postgres = tokio_postgres::Config::new();
        postgres
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.username)
            .password(&self.password)
            .ssl_mode(SslMode::Require)
            .channel_binding(ChannelBinding::Require)
            .connect_timeout(CONNECT_TIMEOUT)
            .options(
                "-c statement_timeout=5000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=5000",
            )
            .application_name("wasp2-paper-observer");

        let (client, connection) = tokio::time::timeout(CONNECT_TIMEOUT, postgres.connect(tls))
            .await
            .map_err(|_| ObserverDatabaseError::ConnectionFailed)?
            .map_err(|_| ObserverDatabaseError::ConnectionFailed)?;
        let connection_task = tokio::spawn(async move {
            connection
                .await
                .map_err(|_| ObserverDatabaseError::ConnectionEnded)
        });

        if !matches!(
            tokio::time::timeout(
                SESSION_VERIFY_TIMEOUT,
                verify_observer_session(&client, &self.database, &self.username),
            )
            .await,
            Ok(Ok(()))
        ) {
            connection_task.abort();
            return Err(ObserverDatabaseError::SessionVerificationFailed);
        }

        let store = match PgObserverStore::from_verified_client(
            client,
            ObserverStoreConfig::new(self.host, trust_digest),
        )
        .await
        {
            Ok(store) => store,
            Err(_) => {
                connection_task.abort();
                return Err(ObserverDatabaseError::StoreVerificationFailed);
            }
        };

        Ok(VerifiedPaperObserverDatabase {
            store: Some(store),
            connection_task: Some(connection_task),
        })
    }
}

/// Verified connection owner that reveals only the coordinator store port.
pub struct VerifiedPaperObserverDatabase {
    store: Option<PgObserverStore>,
    connection_task: Option<JoinHandle<Result<(), ObserverDatabaseError>>>,
}

impl VerifiedPaperObserverDatabase {
    pub fn coordinator_store_mut(
        &mut self,
    ) -> Result<&mut dyn CoordinatorStore, ObserverDatabaseError> {
        if self
            .connection_task
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
        {
            return Err(ObserverDatabaseError::ConnectionEnded);
        }
        self.store
            .as_mut()
            .map(|store| store as &mut dyn CoordinatorStore)
            .ok_or(ObserverDatabaseError::ConnectionEnded)
    }

    pub fn connection_is_alive(&self) -> bool {
        self.connection_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    /// Transfers the verified observer store and its monitored connection
    /// driver to the long-running supervisor. Both parts must remain owned and
    /// supervised together; neither is exposed outside this crate.
    pub(crate) fn into_supervised_parts(
        mut self,
    ) -> Result<
        (
            PgObserverStore,
            JoinHandle<Result<(), ObserverDatabaseError>>,
        ),
        ObserverDatabaseError,
    > {
        let store = self
            .store
            .take()
            .ok_or(ObserverDatabaseError::ConnectionEnded)?;
        let connection_task = self
            .connection_task
            .take()
            .ok_or(ObserverDatabaseError::ConnectionEnded)?;
        if connection_task.is_finished() {
            connection_task.abort();
            return Err(ObserverDatabaseError::ConnectionEnded);
        }
        Ok((store, connection_task))
    }
}

impl Drop for VerifiedPaperObserverDatabase {
    fn drop(&mut self) {
        if let Some(connection_task) = self.connection_task.as_ref() {
            connection_task.abort();
        }
    }
}

fn required_env(name: &'static str) -> Result<String, ObserverDatabaseError> {
    env::var(name).map_err(|_| ObserverDatabaseError::UnsafeConfiguration)
}

fn validate_rds_host(host: &str, aws_region: &str) -> Result<(), ObserverDatabaseError> {
    let required_suffix = format!(".{aws_region}.rds.amazonaws.com");
    if host.is_empty()
        || host.len() > 253
        || !host.ends_with(&required_suffix)
        || host.starts_with('.')
        || host.ends_with('.')
        || host.split('.').any(|label| {
            label.is_empty()
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
        })
    {
        return Err(ObserverDatabaseError::UnsafeConfiguration);
    }
    Ok(())
}

fn validate_identifier(value: &str, max_bytes: usize) -> Result<(), ObserverDatabaseError> {
    if value.len() < 3
        || value.len() > max_bytes
        || !value.starts_with(|character: char| character.is_ascii_lowercase())
        || !value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
    {
        return Err(ObserverDatabaseError::UnsafeConfiguration);
    }
    Ok(())
}

fn parse_root_bundle(bytes: &[u8]) -> Result<(RootCertStore, HashDigest), ObserverDatabaseError> {
    let items = <(SectionKind, Vec<u8>)>::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ObserverDatabaseError::InvalidTrustBundle)?;
    if items.is_empty() || items.len() > MAX_CA_CERTIFICATES {
        return Err(ObserverDatabaseError::InvalidTrustBundle);
    }
    let mut roots = RootCertStore::empty();
    for (kind, der) in items {
        if kind != SectionKind::Certificate {
            return Err(ObserverDatabaseError::InvalidTrustBundle);
        }
        roots
            .add(CertificateDer::from(der))
            .map_err(|_| ObserverDatabaseError::InvalidTrustBundle)?;
    }
    if roots.is_empty() {
        return Err(ObserverDatabaseError::InvalidTrustBundle);
    }
    Ok((roots, HashDigest::sha256(bytes)))
}

async fn verify_observer_session(
    client: &Client,
    expected_database: &str,
    expected_user: &str,
) -> Result<(), ObserverDatabaseError> {
    let row = client
        .query_one(
            r#"
SELECT
    current_database()::text AS database_name,
    current_user::text AS user_name,
    current_user = session_user AS direct_login,
    current_setting('server_version_num')::integer AS server_version_num,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_ssl
        WHERE pid = pg_catalog.pg_backend_pid() AND ssl
    ) AS tls_used,
    NOT pg_catalog.pg_is_in_recovery() AS primary_server,
    role.rolsuper,
    role.rolinherit,
    role.rolcreaterole,
    role.rolcreatedb,
    role.rolcanlogin,
    role.rolreplication,
    role.rolbypassrls,
    ARRAY(
        SELECT member.rolname::text
        FROM pg_catalog.pg_roles AS member
        WHERE member.rolname <> current_user
          AND pg_catalog.pg_has_role(current_user, member.oid, 'member')
        ORDER BY member.rolname
    )::text[] AS memberships,
    NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_database WHERE datdba = role.oid
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner = role.oid
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class WHERE relowner = role.oid
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc WHERE proowner = role.oid
    ) AS owns_no_database_objects
FROM pg_catalog.pg_roles AS role
WHERE role.rolname = current_user
"#,
            &[],
        )
        .await
        .map_err(|_| ObserverDatabaseError::SessionVerificationFailed)?;

    let database_name: String = decode(&row, "database_name")?;
    let user_name: String = decode(&row, "user_name")?;
    let direct_login: bool = decode(&row, "direct_login")?;
    let server_version_num: i32 = decode(&row, "server_version_num")?;
    let tls_used: bool = decode(&row, "tls_used")?;
    let primary_server: bool = decode(&row, "primary_server")?;
    let rolsuper: bool = decode(&row, "rolsuper")?;
    let rolinherit: bool = decode(&row, "rolinherit")?;
    let rolcreaterole: bool = decode(&row, "rolcreaterole")?;
    let rolcreatedb: bool = decode(&row, "rolcreatedb")?;
    let rolcanlogin: bool = decode(&row, "rolcanlogin")?;
    let rolreplication: bool = decode(&row, "rolreplication")?;
    let rolbypassrls: bool = decode(&row, "rolbypassrls")?;
    let memberships: Vec<String> = decode(&row, "memberships")?;
    let owns_no_database_objects: bool = decode(&row, "owns_no_database_objects")?;

    if database_name != expected_database
        || user_name != expected_user
        || !direct_login
        || server_version_num / 10_000 != 17
        || !tls_used
        || !primary_server
        || rolsuper
        || !rolinherit
        || rolcreaterole
        || rolcreatedb
        || !rolcanlogin
        || rolreplication
        || rolbypassrls
        || memberships != [OBSERVER_MEMBERSHIP]
        || !owns_no_database_objects
    {
        return Err(ObserverDatabaseError::SessionVerificationFailed);
    }
    Ok(())
}

fn decode<T>(row: &tokio_postgres::Row, column: &str) -> Result<T, ObserverDatabaseError>
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    row.try_get(column)
        .map_err(|_| ObserverDatabaseError::SessionVerificationFailed)
}

#[cfg(test)]
mod tests {
    use trader_core::HashDigest;

    use super::*;

    fn config() -> PaperObserverDatabaseConfig {
        let host = "wasp2.abc123.us-east-1.rds.amazonaws.com";
        let ca_bundle_pem = "fixture-only";
        PaperObserverDatabaseConfig {
            aws_region: "us-east-1".into(),
            host: host.into(),
            expected_host_digest: HashDigest::sha256(host.as_bytes()),
            port: POSTGRES_PORT,
            database: "alpaca_autotrader_paper".into(),
            username: "trader_observer_paper".into(),
            password: "fixture-only".into(),
            ca_bundle_pem: ca_bundle_pem.into(),
            expected_ca_bundle_digest: HashDigest::sha256(ca_bundle_pem.as_bytes()),
        }
    }

    #[test]
    fn config_is_exactly_paper_observer_and_rds_bound() {
        assert_eq!(config().validate(), Ok(()));

        let mut live_database = config();
        live_database.database = "alpaca_autotrader_live".into();
        assert_eq!(
            live_database.validate(),
            Err(ObserverDatabaseError::UnsafeConfiguration)
        );

        let mut execution_user = config();
        execution_user.username = "trader_runtime_paper".into();
        assert_eq!(
            execution_user.validate(),
            Err(ObserverDatabaseError::UnsafeConfiguration)
        );

        let mut live_user = config();
        live_user.username = "trader_observer_live".into();
        assert_eq!(
            live_user.validate(),
            Err(ObserverDatabaseError::UnsafeConfiguration)
        );

        let mut foreign_host = config();
        foreign_host.host = "postgres.example.com".into();
        assert_eq!(
            foreign_host.validate(),
            Err(ObserverDatabaseError::UnsafeConfiguration)
        );

        let mut wrong_digest = config();
        wrong_digest.expected_host_digest = HashDigest::sha256("different-host");
        assert_eq!(
            wrong_digest.validate(),
            Err(ObserverDatabaseError::UnsafeConfiguration)
        );
    }

    #[test]
    fn trust_parser_rejects_empty_non_certificate_and_private_key_material() {
        assert!(parse_root_bundle(b"").is_err());
        assert!(parse_root_bundle(b"not a certificate").is_err());
        let private_key = format!(
            "-----BEGIN {} KEY-----\nAA==\n-----END {} KEY-----\n",
            "PRIVATE", "PRIVATE"
        );
        assert!(parse_root_bundle(private_key.as_bytes()).is_err());
    }
}
