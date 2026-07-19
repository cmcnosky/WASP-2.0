//! Certificate- and hostname-verifying PostgreSQL connection boundary.
//!
//! Amazon RDS requires an RDS root certificate for verified TLS. The runtime
//! receives a region bundle and credentials from its environment-specific AWS
//! Secrets Manager secret; this module never accepts a DSN or command-line
//! secret and exposes no plaintext/accept-invalid fallback.
//!
//! Primary sources checked 2026-07-19:
//! - <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html>
//! - <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.SSL.html>

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
use trader_core::{Environment, HashDigest};

use crate::store::{DatabaseTrustAnchor, PgExecutionStore, TlsRequiredDatabaseConfig};

const DATABASE_HOST_ENV: &str = "DATABASE_HOST";
const DATABASE_PORT_ENV: &str = "DATABASE_PORT";
const DATABASE_NAME_ENV: &str = "DATABASE_NAME";
const DATABASE_USER_ENV: &str = "DATABASE_USER";
const DATABASE_PASSWORD_ENV: &str = "DATABASE_PASSWORD";
const DATABASE_REQUIRE_TLS_ENV: &str = "DATABASE_REQUIRE_TLS";
const DATABASE_CA_BUNDLE_ENV: &str = "RDS_CA_BUNDLE_PEM";
const EXPECTED_CA_BUNDLE_SHA256_ENV: &str = "EXPECTED_RDS_CA_BUNDLE_SHA256";
const EXPECTED_DATABASE_HOST_SHA256_ENV: &str = "EXPECTED_DATABASE_HOST_SHA256";
const APP_ENVIRONMENT_ENV: &str = "APP_ENVIRONMENT";
const AWS_REGION_ENV: &str = "AWS_REGION";

const POSTGRES_PORT: u16 = 5432;
const MAX_PASSWORD_BYTES: usize = 4 * 1024;
const MAX_CA_BUNDLE_BYTES: usize = 64 * 1024;
const MAX_CA_CERTIFICATES: usize = 16;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_VERIFY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DatabaseConnectionError {
    #[error("unsafe database connection configuration: {0}")]
    UnsafeConfiguration(&'static str),
    #[error("RDS trust bundle is invalid")]
    InvalidTrustBundle,
    #[error("verified PostgreSQL connection could not be established")]
    ConnectionFailed,
    #[error("PostgreSQL session identity or TLS evidence failed closed")]
    SessionVerificationFailed,
    #[error("PostgreSQL schema or runtime permission verification failed closed")]
    StoreVerificationFailed,
    #[error("PostgreSQL connection ended")]
    ConnectionEnded,
}

/// Runtime database inputs. This type intentionally has no `Debug`, `Clone`,
/// or serialization implementation because it owns the database password and
/// CA bytes. The CA is public material but is kept beside the secret solely to
/// support controlled rotation without rebuilding the image.
pub struct DatabaseRuntimeConfig {
    environment: Environment,
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

impl DatabaseRuntimeConfig {
    pub fn from_env() -> Result<Self, DatabaseConnectionError> {
        let environment = match required_env(APP_ENVIRONMENT_ENV)?.as_str() {
            "paper" => Environment::Paper,
            "live" => Environment::Live,
            _ => {
                return Err(DatabaseConnectionError::UnsafeConfiguration(
                    "database runtime supports only isolated paper or live environments",
                ))
            }
        };
        if required_env(DATABASE_REQUIRE_TLS_ENV)? != "true" {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "DATABASE_REQUIRE_TLS must be exactly true",
            ));
        }
        let port = required_env(DATABASE_PORT_ENV)?
            .parse::<u16>()
            .map_err(|_| {
                DatabaseConnectionError::UnsafeConfiguration("database port is invalid")
            })?;
        let expected_host_digest = HashDigest::from_str(&required_env(
            EXPECTED_DATABASE_HOST_SHA256_ENV,
        )?)
        .map_err(|_| {
            DatabaseConnectionError::UnsafeConfiguration("expected database host digest is invalid")
        })?;
        let expected_ca_bundle_digest =
            HashDigest::from_str(&required_env(EXPECTED_CA_BUNDLE_SHA256_ENV)?).map_err(|_| {
                DatabaseConnectionError::UnsafeConfiguration(
                    "expected RDS CA bundle digest is invalid",
                )
            })?;
        let config = Self {
            environment,
            aws_region: required_env(AWS_REGION_ENV)?,
            host: required_env(DATABASE_HOST_ENV)?,
            expected_host_digest,
            port,
            database: required_env(DATABASE_NAME_ENV)?,
            username: required_env(DATABASE_USER_ENV)?,
            password: required_env(DATABASE_PASSWORD_ENV)?,
            ca_bundle_pem: required_env(DATABASE_CA_BUNDLE_ENV)?,
            expected_ca_bundle_digest,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), DatabaseConnectionError> {
        if self.port != POSTGRES_PORT {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "database port must be the pinned PostgreSQL port",
            ));
        }
        if !matches!(self.aws_region.as_str(), "us-east-1" | "us-east-2") {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "database runtime region is outside the reviewed benchmark scope",
            ));
        }
        validate_rds_host(&self.host, &self.aws_region)?;
        if HashDigest::sha256(self.host.as_bytes()) != self.expected_host_digest {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "database host does not match approved deployment evidence",
            ));
        }
        validate_identifier(&self.database, 63, "database name is invalid")?;
        validate_identifier(&self.username, 31, "database username is invalid")?;
        let required_suffix = match self.environment {
            Environment::Paper => "_paper",
            Environment::Live => "_live",
            Environment::Shadow => {
                return Err(DatabaseConnectionError::UnsafeConfiguration(
                    "shadow cannot share a paper/live database connector",
                ))
            }
        };
        if !self.database.ends_with(required_suffix) {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "database name is not bound to the selected environment",
            ));
        }
        if !self.username.ends_with(required_suffix) {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "database username is not bound to the selected environment",
            ));
        }
        if self.password.is_empty() || self.password.len() > MAX_PASSWORD_BYTES {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "database password is missing or oversized",
            ));
        }
        if self.ca_bundle_pem.is_empty() || self.ca_bundle_pem.len() > MAX_CA_BUNDLE_BYTES {
            return Err(DatabaseConnectionError::UnsafeConfiguration(
                "RDS CA bundle is missing or oversized",
            ));
        }
        Ok(())
    }

    pub fn environment(&self) -> Environment {
        self.environment
    }

    pub async fn connect(self) -> Result<VerifiedExecutionDatabase, DatabaseConnectionError> {
        self.validate()?;
        let (roots, trust_digest) = parse_root_bundle(self.ca_bundle_pem.as_bytes())?;
        verify_trust_digest(trust_digest, self.expected_ca_bundle_digest)?;
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let tls_config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .map_err(|_| DatabaseConnectionError::InvalidTrustBundle)?
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
            .application_name("wasp2-autotrader");

        let (client, connection) = tokio::time::timeout(CONNECT_TIMEOUT, postgres.connect(tls))
            .await
            .map_err(|_| DatabaseConnectionError::ConnectionFailed)?
            .map_err(|_| DatabaseConnectionError::ConnectionFailed)?;
        let connection_task = tokio::spawn(async move {
            connection
                .await
                .map_err(|_| DatabaseConnectionError::ConnectionEnded)
        });
        if !matches!(
            tokio::time::timeout(
                SESSION_VERIFY_TIMEOUT,
                verify_session(&client, &self.database, &self.username),
            )
            .await,
            Ok(Ok(()))
        ) {
            connection_task.abort();
            return Err(DatabaseConnectionError::SessionVerificationFailed);
        }
        let store_config = TlsRequiredDatabaseConfig {
            environment: self.environment,
            server_name: self.host,
            trust_anchor: DatabaseTrustAnchor::PinnedBundleSha256(trust_digest),
        };
        let store = match PgExecutionStore::from_verified_tls_client(client, store_config).await {
            Ok(store) => store,
            Err(_) => {
                connection_task.abort();
                return Err(DatabaseConnectionError::StoreVerificationFailed);
            }
        };

        Ok(VerifiedExecutionDatabase {
            store,
            connection_task,
        })
    }
}

/// The only production database handle. Construction has already verified TLS,
/// session authority, schema, and store guards. The connection task is private
/// so callers cannot accidentally detach or ignore it.
pub struct VerifiedExecutionDatabase {
    store: PgExecutionStore,
    connection_task: JoinHandle<Result<(), DatabaseConnectionError>>,
}

impl VerifiedExecutionDatabase {
    pub fn store_mut(&mut self) -> Result<&mut PgExecutionStore, DatabaseConnectionError> {
        if self.connection_task.is_finished() {
            return Err(DatabaseConnectionError::ConnectionEnded);
        }
        Ok(&mut self.store)
    }

    pub fn connection_is_alive(&self) -> bool {
        !self.connection_task.is_finished()
    }
}

impl Drop for VerifiedExecutionDatabase {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

fn required_env(name: &'static str) -> Result<String, DatabaseConnectionError> {
    env::var(name).map_err(|_| {
        DatabaseConnectionError::UnsafeConfiguration("required runtime input is absent")
    })
}

fn validate_rds_host(host: &str, aws_region: &str) -> Result<(), DatabaseConnectionError> {
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
        return Err(DatabaseConnectionError::UnsafeConfiguration(
            "database host is not an exact commercial-region RDS DNS name",
        ));
    }
    Ok(())
}

fn validate_identifier(
    value: &str,
    max_bytes: usize,
    error: &'static str,
) -> Result<(), DatabaseConnectionError> {
    if value.len() < 3
        || value.len() > max_bytes
        || !value.starts_with(|character: char| character.is_ascii_lowercase())
        || !value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
    {
        return Err(DatabaseConnectionError::UnsafeConfiguration(error));
    }
    Ok(())
}

fn parse_root_bundle(bytes: &[u8]) -> Result<(RootCertStore, HashDigest), DatabaseConnectionError> {
    let items = <(SectionKind, Vec<u8>)>::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DatabaseConnectionError::InvalidTrustBundle)?;
    if items.is_empty() || items.len() > MAX_CA_CERTIFICATES {
        return Err(DatabaseConnectionError::InvalidTrustBundle);
    }
    let mut roots = RootCertStore::empty();
    for (kind, der) in items {
        if kind != SectionKind::Certificate {
            return Err(DatabaseConnectionError::InvalidTrustBundle);
        }
        roots
            .add(CertificateDer::from(der))
            .map_err(|_| DatabaseConnectionError::InvalidTrustBundle)?;
    }
    if roots.is_empty() {
        return Err(DatabaseConnectionError::InvalidTrustBundle);
    }
    Ok((roots, HashDigest::sha256(bytes)))
}

fn verify_trust_digest(
    actual: HashDigest,
    expected: HashDigest,
) -> Result<(), DatabaseConnectionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(DatabaseConnectionError::InvalidTrustBundle)
    }
}

async fn verify_session(
    client: &Client,
    expected_database: &str,
    expected_user: &str,
) -> Result<(), DatabaseConnectionError> {
    let row = client
        .query_one(
            r#"
SELECT
    current_database()::text AS database_name,
    current_user::text AS user_name,
    current_setting('server_version_num')::integer AS server_version_num,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_ssl
        WHERE pid = pg_catalog.pg_backend_pid() AND ssl
    ) AS tls_used,
    current_setting('transaction_read_only') = 'off' AS writable_session,
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
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let database_name: String = row
        .try_get("database_name")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let user_name: String = row
        .try_get("user_name")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let server_version_num: i32 = row
        .try_get("server_version_num")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let tls_used: bool = row
        .try_get("tls_used")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let writable_session: bool = row
        .try_get("writable_session")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let primary_server: bool = row
        .try_get("primary_server")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolsuper: bool = row
        .try_get("rolsuper")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolinherit: bool = row
        .try_get("rolinherit")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolcreaterole: bool = row
        .try_get("rolcreaterole")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolcreatedb: bool = row
        .try_get("rolcreatedb")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolcanlogin: bool = row
        .try_get("rolcanlogin")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolreplication: bool = row
        .try_get("rolreplication")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let rolbypassrls: bool = row
        .try_get("rolbypassrls")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let memberships: Vec<String> = row
        .try_get("memberships")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    let owns_no_database_objects: bool = row
        .try_get("owns_no_database_objects")
        .map_err(|_| DatabaseConnectionError::SessionVerificationFailed)?;
    if database_name != expected_database
        || user_name != expected_user
        || server_version_num / 10_000 != 17
        || !tls_used
        || !writable_session
        || !primary_server
        || rolsuper
        || !rolinherit
        || rolcreaterole
        || rolcreatedb
        || !rolcanlogin
        || rolreplication
        || rolbypassrls
        || memberships != ["alpaca_trader_runtime"]
        || !owns_no_database_objects
    {
        return Err(DatabaseConnectionError::SessionVerificationFailed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(environment: Environment) -> DatabaseRuntimeConfig {
        let host = "wasp2.abc123.us-east-1.rds.amazonaws.com";
        let ca_bundle_pem = "fixture-only";
        DatabaseRuntimeConfig {
            environment,
            aws_region: "us-east-1".into(),
            host: host.into(),
            expected_host_digest: HashDigest::sha256(host.as_bytes()),
            port: POSTGRES_PORT,
            database: match environment {
                Environment::Paper => "alpaca_autotrader_paper",
                Environment::Live => "alpaca_autotrader_live",
                Environment::Shadow => "alpaca_autotrader_shadow",
            }
            .into(),
            username: match environment {
                Environment::Paper => "trader_runtime_paper",
                Environment::Live => "trader_runtime_live",
                Environment::Shadow => "trader_runtime_shadow",
            }
            .into(),
            password: "fixture-only".into(),
            ca_bundle_pem: ca_bundle_pem.into(),
            expected_ca_bundle_digest: HashDigest::sha256(ca_bundle_pem.as_bytes()),
        }
    }

    #[test]
    fn environment_database_and_rds_host_are_mechanically_bound() {
        assert!(config(Environment::Paper).validate().is_ok());
        assert!(config(Environment::Live).validate().is_ok());
        assert!(config(Environment::Shadow).validate().is_err());

        let mut wrong_database = config(Environment::Paper);
        wrong_database.database = "alpaca_autotrader_live".into();
        assert!(wrong_database.validate().is_err());

        let mut foreign_host = config(Environment::Paper);
        foreign_host.host = "postgres.example.com".into();
        assert!(foreign_host.validate().is_err());

        let mut cross_region_host = config(Environment::Paper);
        cross_region_host.host = "wasp2.abc123.us-east-2.rds.amazonaws.com".into();
        assert!(cross_region_host.validate().is_err());

        let mut wrong_username = config(Environment::Paper);
        wrong_username.username = "trader_runtime_live".into();
        assert!(wrong_username.validate().is_err());

        let mut wrong_host_digest = config(Environment::Paper);
        wrong_host_digest.expected_host_digest = HashDigest::sha256("another-host");
        assert!(wrong_host_digest.validate().is_err());
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
        let actual = HashDigest::sha256("approved-ca");
        assert!(verify_trust_digest(actual, actual).is_ok());
        assert!(verify_trust_digest(actual, HashDigest::sha256("replacement-ca")).is_err());
    }
}
