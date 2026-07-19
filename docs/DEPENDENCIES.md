# Third-party dependency and license policy

## Admission criteria

Every direct application, build, test, infrastructure, and CI dependency must
have a narrowly stated purpose, an official source, active maintenance, a
license compatible with private commercial use, a pinned/locked version, and a
review of meaningful transitive and supply-chain risk. Prefer standard-library
capabilities and mature narrowly scoped packages over broad frameworks.

Disallowed without a recorded exception:

- copyleft network-service licenses or terms that could require disclosure;
- abandoned, unmaintained, unverifiable, or source-unavailable packages;
- dependencies fetched from local paths, personal forks, arbitrary URLs, or
  moving branches;
- packages that send telemetry or data externally by default;
- a second broker client or a second implementation of strategy/risk decisions.

Public crates and Python packages must be locked. Terraform providers must use
version constraints and the generated lockfile. OCI base images and deployed
application images must be pinned by digest for release. GitHub Actions must be
pinned and updated deliberately. CI produces an SBOM and checks known
vulnerabilities before promotion.

## Review record

For each new direct dependency, append:

| Field | Required value |
|---|---|
| Name/version/ecosystem | Exact direct dependency |
| Purpose | Capability that cannot reasonably remain internal |
| Official source | Registry/project URL |
| License | SPDX identifier and compatibility conclusion |
| Maintenance | Latest release/activity and ownership assessment |
| Security/data access | Network, filesystem, secret, unsafe-code, or telemetry surface |
| Alternatives | Why this dependency is preferred |
| Reviewer/date | Human or accountable agent and UTC date |

Vulnerability exceptions must also record affected version, advisory, exposure,
compensating controls, owner, and an expiry no longer than 30 days.

## Initial direct-dependency review — 2026-07-18

This baseline was selected for this clean-room repository. Versions are exact in
manifests/lockfiles. “Declared license” is the upstream package declaration, not
legal advice; the release gate below requires the generated transitive SBOM and
license inventory to agree before any external deployment.

| Dependency | Purpose | Official source | Declared license | Initial assessment |
|---|---|---|---|---|
| `anyhow 1.0.104` | Application error context | crates.io | MIT OR Apache-2.0 | Narrow app-boundary use; admitted |
| `async-trait 0.1.81` | Testable async adapter traits | crates.io | MIT OR Apache-2.0 | Macro/build surface; admitted |
| `chrono 0.4.38` | UTC domain timestamps | crates.io | MIT OR Apache-2.0 | No local-session calendar authority; admitted |
| `clap 4.5.16` | Operator/local CLI parsing | crates.io | MIT OR Apache-2.0 | No secret values in arguments; admitted |
| `hex 0.4.3` | Stable digest encoding | crates.io | MIT OR Apache-2.0 | Pure conversion; admitted |
| `pyo3 0.29.0` | Python binding to the Rust core | crates.io | MIT OR Apache-2.0 | Research-only boundary; admitted |
| `reqwest 0.13.4` | Narrow paper-Alpaca HTTPS transport with Rustls | [upstream reqwest repository](https://github.com/seanmonstar/reqwest/tree/v0.13.4) | MIT OR Apache-2.0 | Current upstream release with Rust 1.85 MSRV; direct network and secret-header surface is constrained to exact HTTPS paper/data hosts, redirects/proxies/retries/content decoding are disabled, credentials are marked sensitive, and bodies are bounded; preferred over hand-written HTTP/TLS; reviewed by Codex on 2026-07-19 |
| `rustls 0.23.42` / `rustls-pki-types 1.15.0` | Verify the RDS certificate chain and parse an operator-supplied AWS RDS root bundle | [rustls repository](https://github.com/rustls/rustls), [pki-types repository](https://github.com/rustls/pki-types) | Apache-2.0 OR ISC OR MIT (component-dependent) | No plaintext/accept-invalid TLS mode is exposed; the maintained `PemObject` API replaces the archived `rustls-pemfile` crate, only certificate sections are admitted, hostname verification remains mandatory, PEM is bounded, and AWS-LC matches the selected reqwest provider; network access remains in tokio-postgres; reviewed by Codex on 2026-07-19 |
| `serde 1.0.229` | Versioned contract serialization | crates.io | MIT OR Apache-2.0 | Current crates.io release satisfying `serde_json 1.0.150`; no network, filesystem, secret, or telemetry access; untrusted input still requires validation; reviewed by Codex on 2026-07-19 |
| `serde_json 1.0.150` | Canonical JSON I/O/evidence | crates.io | MIT OR Apache-2.0 | Current maintained release compatible with `tokio-postgres 0.7.18` and Rust 1.88; canonical hashing remains internal; dependency has no network, filesystem, secret, or telemetry access; reviewed by Codex on 2026-07-19 |
| `sha2 0.10.8` | SHA-256 evidence hashes | crates.io | MIT OR Apache-2.0 | Not used as a signature/MAC; admitted |
| `thiserror 1.0.63` | Typed library errors | crates.io | MIT OR Apache-2.0 | Proc macro; admitted |
| `tokio 1.53.0` | Production async runtime, database connection driver, shutdown, and bounded timers | crates.io | MIT | No implicit task authority; every spawned task must be monitored and shutdown/reconciliation remain fail-closed; admitted |
| `tokio-postgres 0.7.18` | Narrow async PostgreSQL execution-ledger and fenced-authority client | [upstream rust-postgres repository](https://github.com/sfackler/rust-postgres/tree/tokio-postgres-v0.7.18/tokio-postgres) | MIT OR Apache-2.0 | Active upstream release on 2026-07-17; direct protocol/network access, no telemetry; the caller must establish certificate- and hostname-verified TLS before constructing the store; preferred over an ORM or query framework because the reviewed SQL and SECURITY DEFINER functions remain explicit; admitted by Codex on 2026-07-19 |
| `tokio-postgres-rustls 0.14.0` | Rustls integration and hostname-verified TLS for the RDS PostgreSQL client | [upstream repository](https://github.com/jbg/tokio-postgres-rustls/tree/v0.14.0) | MIT | Current 2026-05-21 release; direct TLS/network boundary with no telemetry, unsafe code forbidden upstream, and no default features; AWS-LC is selected explicitly; preferred over OpenSSL/native-TLS runtime dependencies; reviewed by Codex on 2026-07-19 |
| `tracing 0.1.44` | Structured telemetry | crates.io | MIT | Redaction/allowlisting required; admitted |
| `tracing-subscriber 0.3.23` | Controlled log formatting/filtering | crates.io | MIT | No dynamic remote filter; admitted |
| `uuid 1.10.0` | Stable domain and intent identifiers | crates.io | MIT OR Apache-2.0 | V5 deterministic client-ID derivation remains internal; V4 supplies a fresh non-secret observer owner per process so overlapping tasks cannot share lease identity; admitted |
| `setuptools 83.0.0` | Build the dependency-free Python research package | PyPI | MIT | Exact build-only pin; admitted |
| AWS/archive Terraform providers `5.100.0`/`2.8.0` | Managed AWS IaC and dead-man package | HashiCorp Registry | MPL-2.0 | Locked checksums; admitted for private IaC |
| Terraform CLI `1.8.5` | Format and validate IaC | HashiCorp releases | BUSL-1.1 | Tool-only private use; operator must re-review terms before material reuse |
| Rust 1.88, Python 3.12, PostgreSQL 17, Dockerfile frontend 1.7, Distroless Debian 12 images | Build, checks, database, runtime | Official OCI publishers | Mixed; image SBOM controls | Every Dockerfile frontend/base image is digest-pinned; release scan/license inventory required |
| Pinned GitHub Actions and CI audit/lint tools | Checkout, tool setup, SBOM, scan, tests | Official project repositories/registries | Primarily MIT/Apache-2.0 | Commit-pinned actions; build-only; Dependabot monitored |
| `aquasecurity/trivy-action v0.36.0` (`ed142fd0673e97e23eac54620cfb913e5ce36c25`) | Scan the built OCI image for release-blocking vulnerabilities in CI | github.com/aquasecurity/trivy-action | Apache-2.0 | Replaces the broken v0.28.0 action pin; exact upstream commit, CI/build-only access, no production runtime dependency |
| `docker/setup-buildx-action v4.2.0` (`bb05f3f5519dd87d3ba754cc423b652a5edd6d2c`) | Provide the container-backed Buildx driver required for OCI provenance and SBOM attestations | github.com/docker/setup-buildx-action | MIT | Exact upstream commit, CI/build-only Docker control; avoids the hosted runner's non-attestation Docker driver |

The Python research package has no runtime PyPI dependency. AWS services and
Alpaca are external services governed by their account agreements rather than
software dependencies; their terms/entitlements are separate operator gates.

The Rust 1.88 floor admits the PyO3 release that resolves the current RustSec
advisories while remaining an exact, reproducible toolchain. Dependabot
proposals repeat maintenance, license, MSRV, API, and safety review.

### Release license gate

**HOLD — do not deploy externally or activate paper/live execution** until CI
generates the exact image and dependency SBOMs, every transitive component has a
recognized license compatible with private use, vulnerability checks pass, and
an operator/reviewer records acceptance for the immutable image digest. Unknown,
missing, changed, copyleft-network, source-unavailable, or unexpectedly
telemetric components block promotion. This gate is intentionally stricter than
allowing local foundation builds.
