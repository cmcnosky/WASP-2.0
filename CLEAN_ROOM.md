# Clean-room charter

## Purpose

This repository implements a new private, single-user Alpaca trading system
from a blank Git history. Its design inputs are the requirements recorded in
this repository and current public primary documentation. It is not a port,
rewrite, continuation, or derivative of any previous software project.

## Prohibited inputs

Contributors must not inspect or reuse prior trading or accounting application
code, configuration, schemas, migrations, tests, datasets, notebooks, generated
artifacts, prompts, secrets, logs, runtime state, infrastructure, or design
documents. This prohibition includes EdgeLedger and every earlier Wasp project.
Knowing that those names exist is not permission to open their files.

No local dependency, symlink, Git submodule, file URL, or build input may point
outside this repository. Public dependencies must be fetched from their normal
package registries and recorded in lockfiles.

## Allowed inputs

- Requirements and decisions checked into this repository.
- Current public primary documentation for Alpaca, AWS, Rust, Python, PostgreSQL,
  Terraform, and selected dependencies.
- Newly acquired data whose provenance, entitlement, availability time, and
  checksum are recorded by this system.
- Original implementation and tests written for this repository.

## Evidence and audit

`./scripts/check-clean-room.sh` performs a bounded mechanical audit of this
repository. It rejects submodules, symlinks, external path dependencies,
tracked data/archive artifacts, common secret material, and legacy identifiers
inside implementation surfaces. The audit cannot prove independent creation;
every contributor is also responsible for recording new external design inputs
in `docs/DEPENDENCIES.md` or `docs/DECISIONS.md`.

If contamination is suspected, stop work, preserve evidence without opening the
suspect artifact, and follow `docs/runbooks/INCIDENT_RESPONSE.md`. Do not repair
the issue by silently deleting history.

## Initial attestation

At repository creation, the working tree contained only a new `.git` directory.
No previous project was inspected to create this implementation. The first
implementation checkpoint must include this charter and the governance,
security, architecture, and decision records that accompany it.
