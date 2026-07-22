# How this repository was built (a note for visitors)

Every line of code in this repository was written by AI coding agents — OpenAI Codex
built the system, with Claude (Anthropic) used for independent audit and analysis. It
was directed by a single operator with no software employment history and no computer
science degree: a former homebuilder purchasing manager who runs a small manufacturing
business.

That is not a confession. It is the experiment.

## What the operator actually did

"Prompted an AI" does not describe it, so here is what the human contributed:

- **The constitution.** [AGENTS.md](AGENTS.md) is a working-agreements contract every
  agent session ingests before touching the code: fail closed on anything unverifiable,
  never weaken a gate to make a test pass, software may not approve its own release or
  clear its own halts.
- **Mechanical distrust.** A change is done when [`./scripts/check.sh`](scripts/check.sh)
  passes — clean-room audit, secret scan, the full Rust/Python/SQL test suites,
  cross-language parity, database invariant and race checks. No one's self-report is
  trusted here, including the AI's, including the operator's.
- **Enforced honesty.** [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md)
  grades every capability by evidence — *offline verified*, *structural only*, or
  *absent* — and the system's standing posture is **HOLD — do not trade** until real
  evidence exists. There are no TODOs and no stubs in ~32,000 lines of Rust: everything
  either works as verified or refuses to run.
- **Provenance.** [CLEAN_ROOM.md](CLEAN_ROOM.md): blank Git history, no prior-project
  material, every external input recorded. The audit trail is the entire point.

An independent AI audit of the whole project — including the parts that conclude it may
never pass its own economic gates — is published verbatim in the technical handoff
document attached to this repository's releases. If the builder will do that to his own
work, that is the standard applied to everything here.

The obvious hole in a story like this is that "the agents worked under a governance
contract" is a claim about a process nobody else watched. [stinger/](stinger/) closes it as
far as it can currently be closed: a corpus of sandboxed traps, each one encoding a rule
taken verbatim from this repository's own AGENTS.md, built so that obeying the rule is the
slow path to a green test suite. The agent under test runs contained, and whether it cheated
is decided by deterministic detectors — never by a language model's opinion. Every scenario
must prove it catches its own intended cheat before it is allowed to judge anything.

The result so far is six honest outcomes across all five trap families. It is deliberately
**not** presented as a score: it is one repetition per scenario, and
[stinger/RESULTS.md](stinger/RESULTS.md) says so in the same breath as the number, along with
the three failures that had to be fixed before the measurement meant anything at all. One of
those was a trap this operator wrote badly enough that it could never have fired — caught not
by review but by the harness refusing to run it.

## What this is not

It is not connected to any real brokerage account, database, or cloud. It has no
evidence its trading strategy works, and its own documents treat "no strategy ever
qualifies" as a valid final outcome. It is not investment advice, not a product, and
not a claim that AI replaces engineers. It is one datapoint about what a disciplined
non-engineer can now direct AI to build — with the receipts public so you can judge
the claim yourself.

## Contact

Chris McNosky · Dallas–Fort Worth, TX · cmcnosky@gmail.com
Available for AI-systems direction, agent-governance consulting, and roles where
"make AI-built software provably trustworthy" is the job.
