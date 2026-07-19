use std::{fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use trader_core::{
    backtest::DecisionReplayRequest, evaluate_decision, materialize_order_intent, run_backtest,
    run_decision_replay, DecisionSnapshot, FreshExecutionQuote, OrderPlan, RiskDecision,
    RiskLimitSnapshot, StrategyRelease,
};
use trader_execution::config::RuntimeConfig;

#[derive(Debug, Parser)]
#[command(
    name = "alpaca-autotrader",
    version,
    about = "Fail-closed private trading runtime"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Provider-free process health check. No credentials or network are read.
    Health {
        #[arg(long)]
        local: bool,
    },
    /// Validate environment and endpoint isolation without contacting Alpaca.
    ValidateConfig {
        #[arg(long)]
        config: PathBuf,
    },
    /// Evaluate one immutable decision snapshot through strategy and risk.
    Evaluate {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        release: PathBuf,
        #[arg(long = "risk-limits")]
        risk_limits: PathBuf,
    },
    /// Replay chronological snapshots through the exact production core.
    Backtest {
        #[arg(long)]
        request: PathBuf,
    },
    /// Provider-free deterministic decision parity replay; never performance evidence.
    DecisionReplay {
        #[arg(long)]
        request: PathBuf,
    },
    /// Materialize one authorized plan using a fresh post-decision raw quote.
    MaterializeIntent {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        release: PathBuf,
        #[arg(long = "risk-decision")]
        risk_decision: PathBuf,
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        quote: PathBuf,
    },
}

fn read_json<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("invalid JSON in {}", path.display()))
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Health { local } => {
            if !local {
                bail!("only provider-free `health --local` is implemented; network health requires an authorized adapter")
            }
            print_json(&serde_json::json!({
                "status": "ok",
                "mode": "local",
                "provider_access": false,
                "submission_enabled": false,
                "version": env!("CARGO_PKG_VERSION"),
            }))
        }
        Command::ValidateConfig { config } => {
            let config: RuntimeConfig = read_json(&config)?;
            config.validate()?;
            print_json(&serde_json::json!({
                "status": "valid",
                "environment": config.environment,
                "submission_enabled": config.submission_enabled,
            }))
        }
        Command::Evaluate {
            snapshot,
            release,
            risk_limits,
        } => {
            let snapshot: DecisionSnapshot = read_json(&snapshot)?;
            let release: StrategyRelease = read_json(&release)?;
            let limits: RiskLimitSnapshot = read_json(&risk_limits)?;
            print_json(&evaluate_decision(&snapshot, &release, &limits)?)
        }
        Command::Backtest { request } => {
            let request: DecisionReplayRequest = read_json(&request)?;
            print_json(&run_backtest(&request)?)
        }
        Command::DecisionReplay { request } => {
            let request: DecisionReplayRequest = read_json(&request)?;
            print_json(&run_decision_replay(&request)?)
        }
        Command::MaterializeIntent {
            snapshot,
            release,
            risk_decision,
            plan,
            quote,
        } => {
            let snapshot: DecisionSnapshot = read_json(&snapshot)?;
            let release: StrategyRelease = read_json(&release)?;
            let risk: RiskDecision = read_json(&risk_decision)?;
            let plan: OrderPlan = read_json(&plan)?;
            let quote: FreshExecutionQuote = read_json(&quote)?;
            print_json(&materialize_order_intent(
                &snapshot, &release, &risk, &plan, &quote,
            )?)
        }
    }
}
