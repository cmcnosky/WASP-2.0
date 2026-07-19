use std::{fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use trader_core::{
    backtest::DecisionReplayRequest, evaluate_decision, run_backtest, run_decision_replay,
    DecisionSnapshot, RiskLimitSnapshot, StrategyRelease,
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
    }
}
