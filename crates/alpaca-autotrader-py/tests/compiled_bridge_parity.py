"""Compiled PyO3/CLI parity gate.

``scripts/check-pyo3.sh`` builds the extension with the locked Rust workspace
and runs this file under the pinned Python 3.12 image. This test intentionally
imports the compiled module; it has no mock or Python fallback.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import subprocess
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_json(value: Any) -> str:
    return digest_bytes(compact(value).encode("utf-8"))


def timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def inputs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    strategy = {
        "kind": "momentum_trend",
        "momentum_lookback_sessions": 63,
        "trend_lookback_sessions": 126,
        "cadence": "weekly",
    }
    symbols = ["DIA", "IVV", "IWM", "QQQ", "SCHB", "SPY", "VOO", "VTI"]
    release = {
        "release_id": "compiled-parity-release",
        "code_hash": digest_bytes(b"code"),
        "parameters_hash": digest_json(strategy),
        "universe": symbols,
        "data_hash": digest_bytes(b"data"),
        "cost_model_hash": digest_bytes(b"cost"),
        "statistical_certificate_hash": digest_bytes(b"certificate"),
        "strategy": strategy,
        "valid_from": "2024-01-01T00:00:00Z",
        "expires_at": "2030-01-01T00:00:00Z",
    }
    account = {
        "account_fingerprint": digest_bytes(b"account"),
        "status": "active",
        "trading_blocked": False,
        "cash": 1_000_000_000,
        "buying_power": 1_000_000_000,
        "equity": 1_000_000_000,
        "day_pnl": 0,
        "drawdown": 0,
        "positions": [],
    }
    start = datetime(2025, 1, 1, 21, tzinfo=UTC)
    observations: list[dict[str, Any]] = []
    for symbol_index, symbol in enumerate(symbols):
        for session_index in range(127):
            completed = start + timedelta(days=session_index)
            observations.append(
                {
                    "symbol": symbol,
                    "session": completed.date().isoformat(),
                    "completed_at": timestamp(completed),
                    "raw_close": 50_000_000 + symbol_index * 1_000_000,
                    "total_return_close": (
                        100_000_000
                        + (symbol_index + 1) * session_index * 10_000
                    ),
                }
            )
    as_of = start + timedelta(days=127)
    snapshot = {
        "decision_id": "compiled-parity-decision",
        "release_id": release["release_id"],
        "as_of": timestamp(as_of),
        "market_session": (start + timedelta(days=126)).date().isoformat(),
        "schedule": {
            "eligible_cadences": ["weekly"],
            "calendar_evidence_hash": digest_bytes(b"calendar"),
        },
        "account": account,
        "account_snapshot_hash": digest_json(account),
        "observations": observations,
        "input_data_hash": digest_json(observations),
    }
    limits = {
        "max_gross_exposure": 500_000_000,
        "max_position_weight": 500_000,
        "max_positions": 1,
        "max_order_notional": 500_000_000,
        "max_planned_loss": 10_000_000,
        "daily_loss_limit": 25_000_000,
        "hard_drawdown_limit": 100_000_000,
        "planned_stop_distance_bps": 500,
        "marketable_limit_band_bps": 10,
        "new_positions_enabled": True,
    }
    return snapshot, release, limits


def main() -> None:
    if sys.version_info[:2] != (3, 12):
        raise SystemExit("compiled parity gate requires pinned Python 3.12")
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    core = importlib.import_module("alpaca_autotrader_core")
    module_file = getattr(core, "__file__", "")
    if not module_file or Path(module_file).suffix != ".so":
        raise AssertionError("alpaca_autotrader_core is not a compiled extension")
    snapshot, release, limits = inputs()
    replay_request = {
        "release": release,
        "risk_limits": limits,
        "snapshots": [snapshot],
    }
    with tempfile.TemporaryDirectory() as directory:
        paths = {}
        for name, value in (
            ("snapshot", snapshot),
            ("release", release),
            ("limits", limits),
            ("replay", replay_request),
        ):
            path = Path(directory, f"{name}.json")
            path.write_text(compact(value), encoding="utf-8")
            paths[name] = path
        cli_evaluation = subprocess.run(
            [
                str(args.binary),
                "evaluate",
                "--snapshot",
                str(paths["snapshot"]),
                "--release",
                str(paths["release"]),
                "--risk-limits",
                str(paths["limits"]),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        cli_replay = subprocess.run(
            [
                str(args.binary),
                "decision-replay",
                "--request",
                str(paths["replay"]),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        cli_backtest = subprocess.run(
            [
                str(args.binary),
                "backtest",
                "--request",
                str(paths["replay"]),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    compiled_evaluation = core.evaluate_decision(
        compact(snapshot), compact(release), compact(limits)
    )
    compiled_replay = core.decision_replay(compact(replay_request))
    compiled_backtest = core.backtest(compact(replay_request))
    if cli_evaluation != compiled_evaluation:
        raise AssertionError("CLI and compiled PyO3 evaluation bytes differ")
    if cli_replay != compiled_replay:
        raise AssertionError("CLI and compiled PyO3 replay bytes differ")
    if cli_backtest != compiled_backtest:
        raise AssertionError("CLI and compiled PyO3 backtest bytes differ")
    parsed = json.loads(compiled_evaluation)
    if "order_plans" not in parsed or "intents" in parsed:
        raise AssertionError("compiled bridge did not return safe non-executable plans")
    backtest = json.loads(compiled_backtest)
    if backtest.get("performance_evidence_available") is not False:
        raise AssertionError("incomplete backtest path did not fail closed")
    print("compiled PyO3/CLI parity: PASS")


if __name__ == "__main__":
    main()
