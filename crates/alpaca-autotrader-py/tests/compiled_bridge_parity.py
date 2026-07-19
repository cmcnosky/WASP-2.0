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
import os
import subprocess
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable


_CANONICAL_DIGEST: Callable[[Any], str] | None = None
_JSON_HASH_PROFILE = "wasp-json-sha256-v1"
_PERFORMANCE_REQUEST_MAX_BYTES = 16 * 1024 * 1024
_GOLDEN_IDENTIFIERS = {
    "release_hash": "65a58d7df98da1a8106d3bf3b9bee3c39c87603829a78be798b201ee6ebea4c2",
    "decision_snapshot_hash": "8a8ea28dedff495690766eacf54db07792d4d9bd3407369078aa7cb4054cb4d3",
    "decision_evidence_hash": "430913a73659e9f41226246fc969aaa137d4d59f430873d618a3ae2b182cb77d",
    "plan_id": "e1c7b23e-67e8-5747-9598-231ea6de3536",
    "materialization_evidence_hash": (
        "8fb85efb8990d1ad64931f18b1829aa3ab32645ee2966958f15a877e0f9a2251"
    ),
    "intent_id": "0b355745-88c4-57b3-9d25-e80203f43bc4",
    "client_order_id": "autotrader-b710a13308a56c398246b7e1",
}


def compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_json(value: Any) -> str:
    if _CANONICAL_DIGEST is None:
        raise AssertionError("official Python canonical digest is not loaded")
    return _CANONICAL_DIGEST(value)


def execution_quote_hash(quote: dict[str, Any]) -> str:
    return digest_json(
        [
            quote["symbol"],
            quote["raw_price"],
            quote["provider_at"],
            quote["received_at"],
            quote["valid_until"],
            quote["payload_hash"],
        ]
    )


def timestamp(value: datetime) -> str:
    value = value.astimezone(UTC)
    base = value.strftime("%Y-%m-%dT%H:%M:%S")
    if value.microsecond == 0:
        return base + "Z"
    if value.microsecond % 1_000 == 0:
        return f"{base}.{value.microsecond // 1_000:03d}Z"
    return f"{base}.{value.microsecond:06d}Z"


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


def performance_input(
    core: Any,
    snapshot: dict[str, Any],
    release: dict[str, Any],
    limits: dict[str, Any],
    locked_cost_model: Any,
) -> dict[str, Any]:
    """Build evidence around a plan emitted by Rust; Python never creates the plan."""

    cost_model = {
        "model_id": "locked-cost-v1",
        "decision_to_arrival_latency_ms": locked_cost_model.decision_to_arrival_latency_ms,
        "half_spread_bps": locked_cost_model.half_spread_bps,
        "adverse_slippage_bps": locked_cost_model.adverse_slippage_bps,
        "opportunity_cost_bps": locked_cost_model.opportunity_cost_bps,
        "non_fill_probability_bps": locked_cost_model.non_fill_probability_bps,
        "partial_fill_probability_bps": locked_cost_model.partial_fill_probability_bps,
        # Rust money values are fixed at six decimal places; one cent is 10_000 units.
        "minimum_order_fee": locked_cost_model.minimum_fee_cents * 10_000,
        "stress_variable_cost_multiplier_bps": (
            locked_cost_model.stress_variable_cost_multiplier_bps
        ),
        "stress_empirical_percentile_bps": locked_cost_model.stress_empirical_percentile_bps,
    }
    start = datetime(2025, 1, 1, 21, tzinfo=UTC)
    performance_as_of = start + timedelta(days=126, hours=1)
    sessions = []
    for index in range(128):
        close = start + timedelta(days=index)
        calendar_hash = (
            snapshot["schedule"]["calendar_evidence_hash"]
            if index == 126
            else digest_bytes(f"calendar-{index}".encode())
        )
        sessions.append(
            {
                "session": close.date().isoformat(),
                "regular_open_at": timestamp(close - timedelta(hours=6, minutes=30)),
                "regular_close_at": timestamp(close),
                "eligible_cadences": snapshot["schedule"]["eligible_cadences"]
                if index == 126
                else [],
                "calendar_payload_hash": calendar_hash,
            }
        )
    terminal_observations: list[dict[str, Any]] = []
    for symbol_index, symbol in enumerate(release["universe"]):
        for session_index in range(128):
            close = start + timedelta(days=session_index)
            terminal_observations.append(
                {
                    "symbol": symbol,
                    "session": close.date().isoformat(),
                    "completed_at": timestamp(close),
                    "raw_close": 50_000_000 + symbol_index * 1_000_000,
                    "total_return_close": (
                        100_000_000
                        + (symbol_index + 1) * session_index * 10_000
                    ),
                }
            )
    terminal_as_of = start + timedelta(days=127, hours=1)
    terminal_input_hash = digest_json(terminal_observations)
    observation_partitions = [
        {
            "partition_id": "observations-126",
            "through_session": sessions[126]["session"],
            "available_at": timestamp(performance_as_of),
            "rows_hash": snapshot["input_data_hash"],
        },
        {
            "partition_id": "observations-127",
            "through_session": sessions[127]["session"],
            "available_at": timestamp(terminal_as_of),
            "rows_hash": terminal_input_hash,
        },
    ]
    empty_dividends: list[dict[str, Any]] = []
    dividend_partitions = [
        {
            "partition_id": "dividends-compiled-parity-decision",
            "available_at": timestamp(performance_as_of),
            "event_count": 0,
            "events_hash": digest_json(empty_dividends),
        },
        {
            "partition_id": "dividends-compiled-parity-terminal",
            "available_at": timestamp(terminal_as_of),
            "event_count": 0,
            "events_hash": digest_json(empty_dividends),
        },
    ]
    quote_by_symbol: dict[str, dict[str, Any]] = {}
    next_open = start + timedelta(days=127, hours=-6, minutes=-30)
    provider_at = next_open + timedelta(milliseconds=500)
    received_at = provider_at + timedelta(seconds=1)
    for symbol_index, symbol in enumerate(release["universe"]):
        quote_by_symbol[symbol] = {
            "symbol": symbol,
            "raw_price": 50_000_000 + symbol_index * 1_000_000,
            "provider_at": timestamp(provider_at),
            "received_at": timestamp(received_at),
            "valid_until": timestamp(received_at + timedelta(seconds=10)),
            "payload_hash": digest_bytes(f"performance-quote-{symbol}".encode()),
        }
    execution_quote_hashes = sorted(
        execution_quote_hash(quote) for quote in quote_by_symbol.values()
    )
    manifest = {
        "dataset_id": "compiled-parity-synthetic-v1",
        "stage": "synthetic",
        "source": "compiled-parity-fixture",
        "feed": "synthetic",
        "adjustment_mode": "raw-and-total-return",
        "symbols": release["universe"],
        "sessions": sessions,
        "evaluation_start": sessions[126]["session"],
        # The final session is reserved for next-session execution/valuation;
        # decision sessions are the half-open interval [start, end).
        "evaluation_end": sessions[127]["session"],
        "observation_partitions": observation_partitions,
        "dividend_partitions": dividend_partitions,
        "execution_quote_hashes": execution_quote_hashes,
        "raw_objects_hash": digest_bytes(b"synthetic-raw"),
        "normalized_rows_hash": digest_json(observation_partitions),
        "execution_rows_hash": digest_json(execution_quote_hashes),
        "corporate_actions_hash": digest_json(dividend_partitions),
        "unresolved_critical_defects": 0,
        "certified_at": None,
        "certifier_subject": None,
    }
    manifest_hash = digest_json(manifest)
    cost_hash = digest_json(cost_model)
    performance_release = dict(release)
    performance_release["data_hash"] = manifest_hash
    performance_release["cost_model_hash"] = cost_hash
    performance_snapshot = dict(snapshot)
    performance_snapshot["release_id"] = performance_release["release_id"]
    performance_snapshot["as_of"] = timestamp(performance_as_of)
    evaluation = json.loads(
        core.evaluate_decision(
            compact(performance_snapshot), compact(performance_release), compact(limits)
        )
    )
    plans = evaluation["order_plans"]
    if len(plans) != 1:
        raise AssertionError("performance parity fixture did not emit exactly one Rust plan")
    plan = plans[0]
    submitted_at = received_at + timedelta(seconds=1)
    filled_at = submitted_at + timedelta(seconds=1)
    terminal_at = filled_at + timedelta(seconds=1)
    quote_price = plan["decision_reference_price"]
    modeled_fill_price = quote_price * 10_010 // 10_000
    quote = quote_by_symbol[plan["symbol"]]
    if quote["raw_price"] != quote_price:
        raise AssertionError("performance quote does not match its certified symbol row")
    decision = {
        "decision_id": snapshot["decision_id"],
        "as_of": timestamp(performance_as_of),
        "market_session": snapshot["market_session"],
        "schedule": snapshot["schedule"],
        "observations": snapshot["observations"],
        "input_data_hash": snapshot["input_data_hash"],
        "observation_partition_id": "observations-126",
        "dividend_partition_id": "dividends-compiled-parity-decision",
        "outcomes": [
            {
                "plan_id": plan["plan_id"],
                "execution_session": sessions[127]["session"],
                "quote": quote,
                "quote_evidence_hash": execution_quote_hash(quote),
                "submitted_at": timestamp(submitted_at),
                "fills": [
                    {
                        "fill_id": "compiled-performance-fill-1",
                        "quantity": plan["quantity"],
                        "price": modeled_fill_price,
                        "fee": 0,
                        "filled_at": timestamp(filled_at),
                        "payload_hash": digest_bytes(b"performance-fill"),
                    }
                ],
                "terminal_at": timestamp(terminal_at),
                "terminal_reason": "filled",
                "terminal_payload_hash": digest_bytes(b"performance-terminal"),
            }
        ],
    }
    terminal_valuation = {
        "session": sessions[127]["session"],
        "as_of": timestamp(terminal_as_of),
        "observations": terminal_observations,
        "input_data_hash": terminal_input_hash,
        "observation_partition_id": "observations-127",
        "dividend_partition_id": "dividends-compiled-parity-terminal",
    }
    return {
        "run_id": "compiled-performance-run",
        "run_at": timestamp(terminal_as_of + timedelta(hours=1)),
        "preregistration_hash": digest_bytes(b"preregistration"),
        "release": performance_release,
        "risk_limits": limits,
        "account_fingerprint": snapshot["account"]["account_fingerprint"],
        "initial_cash": snapshot["account"]["cash"],
        "started_at": timestamp(start - timedelta(hours=1)),
        "dataset_manifest": manifest,
        "dataset_manifest_hash": manifest_hash,
        "cost_model": cost_model,
        "cost_model_hash": cost_hash,
        "dividend_events": [],
        "holdout_access": None,
        "decisions": [decision],
        "terminal_valuation": terminal_valuation,
    }


def main() -> None:
    global _CANONICAL_DIGEST

    if sys.version_info[:2] != (3, 12):
        raise SystemExit("compiled parity gate requires pinned Python 3.12")
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    core = importlib.import_module("alpaca_autotrader_core")
    module_file = getattr(core, "__file__", "")
    if not module_file or Path(module_file).suffix != ".so":
        raise AssertionError("alpaca_autotrader_core is not a compiled extension")

    research_src = Path(__file__).resolve().parents[3] / "python" / "src"
    sys.path.insert(0, str(research_src))
    from alpaca_autotrader_research.core_bridge import CoreBridge, CoreInvocationError
    from alpaca_autotrader_research.hashing import sha256_digest
    from alpaca_autotrader_research.protocol import LOCKED_COST_MODEL

    _CANONICAL_DIGEST = sha256_digest

    if getattr(core, "__json_hash_profile__", None) != _JSON_HASH_PROFILE:
        raise AssertionError("compiled core did not expose the locked JSON hash profile")
    if (
        getattr(core, "__performance_request_max_bytes__", None)
        != _PERFORMANCE_REQUEST_MAX_BYTES
    ):
        raise AssertionError("compiled core did not expose the locked request byte ceiling")
    bridge = CoreBridge(core)
    if bridge.identity.json_hash_profile != _JSON_HASH_PROFILE:
        raise AssertionError("Python bridge did not bind the compiled JSON hash profile")
    if (
        bridge.identity.performance_request_max_bytes
        != _PERFORMANCE_REQUEST_MAX_BYTES
    ):
        raise AssertionError("Python bridge did not bind the compiled request byte ceiling")

    snapshot, release, limits = inputs()
    replay_request = {
        "release": release,
        "risk_limits": limits,
        "snapshots": [snapshot],
    }
    performance_request = performance_input(
        core, snapshot, release, limits, LOCKED_COST_MODEL
    )
    compiled_evaluation = core.evaluate_decision(
        compact(snapshot), compact(release), compact(limits)
    )
    parsed = json.loads(compiled_evaluation)
    plans = parsed.get("order_plans")
    if not isinstance(plans, list) or len(plans) != 1 or "intents" in parsed:
        raise AssertionError("compiled bridge did not return one safe non-executable plan")
    risk = parsed["risk"]
    plan = plans[0]
    as_of = datetime.fromisoformat(snapshot["as_of"].replace("Z", "+00:00"))
    provider_at = as_of + timedelta(seconds=1)
    received_at = provider_at + timedelta(seconds=1)
    quote = {
        "symbol": plan["symbol"],
        # Deliberately differ from the decision reference: executable pricing
        # must come from this separately evidenced post-decision observation.
        "raw_price": plan["decision_reference_price"] - 10_000,
        "provider_at": timestamp(provider_at),
        "received_at": timestamp(received_at),
        "valid_until": timestamp(received_at + timedelta(seconds=10)),
        "payload_hash": digest_bytes(b"fresh-execution-quote"),
    }
    materialization_arguments = (
        compact(snapshot),
        compact(release),
        compact(risk),
        compact(plan),
        compact(quote),
    )
    compiled_intent = core.materialize_order_intent(*materialization_arguments)
    python_intent = bridge.materialize_order_intent(
        snapshot=snapshot,
        release=release,
        risk_decision=risk,
        plan=plan,
        quote=quote,
    )

    with tempfile.TemporaryDirectory() as directory:
        paths = {}
        for name, value in (
            ("snapshot", snapshot),
            ("release", release),
            ("limits", limits),
            ("replay", replay_request),
            ("performance", performance_request),
            ("risk", risk),
            ("plan", plan),
            ("quote", quote),
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
        cli_performance = subprocess.run(
            [
                str(args.binary),
                "performance-backtest",
                "--request",
                str(paths["performance"]),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        python_environment = os.environ.copy()
        python_environment["PYTHONPATH"] = os.pathsep.join(
            [str(research_src), python_environment.get("PYTHONPATH", "")]
        ).rstrip(os.pathsep)
        python_cli_performance = subprocess.run(
            [
                sys.executable,
                "-m",
                "alpaca_autotrader_research",
                "core-performance-backtest",
                str(paths["performance"]),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=python_environment,
        ).stdout.strip()
        cli_intent = subprocess.run(
            [
                str(args.binary),
                "materialize-intent",
                "--snapshot",
                str(paths["snapshot"]),
                "--release",
                str(paths["release"]),
                "--risk-decision",
                str(paths["risk"]),
                "--plan",
                str(paths["plan"]),
                "--quote",
                str(paths["quote"]),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        performance_text = compact(performance_request)
        duplicate_payloads = {
            "top-level": performance_text.replace(
                '{"run_id":',
                '{"run_id":"duplicate-run","run_id":',
                1,
            ),
            "nested": performance_text.replace(
                '"release":{"release_id":',
                '"release":{"release_id":"duplicate-release","release_id":',
                1,
            ),
        }
        for label, duplicate_payload in duplicate_payloads.items():
            if duplicate_payload == performance_text:
                raise AssertionError(f"failed to create {label} duplicate-key fixture")
            duplicate_path = Path(directory, f"duplicate-{label}.json")
            duplicate_path.write_text(duplicate_payload, encoding="utf-8")
            rust_rejection = subprocess.run(
                [
                    str(args.binary),
                    "performance-backtest",
                    "--request",
                    str(duplicate_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if rust_rejection.returncode == 0 or "duplicate" not in (
                rust_rejection.stdout + rust_rejection.stderr
            ).lower():
                raise AssertionError(
                    f"compiled CLI did not reject {label} duplicate JSON key"
                )
            python_rejection = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alpaca_autotrader_research",
                    "core-performance-backtest",
                    str(duplicate_path),
                ],
                check=False,
                capture_output=True,
                text=True,
                env=python_environment,
            )
            if python_rejection.returncode == 0 or "duplicate" not in (
                python_rejection.stdout + python_rejection.stderr
            ).lower():
                raise AssertionError(
                    f"Python CLI did not reject {label} duplicate JSON key"
                )
            try:
                core.performance_backtest(duplicate_payload)
            except Exception as error:
                if "duplicate" not in str(error).lower():
                    raise AssertionError(
                        f"compiled PyO3 duplicate rejection was ambiguous for {label}"
                    ) from error
            else:
                raise AssertionError(
                    f"compiled PyO3 did not reject {label} duplicate JSON key"
                )

        oversized_payload = " " * (_PERFORMANCE_REQUEST_MAX_BYTES + 1)
        oversized_path = Path(directory, "oversized-performance.json")
        oversized_path.write_text(oversized_payload, encoding="utf-8")
        rust_oversized = subprocess.run(
            [
                str(args.binary),
                "performance-backtest",
                "--request",
                str(oversized_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if rust_oversized.returncode == 0 or "serialized byte ceiling" not in (
            rust_oversized.stdout + rust_oversized.stderr
        ):
            raise AssertionError("compiled CLI did not enforce the request byte ceiling")
        python_oversized = subprocess.run(
            [
                sys.executable,
                "-m",
                "alpaca_autotrader_research",
                "core-performance-backtest",
                str(oversized_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            env=python_environment,
        )
        if python_oversized.returncode == 0 or "serialized byte ceiling" not in (
            python_oversized.stdout + python_oversized.stderr
        ):
            raise AssertionError("Python CLI did not enforce the request byte ceiling")
        try:
            core.performance_backtest(oversized_payload)
        except Exception as error:
            if "serialized byte ceiling" not in str(error):
                raise AssertionError(
                    "compiled PyO3 request-size rejection was ambiguous"
                ) from error
        else:
            raise AssertionError("compiled PyO3 did not enforce the request byte ceiling")
        try:
            bridge.performance_backtest(
                {"padding": "x" * _PERFORMANCE_REQUEST_MAX_BYTES}
            )
        except CoreInvocationError as error:
            if "serialized byte ceiling" not in str(error):
                raise AssertionError(
                    "Python bridge request-size rejection was ambiguous"
                ) from error
        else:
            raise AssertionError("Python bridge did not enforce the request byte ceiling")
    compiled_replay = core.decision_replay(compact(replay_request))
    compiled_backtest = core.backtest(compact(replay_request))
    compiled_performance = core.performance_backtest(compact(performance_request))
    python_performance = bridge.performance_backtest(performance_request)
    if cli_evaluation != compiled_evaluation:
        raise AssertionError("CLI and compiled PyO3 evaluation bytes differ")
    if cli_replay != compiled_replay:
        raise AssertionError("CLI and compiled PyO3 replay bytes differ")
    if cli_backtest != compiled_backtest:
        raise AssertionError("CLI and compiled PyO3 backtest bytes differ")
    if cli_performance != compiled_performance:
        raise AssertionError("CLI and compiled PyO3 performance bytes differ")
    if python_cli_performance != compiled_performance:
        raise AssertionError("Python CLI and compiled PyO3 performance bytes differ")
    if compact(python_performance) != compiled_performance:
        raise AssertionError("Python bridge and compiled PyO3 performance bytes differ")
    if cli_intent != compiled_intent:
        raise AssertionError("CLI and compiled PyO3 intent bytes differ")
    if compact(python_intent) != compiled_intent:
        raise AssertionError("Python bridge and compiled PyO3 intent bytes differ")
    intent = json.loads(compiled_intent)
    fresh_quote_evidence = {
        "decision_at": snapshot["as_of"],
        "arrival_quote": quote["raw_price"],
        "quote_provider_at": quote["provider_at"],
        "quote_received_at": quote["received_at"],
        "quote_valid_until": quote["valid_until"],
        "quote_payload_hash": quote["payload_hash"],
        "created_at": quote["received_at"],
    }
    for field, expected in fresh_quote_evidence.items():
        if intent.get(field) != expected:
            raise AssertionError(f"materialized intent lost fresh quote evidence: {field}")
    if intent.get("decision_evidence_hash") != plan["decision_evidence_hash"]:
        raise AssertionError("materialized intent lost decision evidence")
    if intent.get("materialization_evidence_hash") == plan["decision_evidence_hash"]:
        raise AssertionError("fresh quote was not bound into distinct materialization evidence")
    if intent.get("arrival_quote") == plan["decision_reference_price"]:
        raise AssertionError("intent reused its non-executable decision reference price")
    backtest = json.loads(compiled_backtest)
    if backtest.get("performance_evidence_available") is not False:
        raise AssertionError("incomplete backtest path did not fail closed")
    performance = json.loads(compiled_performance)
    if performance.get("mechanical_metrics_available") is not True:
        raise AssertionError("performance replay did not return mechanical metrics")
    if performance.get("stressed_performance_evidence_available") is not False:
        raise AssertionError("synthetic replay claimed stressed performance evidence")
    if performance.get("decision_window_complete") is not True:
        raise AssertionError("complete decision window was reported as a prefix")
    if performance.get("terminal_valuation_complete") is not True:
        raise AssertionError("required terminal valuation was not reported as complete")
    if performance.get("evaluation_complete") is not True:
        raise AssertionError("complete decision and terminal windows were reported incomplete")
    if performance.get("qualifies_as_strategy_evidence") is not False:
        raise AssertionError("synthetic performance replay claimed efficacy evidence")
    observed_identifiers = {
        "release_hash": digest_json(release),
        "decision_snapshot_hash": digest_json(snapshot),
        "decision_evidence_hash": plan["decision_evidence_hash"],
        "plan_id": plan["plan_id"],
        "materialization_evidence_hash": intent["materialization_evidence_hash"],
        "intent_id": intent["intent_id"],
        "client_order_id": intent["client_order_id"],
    }
    if observed_identifiers != _GOLDEN_IDENTIFIERS:
        raise AssertionError(
            "cross-language deterministic identifier golden vector changed: "
            f"{observed_identifiers!r}"
        )
    print("compiled PyO3/CLI parity: PASS")


if __name__ == "__main__":
    main()
