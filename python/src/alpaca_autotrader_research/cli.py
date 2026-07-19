"""Small dependency-free operator CLI for the research layer."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from .core_bridge import CoreBridge, CoreBridgeError
from .gates import GateEvidence, evaluate_gates
from .hashing import canonical_json_text
from .ledger import ExperimentLedger, LedgerIntegrityError
from .protocol import generate_preregistration


def _strict_object(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as source:
        value = json.load(source, parse_constant=_reject_json_constant)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value!r} is forbidden")


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed


def _write_json(value: Any, output: Path | None) -> None:
    text = canonical_json_text(value) + "\n"
    if output is None:
        sys.stdout.write(text)
    else:
        output.write_text(text, encoding="utf-8")


def _gate_evidence(value: Mapping[str, Any]) -> GateEvidence:
    return GateEvidence(
        annual_recurring_cost=Decimal(str(value["annual_recurring_cost"])),
        planned_live_capital=Decimal(str(value["planned_live_capital"])),
        annualized_oos_return_lcb=float(value["annualized_oos_return_lcb"]),
        deflated_sharpe_probability=_optional_float(value.get("deflated_sharpe_probability")),
        probability_backtest_overfit=_optional_float(value.get("probability_backtest_overfit")),
        familywise_p_value=_optional_float(value.get("familywise_p_value")),
        statistical_power=_optional_float(value.get("statistical_power")),
        stressed_drawdown=float(value["stressed_drawdown"]),
        certified_max_drawdown=float(value["certified_max_drawdown"]),
        minimum_track_record_passed=_strict_bool(
            value["minimum_track_record_passed"], "minimum_track_record_passed"
        ),
        concentration_passed=_strict_bool(value["concentration_passed"], "concentration_passed"),
        independent_reproduction_passed=_strict_bool(
            value["independent_reproduction_passed"], "independent_reproduction_passed"
        ),
        data_quality_passed=_strict_bool(value["data_quality_passed"], "data_quality_passed"),
        sealed_holdout_passed=_strict_bool(value["sealed_holdout_passed"], "sealed_holdout_passed"),
    )


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _strict_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a JSON boolean")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="alpaca-research")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preregister = subparsers.add_parser("preregister", help="emit the locked experiment family")
    preregister.add_argument("--family-id", required=True)
    preregister.add_argument("--created-at", type=_timestamp, required=True)
    preregister.add_argument("--universe-manifest-hash", required=True)
    preregister.add_argument("--data-snapshot-hash", required=True)
    preregister.add_argument("--output", type=Path)

    verify = subparsers.add_parser("verify-ledger", help="verify the full JSONL hash chain")
    verify.add_argument("ledger", type=Path)

    append = subparsers.add_parser("append-ledger", help="append one immutable research event")
    append.add_argument("ledger", type=Path)
    append.add_argument("--event-type", required=True)
    append.add_argument("--payload", type=Path, required=True)

    gates = subparsers.add_parser("gates", help="evaluate fail-closed promotion gates")
    gates.add_argument("evidence", type=Path)

    subparsers.add_parser("core-status", help="verify the compiled Rust core")

    decision = subparsers.add_parser("core-evaluate", help="invoke the Rust decision core")
    decision.add_argument("--snapshot", type=Path, required=True)
    decision.add_argument("--release", type=Path, required=True)
    decision.add_argument("--risk-limits", type=Path, required=True)

    backtest = subparsers.add_parser(
        "core-backtest", help="invoke the fail-closed Rust performance-backtest boundary"
    )
    backtest.add_argument("request", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "preregister":
            registration = generate_preregistration(
                family_id=args.family_id,
                created_at=args.created_at,
                universe_manifest_hash=args.universe_manifest_hash,
                data_snapshot_hash=args.data_snapshot_hash,
            )
            _write_json(registration, args.output)
        elif args.command == "verify-ledger":
            entries = ExperimentLedger(args.ledger).read_verified()
            _write_json({"entries": len(entries), "verified": True}, None)
        elif args.command == "append-ledger":
            entry = ExperimentLedger(args.ledger).append(
                args.event_type, _strict_object(args.payload)
            )
            _write_json(entry, None)
        elif args.command == "gates":
            report = evaluate_gates(_gate_evidence(_strict_object(args.evidence)))
            _write_json(report.as_dict(), None)
            return 0 if report.eligible else 2
        elif args.command == "core-status":
            identity = CoreBridge.load().identity
            _write_json(
                {"module": identity.module, "version": identity.version, "available": True},
                None,
            )
        elif args.command == "core-evaluate":
            response = CoreBridge.load().evaluate_decision(
                snapshot=_strict_object(args.snapshot),
                release=_strict_object(args.release),
                risk_limits=_strict_object(args.risk_limits),
            )
            _write_json(response, None)
        elif args.command == "core-backtest":
            _write_json(CoreBridge.load().backtest(_strict_object(args.request)), None)
        else:  # pragma: no cover - argparse enforces this
            raise AssertionError("unreachable command")
    except (CoreBridgeError, LedgerIntegrityError, OSError, ValueError, KeyError) as error:
        print(f"HOLD: {error}", file=sys.stderr)
        return 2
    return 0
