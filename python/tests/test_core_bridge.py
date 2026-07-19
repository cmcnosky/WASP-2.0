import json
from types import ModuleType
import unittest
from unittest.mock import patch

from alpaca_autotrader_research.core_bridge import (
    CoreBridge,
    CoreInvocationError,
    CoreProtocolError,
    CoreUnavailableError,
)


def fake_core() -> ModuleType:
    module = ModuleType("alpaca_autotrader_core")
    setattr(module, "__version__", "test")
    setattr(
        module,
        "evaluate_decision",
        lambda snapshot, release, limits: json.dumps(
            {"snapshot": json.loads(snapshot), "source": "rust"}
        ),
    )
    setattr(
        module,
        "backtest",
        lambda request: json.dumps({"request": json.loads(request), "source": "rust"}),
    )
    setattr(
        module,
        "materialize_order_intent",
        lambda snapshot, release, risk, plan, quote: json.dumps(
            {
                "snapshot": json.loads(snapshot),
                "release": json.loads(release),
                "risk": json.loads(risk),
                "plan": json.loads(plan),
                "quote": json.loads(quote),
                "source": "rust",
            }
        ),
    )
    return module


class CoreBridgeTests(unittest.TestCase):
    def test_delegates_decision_to_compiled_contract(self) -> None:
        response = CoreBridge(fake_core()).evaluate_decision(
            snapshot={"b": 2, "a": 1}, release={"id": "r"}, risk_limits={"gross": "1000"}
        )
        self.assertEqual("rust", response["source"])
        self.assertEqual({"a": 1, "b": 2}, response["snapshot"])

    def test_absent_core_has_no_fallback(self) -> None:
        with patch("importlib.import_module", side_effect=ImportError("missing")):
            with self.assertRaises(CoreUnavailableError):
                CoreBridge.load()

    def test_delegates_intent_materialization_to_compiled_contract(self) -> None:
        response = CoreBridge(fake_core()).materialize_order_intent(
            snapshot={"decision_id": "d", "as_of": "2025-01-01T21:00:00Z"},
            release={"release_id": "r"},
            risk_decision={"decision_id": "d", "disposition": "approved"},
            plan={"plan_id": "p", "symbol": "SPY"},
            quote={
                "symbol": "SPY",
                "provider_at": "2025-01-01T21:00:01Z",
                "received_at": "2025-01-01T21:00:02Z",
            },
        )
        self.assertEqual("rust", response["source"])
        self.assertEqual("p", response["plan"]["plan_id"])
        self.assertEqual("2025-01-01T21:00:01Z", response["quote"]["provider_at"])

    def test_core_without_materialization_contract_is_incompatible(self) -> None:
        module = fake_core()
        delattr(module, "materialize_order_intent")
        with self.assertRaises(CoreUnavailableError):
            CoreBridge(module)

    def test_core_errors_and_invalid_output_fail_closed(self) -> None:
        module = fake_core()
        setattr(
            module,
            "backtest",
            lambda request: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        with self.assertRaises(CoreInvocationError):
            CoreBridge(module).backtest({"request": 1})
        setattr(module, "backtest", lambda request: "NaN")
        with self.assertRaises(CoreProtocolError):
            CoreBridge(module).backtest({"request": 1})


if __name__ == "__main__":
    unittest.main()
