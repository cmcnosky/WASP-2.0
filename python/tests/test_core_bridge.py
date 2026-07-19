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
