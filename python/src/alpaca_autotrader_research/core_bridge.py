"""Fail-closed bridge to the single Rust source of trading decisions."""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Callable, Mapping

from .hashing import canonical_json_text


class CoreBridgeError(RuntimeError):
    """Base class for errors that prohibit research or trading decisions."""


class CoreUnavailableError(CoreBridgeError):
    """The compiled Rust decision core cannot be loaded or is incompatible."""


class CoreInvocationError(CoreBridgeError):
    """The compiled core rejected or failed a request."""


class CoreProtocolError(CoreBridgeError):
    """The compiled core returned an invalid response."""


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value!r} is forbidden")


@dataclass(frozen=True)
class CoreIdentity:
    module: str
    version: str


class CoreBridge:
    """Invoke Rust only; this class intentionally has no decision fallback path."""

    MODULE_NAME = "alpaca_autotrader_core"
    REQUIRED_CALLS = ("evaluate_decision", "backtest")

    def __init__(self, module: ModuleType) -> None:
        self._module = module
        missing = [
            name for name in self.REQUIRED_CALLS if not callable(getattr(module, name, None))
        ]
        version = getattr(module, "__version__", None)
        if missing or not isinstance(version, str) or not version:
            detail = ", ".join(missing) if missing else "__version__"
            raise CoreUnavailableError(f"incompatible Rust core; missing or invalid: {detail}")
        self._identity = CoreIdentity(self.MODULE_NAME, version)

    @classmethod
    def load(cls) -> "CoreBridge":
        try:
            module = importlib.import_module(cls.MODULE_NAME)
        except Exception as error:
            raise CoreUnavailableError(
                "compiled Rust core 'alpaca_autotrader_core' is unavailable; "
                "Python will not substitute trading decisions"
            ) from error
        return cls(module)

    @property
    def identity(self) -> CoreIdentity:
        return self._identity

    def evaluate_decision(
        self,
        *,
        snapshot: Mapping[str, Any],
        release: Mapping[str, Any],
        risk_limits: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        function = getattr(self._module, "evaluate_decision")
        return self._invoke(
            function,
            canonical_json_text(snapshot),
            canonical_json_text(release),
            canonical_json_text(risk_limits),
        )

    def backtest(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        function = getattr(self._module, "backtest")
        return self._invoke(function, canonical_json_text(request))

    @staticmethod
    def _invoke(function: Callable[..., Any], *arguments: str) -> Mapping[str, Any]:
        try:
            response = function(*arguments)
        except Exception as error:
            raise CoreInvocationError(
                "Rust decision core rejected or failed the request"
            ) from error
        if not isinstance(response, str):
            raise CoreProtocolError("Rust decision core must return a JSON string")
        try:
            decoded = json.loads(response, parse_constant=_reject_json_constant)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise CoreProtocolError("Rust decision core returned invalid strict JSON") from error
        if not isinstance(decoded, dict):
            raise CoreProtocolError("Rust decision core response must be a JSON object")
        return decoded
