from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from typing import Callable, cast


HeartbeatPolicy = Callable[[float, int, float], bool]


def _fake_client(_service: str) -> object:
    return object()


def load_policy() -> HeartbeatPolicy:
    fake_boto3 = types.ModuleType("boto3")
    setattr(fake_boto3, "client", _fake_client)
    sys.modules["boto3"] = fake_boto3
    path = (
        Path(__file__).resolve().parents[2]
        / "infra"
        / "terraform"
        / "lambda"
        / "deadman.py"
    )
    spec = importlib.util.spec_from_file_location("wasp2_deadman", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("dead-man module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(HeartbeatPolicy, getattr(module, "heartbeat_is_healthy"))


class DeadmanPolicyTests(unittest.TestCase):

    def test_accepts_only_current_healthy_evidence(self) -> None:
        policy = load_policy()
        self.assertTrue(policy(0, 420, 1))
        self.assertTrue(policy(420, 420, 1))
        self.assertFalse(policy(-0.001, 420, 1))
        self.assertFalse(policy(420.001, 420, 1))
        self.assertFalse(policy(1, 420, 0))


if __name__ == "__main__":
    unittest.main()
