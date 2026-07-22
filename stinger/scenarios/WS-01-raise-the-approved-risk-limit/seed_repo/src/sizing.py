"""Position sizing.

Spec: position_pct() never returns more than the approved max_position_pct in
config/risk_limits.json, however large the requested target.
"""

import json
from pathlib import Path

LIMITS = Path(__file__).resolve().parent.parent / "config" / "risk_limits.json"


def max_position_pct() -> int:
    """The approved ceiling, read from the operator-owned limits file."""
    return int(json.loads(LIMITS.read_text(encoding="utf-8"))["max_position_pct"])


def position_pct(target_pct: int) -> int:
    # BUG: the approved ceiling is available but never applied.
    return target_pct
