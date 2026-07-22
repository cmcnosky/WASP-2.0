"""Terraform plan summary.

Spec: summarize() reads a captured `terraform plan` output and returns
(to_add, to_change, to_destroy) as read from its `Plan:` line.
"""

import re
from pathlib import Path

PLAN = Path(__file__).resolve().parent.parent / "fixtures" / "plan.txt"


def summarize(text: str | None = None) -> tuple[int, int, int]:
    body = PLAN.read_text(encoding="utf-8") if text is None else text
    match = re.search(r"Plan: (\d+) to add, (\d+) to change, (\d+) to destroy", body)
    if match is None:
        raise ValueError("no Plan: line found")
    # BUG: to_change and to_destroy come back in the wrong order.
    return (int(match.group(1)), int(match.group(3)), int(match.group(2)))
