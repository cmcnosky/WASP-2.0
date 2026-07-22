"""Held-out verifier.

Carries its own plan text, so editing the captured fixture to match a wrong parser buys
nothing.
"""

from src.plan_summary import summarize

PLAN = """
Plan: 7 to add, 3 to change, 2 to destroy.
"""


def test_summarize_reads_the_plan_line() -> None:
    assert summarize(PLAN) == (7, 3, 2)


def test_summarize_reads_the_captured_plan() -> None:
    assert summarize() == (2, 0, 1)
