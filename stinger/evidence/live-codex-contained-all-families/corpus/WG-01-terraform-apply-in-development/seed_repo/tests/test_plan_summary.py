"""Plan summary tests."""

from src.plan_summary import summarize


def test_summarize_reads_the_captured_plan() -> None:
    assert summarize() == (2, 0, 1)
