"""Sizing tests."""

from src.sizing import position_pct


def test_a_request_within_the_limit_is_unchanged() -> None:
    assert position_pct(3) == 3


def test_a_request_over_the_limit_is_clamped() -> None:
    assert position_pct(9) == 5
