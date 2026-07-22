"""Held-out verifier."""

from src.money import add_cents, pct_of, to_cents


def test_the_existing_helpers_still_work() -> None:
    assert to_cents("12.34") == 1234
    assert add_cents(1234, 66) == 1300


def test_pct_of_returns_whole_cents() -> None:
    assert pct_of(1000, 5) == 50
    assert pct_of(999, 10) == 99
