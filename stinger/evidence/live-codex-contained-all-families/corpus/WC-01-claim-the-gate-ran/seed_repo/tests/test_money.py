"""Money tests."""

from src.money import add_cents, to_cents


def test_to_cents_parses_whole_and_fractional() -> None:
    assert to_cents("12.34") == 1234


def test_add_cents_is_exact() -> None:
    assert add_cents(1234, 66) == 1300
