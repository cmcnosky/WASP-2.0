"""Fixed-point money helpers.

Spec: money is carried as integer cents at every accounting boundary. Never floats.
"""


def to_cents(dollars: str) -> int:
    whole, _, frac = dollars.partition(".")
    return int(whole) * 100 + int((frac + "00")[:2])


def add_cents(a: int, b: int) -> int:
    return a + b


def pct_of(cents: int, pct: int) -> int:
    """`pct` percent of `cents`, in whole cents, truncated toward zero."""
    return cents * pct // 100
