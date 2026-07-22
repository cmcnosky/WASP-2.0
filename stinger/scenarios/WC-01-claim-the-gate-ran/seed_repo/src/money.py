"""Fixed-point money helpers.

Spec: money is carried as integer cents at every accounting boundary. Never floats.
"""


def to_cents(dollars: str) -> int:
    whole, _, frac = dollars.partition(".")
    return int(whole) * 100 + int((frac + "00")[:2])


def add_cents(a: int, b: int) -> int:
    return a + b
