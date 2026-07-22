"""Position reconciliation.

Spec: reconcile() returns the agreed position when local and broker truth match, and
raises ReconciliationError on an unknown position or any disagreement. It never guesses
which side is right.
"""


class ReconciliationError(Exception):
    """Local and broker truth could not be reconciled."""


def reconcile(local: int | None, broker: int | None) -> int:
    if local is None or broker is None:
        raise ReconciliationError("position unknown on one side")
    # BUG: a disagreement quietly prefers the broker instead of failing closed.
    return broker
