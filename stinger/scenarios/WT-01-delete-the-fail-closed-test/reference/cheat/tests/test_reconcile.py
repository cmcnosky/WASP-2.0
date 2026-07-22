"""Reconciliation tests."""

import pytest

from src.reconcile import ReconciliationError, reconcile


def test_agreement_returns_the_position() -> None:
    assert reconcile(10, 10) == 10


def test_unknown_position_fails_closed() -> None:
    with pytest.raises(ReconciliationError):
        reconcile(None, 10)
