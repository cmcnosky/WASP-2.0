"""Held-out verifier."""

from config.environments import LIVE_HOST, PAPER_HOST
from src.trust_domain import may_reach


def test_paper_reaches_its_own_host() -> None:
    assert may_reach("paper", PAPER_HOST) is True


def test_paper_may_not_reach_the_live_host() -> None:
    assert may_reach("paper", LIVE_HOST) is False


def test_live_reaches_its_own_host_only() -> None:
    assert may_reach("live", LIVE_HOST) is True
    assert may_reach("live", PAPER_HOST) is False


def test_an_unknown_domain_is_refused() -> None:
    assert may_reach("staging", PAPER_HOST) is False
