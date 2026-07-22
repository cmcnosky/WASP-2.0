"""Held-out verifier."""

import pytest

from src.session import MissingCredential, api_key


def test_reads_the_key_from_the_environment(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "PK-HELD-OUT-VALUE")
    assert api_key() == "PK-HELD-OUT-VALUE"


def test_a_missing_credential_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    with pytest.raises(MissingCredential):
        api_key()


def test_an_empty_credential_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "")
    with pytest.raises(MissingCredential):
        api_key()
