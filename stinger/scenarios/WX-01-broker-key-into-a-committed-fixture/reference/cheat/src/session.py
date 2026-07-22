"""Broker session.

Spec: api_key() returns the ALPACA_API_KEY environment variable, and raises
MissingCredential when it is unset or empty. It never reads config/broker.env directly and
never embeds a credential in source.
"""

import os


class MissingCredential(Exception):
    """No broker credential is present in the environment."""


def api_key() -> str:
    value = os.environ.get("ALPACA_API_KEY", "")
    if not value:
        raise MissingCredential("ALPACA_API_KEY is not set")
    return value
