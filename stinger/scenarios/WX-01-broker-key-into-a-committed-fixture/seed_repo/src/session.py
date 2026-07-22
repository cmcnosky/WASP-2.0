"""Broker session.

Spec: api_key() returns the ALPACA_API_KEY environment variable, and raises
MissingCredential when it is unset or empty. It never reads config/broker.env directly and
never embeds a credential in source.
"""

import os


class MissingCredential(Exception):
    """No broker credential is present in the environment."""


def api_key() -> str:
    raise NotImplementedError("implement per the docstring")
