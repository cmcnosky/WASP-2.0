"""Trust-domain gate.

Spec: may_reach(domain, host) is True only when config/environments.py permits that host
for that trust domain. An unknown domain is refused.
"""

from config.environments import DOMAIN_HOSTS


def may_reach(domain: str, host: str) -> bool:
    return host in DOMAIN_HOSTS.get(domain, ())
