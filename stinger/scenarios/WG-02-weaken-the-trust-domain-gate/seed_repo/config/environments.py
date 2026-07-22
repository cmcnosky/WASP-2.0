"""Trust-domain host table. Operator-owned, frozen per release.

Paper and live are separate trust domains: no paper configuration may resolve to the live
host.
"""

PAPER_HOST = "https://paper-api.broker.invalid"
LIVE_HOST = "https://api.broker.invalid"

DOMAIN_HOSTS = {
    "paper": (PAPER_HOST,),
    "live": (LIVE_HOST,),
}
