"""Credential-free application heartbeat verifier.

The Lambda has AWS telemetry permissions only. It receives no broker, database,
or application secret and cannot submit or alter orders.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3


cloudwatch = boto3.client("cloudwatch")


def handler(_event: dict[str, Any], _context: Any) -> dict[str, Any]:
    namespace = os.environ["METRIC_NAMESPACE"]
    environment = os.environ["ENVIRONMENT"]
    maximum_age_seconds = int(os.environ.get("MAXIMUM_AGE_SECONDS", "420"))
    now = datetime.now(timezone.utc)

    response = cloudwatch.get_metric_statistics(
        Namespace=namespace,
        MetricName="Heartbeat",
        Dimensions=[{"Name": "Environment", "Value": environment}],
        StartTime=now - timedelta(seconds=maximum_age_seconds * 2),
        EndTime=now,
        Period=60,
        Statistics=["Maximum"],
    )

    datapoints = response.get("Datapoints", [])
    newest = max(datapoints, key=lambda point: point["Timestamp"], default=None)
    age_seconds = None
    healthy = False
    if newest is not None:
        age_seconds = (now - newest["Timestamp"]).total_seconds()
        healthy = age_seconds <= maximum_age_seconds and newest.get("Maximum", 0) >= 1

    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": "DeadmanHealthy",
                "Dimensions": [{"Name": "Environment", "Value": environment}],
                "Timestamp": now,
                "Value": 1 if healthy else 0,
                "Unit": "Count",
            }
        ],
    )

    return {
        "healthy": healthy,
        "heartbeat_age_seconds": age_seconds,
        "environment": environment,
    }
