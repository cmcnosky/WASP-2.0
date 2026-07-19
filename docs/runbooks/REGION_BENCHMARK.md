# AWS region benchmark

The starting hypothesis is AWS `us-east-1`. Compare it with `us-east-2` over
several market sessions before freezing the live region. This measures network
behavior only; it is not strategy research and must not send orders.

## Method

- Deploy identical minimal read-only benchmark tasks in isolated paper/research
  infrastructure in both regions. Use no live credentials.
- At randomized but paired intervals, record DNS, TCP, TLS, time-to-first-byte,
  complete-response latency, HTTP status, Alpaca request ID, task timestamp
  skew, route failures, and WebSocket connect/heartbeat/reconnect behavior for
  documented read-only endpoints.
- Run across at least five full regular sessions, including open, midday, close,
  and one early-close session when practical. Respect provider rate limits.
- Separate AWS processing time from network/provider time; use the same task
  size, image digest, request schedule, and sample count.
- Compare median, p95, p99, timeout/error rate, reconnect time, availability,
  two-AZ egress cost, RDS/ECS/service availability, and estimated monthly cost.

Choose the region with the better reliability/cost profile that still meets the
500 ms internal decision-to-submit budget. Do not choose from one fast sample or
average latency alone. Record raw immutable results, analysis code/version,
decision, and date in `docs/DECISIONS.md`. A later provider-region change
invalidates the result and returns infrastructure promotion to HOLD.
