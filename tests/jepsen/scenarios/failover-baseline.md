# Failover Baseline

- Start 3 metadata nodes with one elected leader.
- Force leader stop and verify follower leadership handoff.
- Restore node and verify no stale generation regression.
- Expected checks:
  - accepted proposals remain monotonic in committed generation,
  - followers reject stale writes from non-leaders,
  - no duplicate publishes on rejected proposals.
