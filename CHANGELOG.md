# Changelog

All notable changes to Ganglion (the raft-backed coordination layer) are
recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet. Earlier history predates this changelog.

## [Unreleased]

### Added

- `RaftMetadataNode::watch_leader` reports this node's view of the current
  leader and wakes only when it changes.

- Optional idle Raft connection monitoring through `set_monitor_idle_peer_connections`.
  The observer owns the idle stream, returns it before the next RPC, and stops on
  connection cancellation/drop. Explicit closure triggers one bounded reconnect
  probe; timeouts are not explicit peer-loss evidence.

- Local peer transport observations for TCP Raft nodes. Consumers can subscribe
  to bounded explicit connection-failure episodes; successful RPCs clear the
  peer's episode. These observations grant no consensus or application authority
  and leave Raft election rules unchanged.

- An injectable raft transport (`RaftDialer`), so consensus runs over real
  TCP or a simulated network without test-only dependencies in consumers.
- The accept-side counterpart (`RaftAcceptor`): `TcpRaftServer` can wrap each
  accepted connection before serving it, and
  `start_durable_tcp_with_transport` injects both seams at once. Callers can
  run the whole raft channel over TLS from their own material while ganglion
  stays TLS-free.
- `CoordinationSnapshot` re-exported as part of the stable surface.

### Changed

- The file raft log store writes and syncs on its own thread. An append
  returns once its entries are indexed and reports the flush to openraft when
  the sync finishes, so the raft core no longer holds a runtime worker for a
  disk sync, which took 34-43 ms under heavy disk load. Votes and truncations
  wait for their sync without blocking a worker, appends queued together share
  one sync, and a failed write fails every later one.

- The vendored OpenRaft logs vote and replication RPCs that fail with
  `Unreachable` at warn instead of error. A stopped peer is routine during
  failover. Timeouts and other RPC errors still log at error.

- Explicit Raft RPC disconnects are verified by one immediate retry with a 200 ms
  deadline. A successful exchange clears suspicion; failed/cancelled exchanges
  discard their socket so late replies cannot leak into another request.

- Real TCP Raft sockets and forwarded metadata writes enable `TCP_NODELAY`,
  including accepted sockets before optional TLS wrapping. Small control frames
  can be sent promptly without waiting for Nagle buffering; consensus and
  durability requirements are unchanged.

### Fixed

- Jepsen scenarios 01-05 always run their Rust smoke checks. With `clojure` on
  `PATH` they used to print a placeholder and report a pass without checking
  anything.

- A coordination listener that cannot bind reports a configuration error that
  names the listener, with a hint when the port is already in use, instead of a
  storage error.

- Persistent Raft connections now redial after a remote fatal response, so a
  stopped embedded core cannot trap a peer on its old socket after restart.
  Cancelled or failed requests also discard their socket, preventing a late reply
  from being read as the next request's response. Deterministic transport tests
  cover both cases.

- Replication no longer panics on an empty log read. A pinned OpenRaft 0.9.25
  source copy retries the original request without acknowledging unread entries,
  correcting both the 0.9.24 unwrap and the 0.9.25 index-zero heartbeat fallback.
  A subprocess regression catches background panics, verifies catch-up after
  transient empty reads, and verifies old streams stop on step-down during
  persistent retries. See
  `vendor/openraft/PATCHES.md` for provenance and upstream replacement conditions.

- Poisoned locks in the openraft adapters are recovered instead of panicking,
  so one panicked holder cannot wedge the coordination layer.
