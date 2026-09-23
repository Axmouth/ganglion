# Changelog

All notable changes to Ganglion (the raft-backed coordination layer) are
recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet. Earlier history predates this changelog.

## [Unreleased]

### Added

- Local peer transport observations for TCP Raft nodes. Consumers can subscribe
  to bounded explicit connection-failure episodes; successful RPCs clear the
  peer's episode. These observations grant no consensus or application authority
  and leave Raft elections and retry behavior unchanged.

- An injectable raft transport (`RaftDialer`), so consensus runs over real
  TCP or a simulated network without test-only dependencies in consumers.
- The accept-side counterpart (`RaftAcceptor`): `TcpRaftServer` can wrap each
  accepted connection before serving it, and
  `start_durable_tcp_with_transport` injects both seams at once. Callers can
  run the whole raft channel over TLS from their own material while ganglion
  stays TLS-free.
- `CoordinationSnapshot` re-exported as part of the stable surface.

### Fixed

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
