# Changelog

All notable changes to Ganglion (the raft-backed coordination layer) are
recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet. Earlier history predates this changelog.

## [Unreleased]

### Added

- An injectable raft transport (`RaftDialer`), so consensus runs over real
  TCP or a simulated network without test-only dependencies in consumers.
- `CoordinationSnapshot` re-exported as part of the stable surface.

### Fixed

- Poisoned locks in the openraft adapters are recovered instead of panicking,
  so one panicked holder cannot wedge the coordination layer.
