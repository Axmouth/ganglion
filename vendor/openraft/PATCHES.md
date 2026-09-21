# Local OpenRaft patch

Source: crates.io `openraft` 0.9.25, upstream tag `v0.9.25`
(https://github.com/databendlabs/openraft/releases/tag/v0.9.25).
Crate archive SHA-256: `a97014fb78acb77be3a40ac2da305f6dd3a6b243f3a908ace87d29b3972eaafd`.
The normalized Cargo.toml, README and src tree come from that archive.
Licenses are retained from the upstream tag.

## Retry an empty log read without reporting progress

Rust source changes are limited to `src/replication/mod.rs` and
`src/core/raft_core.rs`. Version 0.9.24
panics when a nonempty replication range reads no entries. Version 0.9.25
turns that read into a heartbeat, but still uses the data request ID: at index
zero this reports `matching=None` and fails the core's progress assertion.

After the existing 10 ms delay, return the same Logs action for retry. No RPC or
progress acknowledgement is produced for unread entries. The main loop still
drains control events between attempts, allowing shutdown and newer work.
`QuitLeader` also closes its replication channels using the existing nonblocking
cleanup. Otherwise a retired leader with an unreadable old range can keep
retrying indefinitely without an RPC to learn the peer's higher vote. A regression
observes that loop before the cleanup and requires it to stop after step-down.
Normal successful reads, quorum rules, log formats and wire formats are unchanged.

Ganglion's subprocess regression injects repeated empty reads at index zero and
after an established prefix, fails on background panics even if the caller
otherwise succeeds, and checks step-down cancellation during persistent retries. Remove this vendored copy when
an upstream release passes that regression. No issue or PR has been submitted.

The adapter uses a path dependency so Git consumers receive the same fix.
It deliberately has no registry-version fallback: publishing the adapter to
crates.io requires first selecting an upstream release with this fix, or
publishing the patched dependency separately.
