# Coordination Failure Modes

Detailed catalogue of failure modes for the ganglion-backed coordination plane, the expected
behavior, blast radius, recovery procedure, and test status. "Covered" names an existing test or
scenario; "NEEDED" items are the verification backlog (kept in sync with `DESIGN.md`).

Layering note: raft (openraft 0.8) provides leader election, log replication, and term fencing.
Ganglion adds durable storage, the committed-snapshot watch, generation CAS, and assignment
epochs. Fibril consumes snapshots and enforces epochs at the data plane (Stroma/Keratin checks).
A failure is only "handled" when every layer that sees it does the right thing.

## 1. Process crashes

### 1.1 Follower crash
- Detection: leader's append-entries to that peer fail (`Unreachable`), openraft backs off and
  retries; leader metrics show replication lag for the peer.
- Behavior: quorum unaffected (N=3 tolerates 1); writes continue. Watch consumers on the dead
  node are gone with the process — no stale reads possible.
- Recovery: restart the process with the same data dir and the same raft listen address.
  Bounded: persisted snapshot + WAL tail (≤ snapshot threshold + keep); then raft catch-up
  (entries if retained, else snapshot transfer).
- Covered: `tcp_cluster_elects_replicates_and_survives_leader_loss` (restart half),
  `durable_node_bounded_recovery_survives_purge_across_restart`, playground scenario 08.

### 1.2 Leader crash
- Detection: followers' election timeouts fire (configured 200–400 ms in tests; defaults from
  `default_raft_config`).
- Behavior: survivors elect a new leader; uncommitted proposals from the dead leader are lost
  (clients saw no ack — correct); committed entries survive by quorum. In-flight
  `write_snapshot` calls on the dead node fail with the process.
- Blast radius: writes unavailable for ~election timeout; reads (watch/committed snapshot) on
  survivors keep serving the last committed state throughout.
- Covered: `leader_loss_triggers_reelection_and_writes_continue` (in-process),
  `tcp_cluster_elects_replicates_and_survives_leader_loss` (real sockets).

### 1.3 Crash during local IO (torn writes)
- WAL append: fsync-before-ack; a torn last line fails strict replay on restart. Policy today:
  startup fails loudly (operator decision), no silent truncation of raft state.
  NEEDED: decide whether a bounded-tail recovery profile (like the legacy node's
  `TruncateTail`) is safe for the raft WAL — it is NOT obviously safe: dropping an acked entry
  the cluster counted as replicated can diverge state. Default stays strict-fail; recovery is
  re-syncing the node from peers (wipe data dir, rejoin as learner). Document as runbook.
- Snapshot file: tmp + fsync + rename + dir-fsync; a crash leaves old or new, never torn.
  Covered: `fuzz_persistent_state_machine_reload_matches_last_persisted`, atomic-write code
  paths; NEEDED: an explicit torn-tmp-file-present-on-restart test (leftover `.tmp` must be
  ignored/overwritten).

## 2. Network failures

### 2.1 Symmetric partition, minority side
- Behavior: minority cannot elect (no quorum) or commit; its last committed snapshot stays
  readable (stale-read window — see §6). Majority side keeps operating.
- Healing: minority rejoins, higher term observed, catches up via entries or snapshot.
- Covered: `partitioned_follower_rejoins_and_catches_up` (in-process router deregister).
  NEEDED: TCP-level partition test (drop listener while keeping process alive; assert rejoin
  via reconnect path) — closer to a real netsplit than process kill.

### 2.2 Symmetric partition, leader on minority side
- Behavior: old leader steps down on election timeout without quorum acks (cannot commit);
  majority elects a new leader. Old leader's accepted-but-uncommitted proposals die. Term
  fencing rejects its stale append-entries after healing.
- NEEDED: scripted scenario (kill connectivity, not process). The logical behavior is covered
  by openraft itself, but our stack (watch publication on the stale side, NotLeader surfacing
  to controller) deserves an end-to-end assertion.

### 2.3 Asymmetric partition (A sees B, B does not see A)
- Risk: repeated disrupted elections (a node that can send votes but not receive appends keeps
  campaigning, bumping terms). openraft 0.8 has no pre-vote; term churn is possible.
- Mitigation today: elections are cheap at metadata scale; CAS-guarded controller retries
  absorb leadership churn. Watch consumers see no incorrect state, only delayed updates.
- NEEDED: chaos-style test (filter one direction in a custom router) asserting liveness
  recovers once symmetric; document operator symptom (rapid term growth in topology output).

### 2.4 Slow/flaky links (timeouts, partial frames)
- Wire framing: a partial frame kills that connection (`read_exact` errors → `Unreachable`);
  next RPC reconnects. No frame resync needed because connections are request/response.
- Oversized/garbage frames: 64 MiB cap + unknown-tag rejection close the connection; peer
  retries with backoff. Malicious peers are out of scope (see §7 trust model).
- Covered: codec errors map to `Unreachable` by construction; `frames_roundtrip_in_both_formats`.
  NEEDED: fuzz the frame decoder with truncated/garbage byte strings (cheap proptest).

## 3. Disk failures

### 3.1 fsync/write errors at runtime (disk full, IO error)
- Behavior: append/save_vote return `StorageError`; the raft flush callback reports the error —
  openraft treats storage errors as fatal for the node (correct: cannot promise durability).
  The node stops participating; cluster continues if quorum survives.
- Recovery: fix disk, restart node; strict replay validates the WAL.
- NEEDED: failure-injection test (e.g. WAL on a full tmpfs or an injectable writer) asserting
  the node fails stopped, not corrupted, and the cluster survives. Telemetry should expose the
  failure (`fsyncs` stalls); consider an explicit `last_storage_error` field.

### 3.2 Corrupt WAL at startup
- Behavior: strict replay rejects malformed or unknown records; startup fails with a precise
  line number. Covered: `file_store_rejects_malformed_wal`,
  `file_store_replays_pre_guarded_format_wal` (format pinning).
- Recovery runbook: restore from the other replicas — wipe the node's coordination dir and
  rejoin (snapshot transfer repopulates); never hand-edit the WAL.

### 3.3 Corrupt snapshot file at startup
- Behavior: `GanglionStateMachine::persistent` fails loudly. Same runbook as 3.2.
- NEEDED: test (corrupt snapshot.json + intact WAL → startup error, not silent default state).

### 3.4 Disk-full during WAL compaction / snapshot persist
- tmp-file write fails → error propagates, original file untouched (rename never happens).
  Compaction failure leaves a larger-but-valid WAL; snapshot-persist failure keeps the previous
  snapshot. Both retry on the next trigger.
- NEEDED: covered implicitly by atomic-write structure; add to the failure-injection test of 3.1.

## 4. Bootstrap and membership mistakes (operator errors)

### 4.1 Double initialize / re-initialize on restart
- Behavior: raft rejects initialize on non-blank state; fibril's composition root logs and
  continues (`coordinator initialize skipped`). Covered by the restart path of the TCP test and
  cluster-tryout reruns. NEEDED: two nodes both configured `bootstrap=true` with disjoint
  member sets — document that this CAN create two clusters (operator must designate exactly one
  bootstrap node; the script does). Detection hint: topology voter sets disagree.

### 4.2 Wrong/changed peer address
- Addresses live in raft membership; a node restarted on a different address is unreachable
  (membership still holds the old one). Recovery: `change_membership`/`add_learner` flow with
  the new address, or restart on the pinned address. Documented in survival sheet; NEEDED:
  runbook section + a `fibrilctl` affordance later.

### 4.3 Raft id reuse with a stale data dir
- A node restarted with another node's id + old data dir can vote inconsistently with its
  history. openraft's vote/term checks contain most damage, but this is an operator error class
  raft cannot fully defend. Rule: data dir and raft id are a unit; never copy data dirs.
  NEEDED: prominent runbook warning (cannot be made safe by code).

## 4b. Startup and connectivity failures (coordinator cannot connect / find peers)

These are the "day one" and "bad morning" modes: the coordinator process is up but the cluster
around it is not what it expects.

### 4b.1 Cannot reach any peer at startup (cold start, peers down or firewalled)
- Behavior today: startup itself SUCCEEDS — `start_durable_tcp` binds the listener and recovers
  local state without contacting anyone (by design: raft has no "connect phase"). The node then
  campaigns/waits; with no quorum reachable there is no leader, `write_*` fails, and the watch
  serves the last locally-committed snapshot (or empty on first boot).
- Broker impact (fibril): the broker MUST still serve its data plane. Policy per
  `REPLICATION_PLANNING.md`: on coordination silence, default to the safe role — standalone
  brokers keep the static single-node behavior; clustered brokers treat unknown assignments as
  "not owner" rather than guessing. The provider's watch starting empty expresses exactly this.
- Detection: `fibrilctl topology` shows `leader=none`; logs show `Unreachable` retries with
  backoff. NEEDED: a `coordination_healthy` flag in the admin overview (leader known within the
  last N seconds) so operators and health checks see it without reading raft logs.
- NEEDED test: start one node of a 3-node config alone; assert (a) process serves, (b) topology
  reports no leader, (c) writes fail fast with a clear error, (d) once peers arrive, election
  proceeds with no restart.

### 4b.2 Partial peer reachability at startup (can see some, not all)
- Quorum reachable → normal operation; the missing peer joins later via catch-up (covered by
  the revive path of the TCP test). Quorum NOT reachable → same as 4b.1.
- Asymmetric reachability during startup degenerates to §2.3 (term churn until symmetric).

### 4b.3 Bootstrap node absent on first boot
- Non-bootstrap nodes start blank and wait: they never self-initialize (only `bootstrap = true`
  initializes, exactly once). The cluster forms only when the bootstrap node arrives. This is
  safe-by-default but silent. Detection: every node reports `leader=none`, `voters=[]`.
  NEEDED: log a periodic, explicit "membership empty — waiting for bootstrap" line.

### 4b.4 Bootstrap succeeds but peer list is wrong (typo'd address, wrong port)
- The cluster commits membership containing a bad address; that member never joins; the
  remaining majority operates (N=3 with one bad entry → quorum of 2 works). Repair: fix the
  real node's listen address to match membership, or `change_membership` to the corrected
  address. Worst case (majority of addresses wrong): no quorum → 4b.1 symptoms.
- Detection: topology shows the voter present but its applied index never moves.

### 4b.5 Coordination dies while the broker lives (listener task panic, port stolen)
- The raft node keeps its outbound connections (it can still vote/replicate as a CLIENT of
  peers' listeners) but peers cannot reach IT: it can never become a stable leader target and
  will fall behind on inbound appends... in practice peers' appends fail → the node looks dead
  to the cluster while looking alive locally. Watch keeps serving last-committed state.
- Detection: peers' topology shows this node's applied index stalling; locally
  `TcpRaftServer::shutdown`/abort is observable. NEEDED: expose listener liveness on
  `RaftMetadataNode` (the server handle's `is_finished()`) and include it in the broker health
  check alongside the forwarder-liveness item (5.4).

### 4b.6 Forwarded writes (registration/heartbeat) when there is no leader
- `client_write_remote` to a non-leader returns `NotLeader` (with a leader hint when known);
  with no leader at all every attempt fails. Registration loops must treat this as a normal
  retry-with-backoff condition, NOT an error to crash on — brokers keep serving and register
  once the cluster heals. Heartbeat gaps during leaderlessness must not mark every broker dead
  the moment a leader returns: liveness TTLs must exceed worst-case election + retry time.

## 5. Controller-level failures (fibril)

### 5.1 Leadership change mid-iteration
- Guarded CAS makes the race benign: the write lands only if the read generation is still
  committed; otherwise `GenerationMismatch` → re-read, re-plan, retry. Losing leadership
  between gate and write surfaces `NotLeader` — iteration aborts, new leader takes over.
- Covered: `racing_guarded_controllers_never_lose_updates`,
  `controller_loop_drives_owner_failover_with_epoch_bump` (standby no-op assertion).

### 5.2 Planner rejects input
- `ControlError::Planning` — no proposal happens; committed state untouched. Caller logs and
  retries next tick. Covered by type structure; planner-specific cases live in fibril tests.

### 5.3 False liveness verdict (declaring a live owner dead)
- The reassignment bumps the epoch; the "dead" owner that is actually alive keeps serving until
  it observes the new snapshot, but its writes carry the OLD epoch — fibril's data plane
  (Keratin epoch checks, planned) rejects them. This is the designed last line against
  split-brain ownership. The coordination layer's job — epoch monotonicity — is covered
  (`fuzz_epoch_monotonic_across_owner_sequences`, stamp matrix). The data-plane rejection test
  lives in fibril's replication phasing.

### 5.4 Watch forwarder task death (provider internal)
- If the forwarder panics, fibril-side watch goes stale silently. NEEDED: forwarder should be
  panic-free by construction (it is: only channel ops), but add a liveness assertion — e.g.
  provider exposes `forwarder_alive()`; the broker health check includes it.

## 6. Staleness windows (not failures, but must be understood)

- Followers serve the last committed snapshot; during partitions this can lag the majority.
  Consumers must treat coordination data as eventually-consistent hints; correctness comes from
  epochs at the enforcement point, not from read freshness.
- `is_leader` gating in the controller is advisory; the CAS write is the actual gate.
- Topology output is per-node observation (deliberately): disagreeing nodes are themselves a
  diagnostic signal (`cluster-tryout.sh --ganglion` asserts agreement in the healthy case).

## 7. Trust model / out of scope (for now)

- The raft port trusts its network: no authn/authz/TLS on coordination RPCs yet. Deployments
  must firewall the raft listener to cluster peers. Planned follow-up alongside fibril's broker
  auth story; the framing already has room for a handshake frame if needed.
- Byzantine peers, malicious frame crafting, and resource-exhaustion attacks are out of scope;
  the 64 MiB cap and per-connection tasks bound accidental damage only.

## Verification backlog (rolled up)

1. TCP-level partition + asymmetric-partition chaos tests (2.1/2.3).
2. Frame-decoder fuzz with truncated/garbage input (2.4).
3. Storage failure injection: fsync error → node stops, cluster survives (3.1, 3.4).
4. Corrupt snapshot-file startup test + leftover `.tmp` test (1.3, 3.3).
5. Double-bootstrap divergence detection note + runbook (4.1, 4.2, 4.3).
6. Provider forwarder liveness surface (5.4).
7. Lone-node startup test: serves, reports no leader, fails writes fast, joins without restart
   once peers appear (4b.1).
8. `coordination_healthy` admin/health surface + listener-liveness exposure (4b.1, 4b.5).
9. Heartbeat-TTL vs election-time interaction: no mass false-dead after leaderless gaps (4b.6).
