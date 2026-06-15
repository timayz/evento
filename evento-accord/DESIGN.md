# evento-accord — Design

A new evento `Executor` backed by the **Accord** consensus protocol (Apache
Cassandra [CEP-15]), turning evento into a **leaderless, strictly-serializable,
highly-available replicated event store**. Events are replicated across `N`
nodes with a single global serial order and no elected leader, tolerating `f`
node failures where `N = 2f + 1`.

[CEP-15]: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15:+General+Purpose+Transactions

## Goals (decided 2026-06-13)

- **HA replicated event store** — not just cross-aggregate transactions, not a
  simulation. Real replication with strict serializability.
- **Real multi-node cluster** — a real network transport, real nodes.
- **Greenfield** — evento has no networking today; we build the Accord
  `api`-style trait interfaces from scratch.

## Background: what Accord is

Leaderless, timestamp-ordered, dependency-tracking consensus (lineage:
EPaxos → Caesar/Tempo → Accord). Provides strict serializability for
general multi-key transactions, **one round-trip in the conflict-free case**.

Phases: `PreAccept → (Accept) → Commit → Read → Apply`, plus `Recovery`.

- The coordinator picks a proposed timestamp **t0** (a Hybrid Logical Clock
  value: `(micros, logical, node)`), which also names the transaction (`TxnId`).
- **PreAccept** asks the replicas for conflicting dependencies and whether `t0`
  can stand as the execution timestamp. If a **fast-path quorum** agrees with no
  reordering, we skip Accept — one round-trip.
- Otherwise **Accept** (slow path) settles a final execution timestamp
  **t ≥ t0** and the dependency set.
- Replicas execute transactions in **t order**, each waiting on its
  dependencies — the "reorder buffer" that bounds waiting by clock skew +
  latency instead of adding message rounds.

Quorums for `N = 2f + 1`:

| Quorum     | Size            | N=3 (f=1) | N=5 (f=2) |
|------------|-----------------|-----------|-----------|
| Fast path  | `⌊(e + f)/2⌋ + 1` | 3       | 4         |
| Slow / recovery | `f + 1`    | 2         | 3         |

The fast-path quorum is over the **fast-path electorate** of size `e` — a
cluster-agreed subset of the replicas (default `e = N`, where the formula is the
classic `⌈3f/2⌉ + 1`). Shrinking `e` (toward `f + 1`, placed in one region) shrinks
the fast quorum, enabling single-region one-round-trip commits (Phase D).

## How Accord maps onto evento

| Accord concept                          | evento concept                                                       |
|-----------------------------------------|----------------------------------------------------------------------|
| `Key` (ownership / sharding unit)       | `RoutingKey` / aggregate id                                          |
| Transaction = read keys + condition + write | `write(events)` as a **conditional multi-aggregate append** (condition = `version` / `original_version` match → `WriteError::InvalidOriginalVersion`) |
| Execution timestamp `t` (global order)  | the subscription / projection cursor — total order across the cluster |
| `DataStore` (applied state)             | an existing evento backend (`Fjall` / `Sql`) per replica            |
| `Journal` (durable command log)         | new durable log of PreAccept/Accept/Commit/Apply records            |

`Executor::write` coordinates one Accord transaction that conditionally appends
events. `read` / subscriptions / snapshots are served locally from the applied
`DataStore` in execution-timestamp order.

## Crate layering (mirrors `accord-core`'s storage/transport-agnostic split)

```
            ┌──────────────────────────────────────────────┐
            │  Executor impl (`Accord`)  — M5              │
            │  write→coordinate · read/subscribe→DataStore │
            └──────────────────────────────────────────────┘
                                │
            ┌──────────────────────────────────────────────┐
            │  Coordinator (drives a write txn → Apply)    │
            └──────────────────────────────────────────────┘
                                │
            ┌──────────────────────────────────────────────┐
            │  Protocol core (per-txn command state machine)│
            │  PreAccept/Accept/Commit/Read/Apply/Recover  │
            └──────────────────────────────────────────────┘
                                │
   ┌────────────┬───────────┬──────────┬──────────┬──────────────┐
   │ MessageSink│ Topology  │  Clock   │ Journal  │  DataStore   │   ← `api` traits
   │ (transport)│(membership)│ (HLC)   │(durable) │(applied state)│
   └────────────┴───────────┴──────────┴──────────┴──────────────┘
```

The `api` traits are dependency-injected. Each gets two impls: a deterministic
in-process one for the simulation test harness, and a production one (real TCP
transport, static-config membership, Fjall/SQL-backed store).

## Transport

**Length-delimited framed TCP with serde encoding, behind the `MessageSink`
trait** (recommended over gRPC/QUIC) — ✅ implemented in `tcp.rs`:

- Reuses evento's existing `bitcode`/serde serialization — no parallel protobuf
  schema. `Event` is bridged to the wire by a `WireEvent` DTO whose only
  non-trivial field is the bitcode-encoded `Metadata`, so evento-core is
  unmodified. Frames are `bitcode::serialize`d over a `LengthDelimitedCodec`.
- Accord traffic is fire-and-correlate-by-`TxnId`, not RPC request/response, so
  one-way framed messages fit better than gRPC call semantics.
- Per-peer lazy writer tasks connect on demand and reconnect after a drop;
  delivery is best-effort (loss tolerated by quorums/recovery). `serve` runs the
  inbound accept loop, decoding frames into `Envelope`s for a node's inbox.
- Minimal deps (`tokio-util` codec, `bytes`), fully under our control — the same
  `MessageSink` trait still powers the in-memory simulation harness.

The trait isolates the choice; `tonic` stays a swap-in if we later want TLS/codegen.

Membership for the first milestones is **static config** (fixed node-id ↔
address list). Dynamic membership / gossip is deferred to M4.

## A note on the config / metadata layer

Accord itself is only the *transaction* protocol (the data plane). It does **not**
decide membership or epochs — it consumes an already-agreed topology through the
`Topology` trait (modelled on `accord-core`'s `TopologyService`). In real
Cassandra, that agreement is provided by a **separate** subsystem: the cluster
metadata service (CEP-21, Transactional Cluster Metadata — a linearizable
replicated metadata *log*).

`evento-accord` is standalone, with no host metadata service, so the config Paxos
(`ConfigPrepare`/`Promise`/`Accept`/`Accepted`/`Commit`) **decides** each epoch's
layout — an implementation choice, not part of Accord. Per-epoch single-decree
Paxos remains the *decider*, but the decided `(epoch, layout)` entries are now a
durable, replicated **metadata log** (Phase E), closing the original stand-in's
gaps:

- **Log replay / catch-up.** A node tracks the committed log and installs epochs in
  **strict contiguous order** (`MetadataLog`/`apply_log` in `node.rs`): an entry for
  a future epoch is held until every intervening epoch arrives, so a node can never
  skip an epoch. A node that missed epochs (down/partitioned across a `ConfigCommit`)
  converges by pulling the missing run from a peer (`MetadataFetch`/`MetadataEntries`,
  metadata anti-entropy in the recovery sweep), or eagerly when a later `ConfigCommit`
  reveals the gap.
- **Durability.** Acceptor state is persisted **before** any promise/accept reply
  (`Journal::record_acceptor`), and the committed log is persisted
  (`Journal::append_metadata`); `recover_state` restores both, so a restarted acceptor
  keeps its promised ballots (Paxos crash-safety) and re-installs its epoch prefix.
- **Automatic completion.** The recovery sweep finishes any accepted-but-uncommitted
  epoch itself (the automatic equivalent of `recover_topology`).

Remaining gap versus a full CEP-21 service: **no leader** for liveness under
contention (concurrent reconfigurations can still duel on ballots) and no Raft-style
single log stream. Keep this boundary in mind: anything `Config*`/`Metadata*` is the
control plane, everything else is Accord.

## Milestones

- **M0 — Skeleton.** ✅ `api` traits, core types (`NodeId`, `Timestamp`,
  `TxnId`, `Ballot`, `Key`, `Message`, `CommandState`), in-memory `MessageSink`,
  and a fully-implemented + tested Hybrid Logical Clock.
- **M1 — Single-shard happy path.** ✅ PreAccept → fast/slow path → Commit →
  execute; static membership; no failures. The `Replica` state machine
  (`replica.rs`) computes execution timestamps + dependencies and executes in
  `(execute_at, txn)` order; the `Node` actor (`node.rs`) runs the inbox loop
  and coordinates writes; `InMemoryDataStore` (`store.rs`) mirrors evento's
  `(type, id, version)` uniqueness. `tests/cluster.rs` proves strict-serializable
  ordering and one-winner conflict resolution across 3- and 5-node clusters.
  *M1 simplification:* the coordinator waits for **all** nodes each phase (no
  failures); quorum-based progress moves in with recovery (M2).
- **M2 — Recovery + quorum progress.** ✅ Writes now progress on a fast/slow
  quorum with a per-phase timeout (`PHASE_TIMEOUT`), tolerating `f` failures.
  Ballot-fenced recovery (`Node::recover`) lets a live node take over a
  transaction whose coordinator died: it collects `RecoverOk` from a recovery
  quorum and decides by max status — re-commit (Committed/Applied), re-propose
  highest-ballot values (Accepted), or, for PreAccepted-only, apply the
  **superseding-rejects** check (a later conflict that didn't witness the txn
  proves the fast path was impossible, so raise the timestamp; otherwise keep
  `t0`). `Commit` carries events + a `reply_to` so a recoverer collects the
  outcome; an already-applied command answers from its stored result.
  `tests/cluster.rs` adds down-node tolerance, coordinator-crash recovery, and
  idempotent double-recovery. Journal is written on commit/apply.
  *M2 simplification:* journal-replay across process restarts is deferred; a
  recovery quorum that has never witnessed the txn errors rather than
  invalidating it as a no-op.
- **M3 — Multi-shard transactions.** ✅ `ShardedTopology` partitions keys into
  **disjoint** shards, each with its own replica set; the coordinator reaches a
  quorum in **each** touched shard (`plan` + `collect_by_shard` group responses
  by responder shard). Execution is split into **Read → Apply** so a write
  spanning aggregates commits **atomically**: every shard reads its owned keys'
  version condition (`ReadOk`), the coordinator commits only if all hold, then
  all shards `Apply` (append) or all abort. A replica reads/applies only its
  owned events; crucially, its execution **barrier is per-shard** — computed from
  its local conflict graph over owned keys, *not* the coordinator's global
  dependency union (which references shards it never witnessed). `Status::Reading`
  holds a transaction's slot between the read and the decision.
  `tests/shard_cluster.rs` proves shard routing, atomic cross-shard commit, and
  atomic cross-shard abort (a stale aggregate aborts the whole write, leaving the
  other untouched).
- **M4 — Topology changes.** ✅ `DynamicTopology` carries an
  **epoch** and can `install` a new shard layout atomically. A joining node
  **bootstraps**: `begin_join` starts buffering consensus messages; `Node::join`
  fetches a peer's committed state (`SyncRequest` / `SyncData` carrying serialized
  `CommandState`s), imports each command into its conflict graph
  (`Replica::import_applied`) and applies its events, then **replays the buffered
  messages** — so a transaction that commits *during* the join still lands
  (sync-point-style). A node drives an epoch change with `Node::change_topology`,
  which runs **single-decree Paxos** over the current members (`ConfigPrepare` /
  `ConfigPromise` / `ConfigAccept` / `ConfigAccepted` / `ConfigCommit`) so the
  layout decision is durable; `Node::recover_topology` lets any node complete a
  change whose coordinator died after the value was accepted. **Node leave** is
  just installing a smaller layout. `tests/membership.rs` proves: a coordinated
  join where the joiner converges on the global order; optimistic concurrency on
  the joined node; a write committed mid-bootstrap reaching the joiner *only*
  through the replay buffer (via a partition); a node leaving while the smaller
  cluster keeps serving; and a config change surviving the coordinator crashing
  after the value is durable but before commit (another node recovers it).
  **Range movement** composes from these: re-sharding into more shards is a config
  change, and the new shard's nodes bootstrap *only their key range* (the join's
  ownership filter); `tests/resharding.rs` moves a key to a new 3-node shard and
  shows writes route to — and version state is enforced by — its new owners. The
  config-Paxos **acceptor set is the current members** (a node proposing the next
  epoch is still on this one), so reconfiguration keeps working after the founders
  have left (`config_changes_outlive_the_founders` removes a founder majority and
  still reconfigures). *Remaining:* garbage-collecting a moved range from its old
  owners, and multi-shard read routing for the executor.
- **M5 — Executor wiring.** ✅ `AccordExecutor` is a real `evento_core::Executor`:
  **writes** are coordinated through the Accord cluster (mapping the outcome to
  `Ok(())` / `WriteError::InvalidOriginalVersion`); **reads, subscriptions, and
  snapshots** delegate to a local evento backend (`Fjall`) that each replica keeps
  current by applying committed transactions via `ExecutorDataStore` (the bridge
  `DataStore` — `version` reads the aggregate's current version, `apply` appends).
  The coordinator waits for its own local apply (read-your-writes). On a
  **multi-shard** cluster, a single-aggregate read for a key this node does not
  own is **routed** to an owner (`ReadForward` / `ReadReply`, served from the
  owner's backend); broad scans are served locally. `tests/executor.rs`
  (fjall-backed, single shard) proves replication, read-your-writes, and
  cluster-wide optimistic concurrency through the standard evento API;
  `tests/shard_executor.rs` proves read routing on a 2-shard cluster.
- **Transport — framed TCP.** ✅ `tcp.rs`: `TcpTransport` (`MessageSink`) + `serve`
  over real sockets, static `NodeId → SocketAddr` membership, bitcode wire
  encoding. `tests/tcp_cluster.rs` runs a 3-node cluster over localhost TCP
  proving replication + conflict resolution end-to-end.
- **Throughout — deterministic simulation harness** (cf. Accord's
  `accord-maelstrom` + burn tests). Strongly recommended even though production
  is a real cluster: it is how recovery/topology bugs are found.

## Open questions to resolve from `accord-core` source (per milestone, not up front)

- Exact dependency-set computation and how reads execute at timestamp `t`.
- Precise recovery message flow (M2).
- Epoch / bootstrap handling on topology change (M4).
- Multi-shard commit across disjoint replica sets (M3).

## Status

**M0–M3 + M5 complete, production TCP transport, M4 first increment.** Multi-shard
Accord: quorum progress (tolerates `f` failures per shard), ballot-fenced
recovery, atomic cross-shard conditional appends via the Read→Apply split with
**per-shard dependencies**, a real framed-TCP `MessageSink`, `AccordExecutor`
(drop-in `evento_core::Executor`), and an epoch-versioned `DynamicTopology` with
node-join bootstrap with buffer-replay, node leave, Paxos-backed epoch changes
that survive a coordinator crash (acceptor set tracks current membership), and
range movement (re-sharding), and multi-shard executor read routing, plus an opt-in
linearizable-read **read-index** barrier (`NodeConfig.linearizable_reads`), and a
region-favouring **fast-path electorate** (Phase D, including a region-derived
electorate on `DynamicTopology`). **98 tests**
(42 unit, 6 cluster, 5 electorate, 3 multi-shard, 10 membership, 1 resharding, 6 executor,
1 linearizable-stress, 1 shard-executor, 2 TCP, 2 mTLS, 13 simulation, 3 restart,
3 fjall-journal), clippy clean,
stable across repeated runs (the simulation suite is deterministic — see Phase A).
The full M0–M5 roadmap plus elastic membership (M4) is implemented. **External
verification has begun**: an independent Jepsen/Elle harness (`evento-accord/jepsen/`,
self-contained Docker cluster) drives the cluster under partition/kill/pause and checks
strict-serializability — see Phase D. It already found and fixed a real bug (anti-entropy
never converging a node that received a txn's Commit but missed its Apply). Findings:
writes are serializable & atomic; with `--linearizable-reads` reads are linearizable and
the cluster is strict-serializable under quorum-preserving partition (validated to
concurrency 10) and always serializable.

## Production roadmap

The above is a faithful, well-tested **reference implementation**. Hardening it
toward production. **Phases A, B, and C are complete:** the protocol is exercised
by a deterministic, bit-reproducible fault-injection simulation (crash/partition/
restart, with correctness oracles); consensus state is durable (disk-backed
journal, restart recovery), group-commit-batched, and **bounded** (snapshots +
compaction + log truncation); and it is geo-hardened (bounded clock-skew, tunable
config, backpressure, a phi-accrual failure detector, region-aware read routing,
and mutual TLS). Goal: production-ready, geo-distributed, on any evento storage
backend (sql/fjall). Phases, in order:

- **Phase A — Trust the protocol (verification).** ✅
  `tests/simulation.rs` is a seeded fault-injection harness with correctness
  **oracles** (agreement, no double-commit, no lost/phantom commits, no
  split-brain). Four scenarios: minority-replica churn under a live quorum; full
  chaos (writers on random coordinators that crash mid-write while a minority of
  any nodes churn); **network partitions** (the cluster is repeatedly split into a
  live majority {0,1,2} and a stranded minority {3,4}, then healed — a stranded
  minority must never invent a phantom commit, checked by a `minority_never_ahead`
  oracle); and **node restarts** (minority nodes are torn down and rebuilt from
  their durable journal mid-workload, so a restart must never resurrect a stale
  commit and recovery + anti-entropy must catch the node back up). Across all
  seeds, **safety holds** — no split-brain, no double-commit — and after healing
  the cluster **converges**. The harness already paid off: it found two real gaps
  (non-convergence from stalled transactions and from missed transactions), both
  now fixed (see Phase B). ✅ **Deterministic runtime:** the tests run under
  `#[tokio::test(start_paused = true)]` — a single-threaded, virtual-time runtime —
  with a virtual physical-time source injected into the HLC
  (`HybridLogicalClock::with_physical`) and deterministic event ids, so a given
  seed reproduces **exactly** (proven by `simulation_is_bit_reproducible`, which
  runs a seed twice and asserts byte-identical fingerprints). The protocol core was
  already order-independent (every map iteration feeding a decision is sorted or
  keyed; `Replica::stuck` now sorts too). Virtual time also made the suite ~85×
  faster (no real sleeping). *Possible future step:* `madsim` for a contractually
  guaranteed deterministic scheduler + simulated network, if the in-process
  virtual-time guarantee ever proves insufficient.
- **Phase B — Durability & recovery (delivers the pluggable-storage goal).**
  ✅ **Automatic recovery** (`Node::start_recovery` runs a progress sweep that
  takes over any transaction stalled past a timeout — its coordinator presumed
  dead) and ✅ **anti-entropy repair** (the same sweep pulls committed transactions
  a peer has that this node missed, so a healed node converges); together they get
  the chaos scenario to full convergence. ✅ **Restart recovery:** the `Journal`
  records every command transition (`load_all`), and `Node::recover_state` rebuilds
  the `Replica` (status, ballots, decision) and replays committed transactions into
  the data store on startup; in-flight transactions resume via the sweep.
  `tests/restart.rs` proves a node rebuilds from its journal and rejoins — both
  cleanly and *while writes are in flight*. ✅ **Disk-backed journal:**
  `FjallJournal` (bitcode-serialized `CommandState`s in a fjall database) is a
  genuinely durable `Journal`; `tests/fjall_journal.rs` proves records survive a
  full close/reopen (a real process restart). The `Journal` trait stays open, so a
  sql-backed journal is a drop-in alternative. ✅ **Group-commit fsync:** the
  `Journal` trait splits into `stage` (buffer a write) + `flush` (one fsync), and
  the node's inbox loop drains up to `MAX_JOURNAL_BATCH` queued messages, stages
  each, then flushes **once** — so a burst of consensus messages costs a single
  fsync instead of one per message, with no weakening of durability (a reply is
  still deferred until the decision behind it is durable). `FjallJournal::flush`
  is the `persist(SyncAll)`; the handler refactor returns deferred, durability-
  gated sends (`handle_staged`/`drive_staged`). A node-level test
  (`inbox_drains_a_burst_into_one_group_commit`) proves 50 staged records collapse
  to one flush, and the deterministic simulation confirms safety/convergence are
  unchanged. ✅ **Snapshots + compaction + log truncation** (bounds both the
  journal and the in-memory `Replica.commands`/`by_key`, previously unbounded —
  the blocker for long-running deployments). A **redundancy watermark** (mirroring
  Accord's `redundantBefore`): below it every replica has applied everything, so
  the state is redundant. Each node gossips its `applied_through` (lagged by
  `COMPACTION_MARGIN` so a not-yet-propagated commit is never skipped); the
  recovery sweep computes the **per-shard minimum** and `Node::compact`s below it —
  dropping redundant commands (`Replica::compact`, pruning `by_key`) and
  truncating the journal (`Journal::truncate` + a persisted watermark). `is_ready`
  treats a dependency missing below the watermark as satisfied (a `Commit`'s
  quorum-unioned deps can name a compacted-away transaction). A still-behind or
  partitioned peer holds the min down (and an unheard-from peer blocks compaction),
  so truncating is always safe. **Catch-up:** only a brand-new joining node can be
  below the watermark, so `join` transfers a **data-store snapshot**
  (`DataStore::snapshot`) for the truncated prefix plus the recent commands and the
  watermark; anti-entropy stays command-based. `recover_state` restores the floor
  and trusts the durable data store for the prefix, replaying only the journal
  tail. Validated by the deterministic simulation: a **bound oracle**
  (`consensus_state_stays_bounded`) proves `command_count` returns to zero after a
  workload while committed state is preserved, and safety/convergence/bit-
  reproducibility hold across all seeds with compaction live (churn, partitions,
  restarts), plus join-after-compaction and restart-after-compaction tests.
- **Phase C — Geo hardening.** ✅ ✅ **Bounded clock-skew
  handling:** the HLC now **witnesses** every inbound peer timestamp
  (`Node::handle_staged` → `Clock::witness`, previously dead code) so node clocks
  track the cluster — but adoption is **capped at `MAX_SKEW`** beyond the local
  wall clock (`clock.rs`), so a faulty/far-future timestamp can't run a clock away
  (which, unbounded, would pin the physical clock below the logical and overflow
  the `logical` counter). With clocks kept within `MAX_SKEW` and the recovery/
  compaction margins sized above it, the `now - margin` cutoffs stay valid under
  drift, so a skewed node no longer stalls recovery or compaction; a node beyond
  the bound degrades gracefully rather than poisoning peers. Clock unit tests pin
  the cap and the catch-up; a simulation scenario (`safety_and_bound_hold_under_clock_skew`)
  proves a skewed cluster stays safe, converges, and keeps state bounded.
  ✅ **Tunable timing/sizing:** the hard-coded constants are now an injected
  `NodeConfig` (fast/collect timeouts, recovery interval/timeout, compaction
  margin, journal-batch cap) with `Default` reproducing the shipped values and a
  fluent `Node::with_config` (so geo deployments raise the timeouts); the clock's
  skew bound is likewise tunable via `HybridLogicalClock::with_max_skew`. A node
  unit test confirms a custom batch cap is honoured end to end.
  ✅ **Backpressure:** the inbox (`transport.rs`) and per-peer TCP writer/inbox
  channels (`tcp.rs`) are now **bounded** (`mpsc::channel`, capacity
  `DEFAULT_INBOX_CAPACITY` / `CHANNEL_CAPACITY`); a full channel **sheds** via
  `try_send` (loss is already tolerated by quorums/recovery) instead of blocking
  the sender or growing memory without limit, so a flooding or slow/unreachable
  peer can't OOM a node. A transport unit test proves a flood past capacity is
  bounded and never blocks; the deterministic sim (well under capacity) is
  unaffected.
  ✅ **Phi-accrual failure detector:** every inbound message is a heartbeat, and a
  per-peer φ (`failure_detector.rs`, Akka's logistic approximation) accrues from
  the heartbeat inter-arrival distribution; the recovery sweep takes over a
  stalled transaction once its coordinator's φ exceeds `NodeConfig.phi_threshold`
  — adapting to a link's real latency — with the fixed `recovery_timeout` kept as
  a liveness fallback (so the change is purely additive: never slower than before,
  and at the default config the fallback still dominates). Unit tests pin the φ
  math and that a silenced peer is suspected while a regular one is not; sim tests
  prove that with the fixed timeout disabled the detector alone recovers a crashed
  coordinator's stalled transaction, and that a healthy cluster recovers nothing.
  ✅ **Region awareness:** the `Topology` carries optional `region(node)` tags
  (`RegionId`; `ShardedTopology::with_regions`), and read routing
  (`Node::an_owner_of`) prefers a **same-region** owner before any other — so a
  read forwarded for a non-local key stays in-region when possible. Quorum latency
  needs no change: the coordinator already collects the *first N* responses, so
  nearby replicas form the quorum naturally. (The deeper region-favouring fast-path
  *electorate* — single-region commits in one local round-trip — is implemented in
  **Phase D**.) A unit test proves a read routes to the same-region owner even when
  a remote owner is listed first.
  ✅ **TLS + mutual auth:** the TCP transport can wrap every connection in rustls
  with mutual certificate auth — `TcpTransport::with_tls` (client side) + `serve_tls`
  (server side, verifying client certs), the stream becoming plain-or-TLS via
  `tokio_util::either::Either` so framing/consensus are unchanged. Operators supply
  the rustls connector/acceptor; plaintext `new`/`serve` stay for trusted networks.
  `tests/tls_cluster.rs` runs a 3-node cluster over **real mutual-TLS sockets**
  (rcgen-generated CA + leaf), proving replication and one-winner conflict
  resolution end to end over an authenticated, encrypted transport.
- **Phase D — Sign-off.** 🚧 *In progress.* ✅ **Observability:** a per-node
  `Metrics` (`metrics.rs`) of atomic counters — writes committed/conflicted, fast
  vs slow path, recoveries, compactions, journal flushes, messages handled —
  exposed via `Node::metrics()` (a `MetricsSnapshot`) for export to a backend,
  alongside `tracing` debug events at the same decision points (write outcome,
  recovery takeover, compaction). A sim test asserts the counters track real
  activity (clean commits, a raced conflict, transport/journal traffic).
  ✅ **Performance baseline:** a criterion benchmark (`benches/throughput.rs`,
  `cargo bench`) measures (a) conflict-free write **latency** and (b) **concurrent
  throughput** (a 64-deep burst). The baseline probe surfaced a real bottleneck —
  and the fix is now in (see **Write pipelining** below): the execution scheduler
  (`next_apply`/`next_read`) used to linearly scan **every** command per execution
  step, so as applied state accumulates between compactions the inbox loop's
  per-message cost grew O(n). Replacing the scan with an ordered execution queue
  (`Replica.pending`) cut the 64-deep-burst time **~10–15×** (3 nodes ≈ 1.7→27 K
  writes/s; 5 nodes ≈ 2.4→25 K) and write latency to ≈ 65 µs (1 node), 87 µs (3),
  87 µs (5). The node's single inbox loop remains a *further* (now far less urgent)
  parallelization lever.

  **Phase D remaining (sign-off):**
  - [x] Observability — in-process metrics + `tracing`.
  - [x] Performance baseline — criterion latency/throughput benchmark.
  - [~] **External / adversarial verification — Jepsen suite.** 🚧 Harness built
        (`evento-accord/jepsen/`): a self-contained Docker cluster (control + n1..n5)
        runs the `jepsen-node` binary (`examples/jepsen-node`, a generic-API Accord
        replica over real TCP + durable Fjall + `FjallJournal`) under an **Elle**
        list-append workload — atomic multi-key appends + single-key reads — with a
        network-partition nemesis. **First finding (partitions only):** with
        `--consistency-model serializable` the history is **valid** (atomic multi-key
        writes commit in one global order — no G0/G1c/lost-update; the consensus core
        is sound under partitions), but `strict-serializable` (the default bar)
        **fails** with a real-time read anomaly (`:G-single-item-realtime`): a read
        served off a *lagging* replica returns state inconsistent with wall-clock
        order, because local reads have **no linearizing barrier**. So writes are
        serializable & atomic; **reads are not linearizable**. The nemesis set
        (`--faults`, `jepsen.nemesis.combined`) now also drives **process kill** (incl.
        all-nodes-down → journal-recovered restart) and **pause** (SIGSTOP/SIGCONT):
        under `partition+kill+pause` the history stays **serializable**, confirming
        write-path safety under compound faults. A **linearizable read path**
        (`Node::read_barrier`, gated by `NodeConfig.linearizable_reads`) addresses the
        read gap as a lightweight **read-index** that stores nothing (reads never enter
        the conflict graph or journal — `ReadProbe`/`ReadProbeOk` + `Replica::read_probe`
        / `deps_applied`): probe a slow quorum for the deps a read would witness, wait for
        them to apply locally, then serve. Jepsen results (Elle `:cycle-search-timeout`
        raised to 60 s so high-contention runs reach a conclusive verdict): under a
        quorum-preserving `:one` partition the cluster is **strict-serializable at conc 2,
        5, and 10** (1922 ok at conc 10) and healthy is strict-serializable (2920 ok) —
        reads are linearizable. **A real bug surfaced and was fixed along the way:** at
        conc 10 the run was first invalid with a *confirmed* `G-nonadjacent-item-realtime`
        (read/write **anti-dependencies** = stale reads, not the "premature visibility" we
        first guessed). A deterministic Rust reproduction (`tests/linearizable_stress.rs`:
        writers/readers under crash-heal churn + monotonicity/premature/convergence
        oracles) localized it to **anti-entropy never converging a churned node**:
        `Replica::import_applied` skipped any *known* command, but a node that received a
        txn's `Commit` and missed its `Apply` holds it at `Committed` with no decision —
        normal execution can't apply it (needs a decision) and anti-entropy refused to,
        so it was stuck forever and the node diverged permanently; reads off it were
        stale. Fix: `import_applied` adopts a peer's *applied* state for a known-but-
        unapplied command. *Remaining:* clock-skew (off by default — bumps the shared
        kernel clock, needs real VMs; the node image now ships a compiler for jepsen's
        time helper and `run.sh` refuses it under Docker without `ALLOW_CLOCK_SKEW=1`);
        then the box can be checked.
  - [ ] **Independent expert review** of the protocol and implementation.
  - [ ] **Real-cluster soak + chaos** over days (kills, partitions, clock skew,
        disk pressure, slow disks/links) on actual hardware — zero production
        mileage today.
  - [x] **Write pipelining** — the measured throughput ceiling was the inbox
        loop's **O(n) execution scan**, not the network/fsync: `next_apply`/
        `next_read` scanned every command (incl. all the accumulated `Applied` ones)
        per step. Fixed with an ordered execution queue (`Replica.pending`, a
        `BTreeSet<(execute_at, txn)>` of only the un-applied commands), kept in
        lockstep with `commands`. **~10–15× throughput** on the 64-deep burst and
        ~10× lower write latency, validated against the benchmark; the simulation
        (incl. bit-reproducibility — the `BTreeSet` order is deterministic) and all
        67 tests still pass. (Thread-level parallelization of the single inbox loop
        remains a further, now far less urgent, lever.)
  - [x] **Fast-path electorate** (deferred from Phase C) — region-favouring
        single-round-trip commits, now implemented and validated. A shard's
        **electorate** is a cluster-agreed subset of its replicas (the same for
        every coordinator — a `Topology::fast_electorate` property, defaulting to
        the whole set) whose `PreAccept` votes decide the fast path. The fast
        quorum generalises to `⌊(e + f)/2⌋ + 1` over the electorate size `e`
        (`api.rs`), reducing to the classic `⌈3f/2⌉ + 1` at `e = N` and shrinking
        toward `f + 1` as the electorate shrinks; the builders assert the
        recovery-sound bound `f + 1 ≤ e ≤ N`. Placed in one region, the electorate
        lets a co-located coordinator commit in **one local round-trip** without
        waiting for remote replicas — non-electorate replicas still witness (for
        deps & recovery) but do not vote, so the coordinator gates the fast path on
        electorate responses only (`node.rs coordinate`). Recovery stays sound
        because the shared electorate keeps the two intersection invariants
        (`fast ≥ f+1`; `2·fast > e`); **a latent recovery gap surfaced and was
        fixed** along the way: `recover` decided the PreAccepted keep-`t0`-vs-raise
        branch on whatever arrived before the timeout, not on a quorum — harmless
        at `e = N` but a split risk under a shrunk electorate, so it now gates the
        decision on a slow quorum (`f + 1`) per touched shard. Validated by a
        **latency model** in the in-memory transport (per-link delay, **off by
        default** so the deterministic suite is byte-identical): `tests/electorate.rs`
        shows a region-local electorate commits on the fast path in a few local
        round-trips — far below a cross-region reply or the fast-path timeout —
        while the default electorate falls back to the slow path under the same
        geography; plus the recovery-quorum gate and idempotent double-recovery
        under a shrunk electorate. The deterministic partition oracle also runs
        under a shrunk electorate (`safety_holds_under_partitions_with_a_shrunk_electorate`,
        20 seeds). **`DynamicTopology` electorate:** rather than threading an explicit
        electorate through config-Paxos / the metadata log, the dynamic topology
        derives it **from the agreed layout + static region tags**
        (`DynamicTopology::with_regions`): each shard's electorate is the largest
        in-region group of its replicas (lowest-`RegionId` tie-break), used when its
        size is in `[f+1, N]` and the whole shard otherwise. Since every node holds
        the same layout and region map, all nodes derive the identical electorate —
        so **nothing extra crosses consensus or the wire** (no format-version bump),
        and it is recomputed correctly after every epoch change. A latency-model
        integration test (`tests/membership.rs`) shows a region-local coordinator
        commits on the fast path and that the property survives a `change_topology`
        epoch bump.

- **Phase E — Production readiness.** 🚧 *In progress.* The items that make it
  safe to run, not just correct in a lab. **Five of the six items are done; PKI/cert
  management is deferred** (see below):
  - [x] **Cluster-metadata / membership service** — the config-Paxos stand-in is now
        a durable, replicated **metadata log** (the "core" CEP-21 depth: log replay +
        durability, no leader/Raft). Per-epoch single-decree Paxos still decides each
        entry, but decided `(epoch, layout)` entries form an append-only log installed
        in **strict contiguous order** (`MetadataLog`/`apply_log`, so no epoch is ever
        skipped); a behind node catches up via `MetadataFetch`/`MetadataEntries`
        (metadata anti-entropy in the recovery sweep, plus an eager pull when a later
        `ConfigCommit` reveals a gap); acceptor state + the committed log are persisted
        via the extended `Journal` (`record_acceptor`/`append_metadata`) and restored
        by `recover_state` (a restarted acceptor never regresses a ballot); and the
        sweep auto-completes an accepted-but-uncommitted epoch. Validated by
        `tests/membership.rs` (replay-of-missed-epochs, restart durability,
        restarted-acceptor-rejects-stale-ballot) and a deterministic
        **membership-churn** simulation scenario (`membership_churn_converges` +
        `membership_churn_is_bit_reproducible`) proving the log converges under
        concurrent reconfiguration, writes, and minority churn while data-plane safety
        holds. *Remaining (deferred): a leader/lease for liveness under contention.*
  - [x] **Metrics export** — `MetricsSnapshot::to_prometheus` /
        `to_prometheus_labeled` render the counters in **Prometheus text exposition
        format** (`accord_*_total` counters, optional labels e.g. the node id),
        hand-rolled so the crate stays **dependency-free** — the operator wires the
        `/metrics` HTTP endpoint and serves the string, alongside the existing
        `tracing` events. (A live `prometheus`-crate registry behind an optional cargo
        feature is the future path if ever wanted; unwarranted for 9 plain counters.)
  - [ ] **PKI / cert management** — issuance, rotation, per-node identities (the
        TLS tests use a self-signed shared cert). *Deferred — cert issuance is an
        external-CA concern; the TLS API already accepts an operator-supplied rustls
        connector/acceptor, so per-node identities are wireable today.*
  - [x] **Format & upgrade story** — every bitcode record (the framed-TCP wire and the
        disk-journal *values*) now carries a 4-byte `[MAGIC | format_version | kind]`
        header (`src/format.rs`, `encode_tagged`/`decode_tagged`), so a layout change
        is **detected, not silently mis-parsed** (bitcode is positional). Journal keys
        stay bare (their `TxnId` ordering drives the truncation scan). **Rolling-upgrade
        path:** version mismatch makes peers mutually *shed* frames (loss the consensus
        layer tolerates), so a cluster rolls **drain-and-replace** (keep a quorum on one
        version at a time); a future field addition bumps `FORMAT_VERSION` and adds a
        per-version `decode_tagged` branch. Pre-1.0: the tag is mandatory, with no reader
        for untagged records (upgrade-from-untagged wipes and re-bootstraps via `join`).
  - [x] **Snapshot-at-scale** — `ExecutorDataStore::version`/`snapshot` now **paginate**
        the backend (`SNAPSHOT_PAGE_SIZE`-event pages, walking until exhausted) instead
        of a single `u16::MAX`-capped page, so an aggregate (or store) of any size is
        handled. (`version` folds a running max across pages.) A streaming snapshot for
        truly huge stores is a noted future refinement.
  - [x] **Backpressure/rate-limiting policy** beyond drop-on-full — (1) shed
        observability: a `messages_shed` metric counts frames dropped for a full peer
        queue (`TcpTransport`, sharable `Arc<Metrics>` via `with_metrics`) and an
        `InMemoryNetwork::shed_count`, both **observe-only** (never feed control flow, so
        the simulation stays deterministic); (2) a bounded consensus backlog —
        `NodeConfig.max_commands` makes a node **refuse to coordinate a new write** once
        `Replica.commands` reaches the cap (e.g. compaction stalled under a sustained
        partition), shedding *new load* only. Existing consensus state is **never
        evicted** and peer messages are **never refused** (the gate is the local
        coordinator's entry point alone — refusing peer messages would break
        safety/liveness).

## Production-readiness verdict

**Not production-ready.** This is a faithful, well-tested **reference
implementation** — verified in a deterministic fault-injection simulation,
durable, bounded, geo-hardened, observable, and benchmarked (Phases A–C complete,
Phases D and E partial — Phase E's format-versioning, Prometheus metrics export,
snapshot-at-scale, backpressure, and the durable replicated metadata log are in,
leaving only PKI/cert management) — and a strong base to *take* to production. It is suitable for
prototypes, demos, and controlled/low-stakes use. It is **not** yet safe for
production: external/adversarial verification has only just begun (a Jepsen/Elle
harness now exists and its first partition run already found that **reads are not
linearizable** by default — writes are serializable & atomic, but a read off a lagging
replica can break real-time order; an opt-in `linearizable_reads` read-index barrier
makes reads linearizable and the cluster strict-serializable under quorum-preserving
partition — Jepsen-validated to concurrency 10, after the harness found and fixed an
anti-entropy convergence bug), and there is still no independent review and no
real-cluster soak mileage. The membership/metadata layer is now a durable, replicated
metadata log (Phase E) with replay and restart-durability, though it still lacks a
leader for liveness under contention. The gating items are the unchecked boxes in
Phases D and E above, in roughly that order (verification and soak first). The crate
version (`2.0.0-alpha.*`) reflects this.

The event-data path is already backend-agnostic (`AccordExecutor` runs on any
`evento_core::Executor`); Phase B extends that to the consensus state.
