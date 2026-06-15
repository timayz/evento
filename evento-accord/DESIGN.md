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
| Fast path  | `⌈3f/2⌉ + 1`    | 3         | 4         |
| Slow / recovery | `f + 1`    | 2         | 3         |

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
(`ConfigPrepare`/`Promise`/`Accept`/`Accepted`/`Commit`) is a **minimal stand-in
for that role** — an implementation choice, not part of Accord. It is scoped to
"agree on one topology layout per epoch" (single-decree Paxos per epoch number)
rather than a general metadata log; the epoch number supplies the ordering a log
would otherwise provide. Known gaps versus a full metadata service: a node that
misses several epoch changes has no log to replay (it only learns of changes it
receives a `ConfigCommit` for), and there is no leader for liveness under
contention. Keep this boundary in mind: anything `Config*` is the control plane,
everything else is Accord.

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
range movement (re-sharding), and multi-shard executor read routing. **64 tests**
(24 unit, 6 cluster, 3 multi-shard, 7 membership, 1 resharding, 3 executor,
1 shard-executor, 2 TCP, 2 mTLS, 10 simulation, 3 restart, 2 fjall-journal), clippy
clean,
stable across repeated runs (the simulation suite is deterministic — see Phase A).
The full M0–M5 roadmap plus elastic membership (M4) is implemented.

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
  nearby replicas form the quorum naturally. (The deeper region-favoring fast-path
  *electorate* — single-region commits in one local round-trip — is a larger
  CEP-15 change left for later; it isn't validatable without a latency model.) A
  unit test proves a read routes to the same-region owner even when a remote owner
  is listed first.
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
  `cargo bench`) measures (a) conflict-free write **latency** — ≈ 0.46 ms (1
  node), 0.72 ms (3), 1.2 ms (5), growing with quorum size as expected — and (b)
  **concurrent throughput** (a 64-deep burst). The throughput probe surfaced a
  real bottleneck: aggregate throughput is *lower* than serial latency predicts,
  because each node's **single inbox loop** serializes message processing across
  all in-flight transactions (every write still needs three sequential quorum
  round-trips through it). So the inbox loop — not the network or fsync — is the
  throughput ceiling, and parallelizing/pipelining it is the concrete next perf
  lever, now measurable.

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
        write-path safety under compound faults. *Remaining:* clock-skew (off by
        default — bumps the kernel clock, so it needs real VMs, not shared-kernel
        Docker), then a linearizable read path (or an explicit weaker-reads contract)
        before the box is checked.
  - [ ] **Independent expert review** of the protocol and implementation.
  - [ ] **Real-cluster soak + chaos** over days (kills, partitions, clock skew,
        disk pressure, slow disks/links) on actual hardware — zero production
        mileage today.
  - [ ] **Write pipelining** — parallelize the per-node inbox loop (the measured
        throughput ceiling); validate the win against the benchmark.
  - [ ] **Fast-path electorate** (deferred from Phase C) — region-favoring
        single-round-trip commits; not validatable without a latency model.

- **Phase E — Production readiness.** ⬜ *Not started.* The items that make it
  safe to run, not just correct in a lab:
  - [ ] **Cluster-metadata / membership service** replacing the minimal config-
        Paxos stand-in (today: single-decree per epoch, no log replay for a node
        that misses epochs, no leader for liveness under contention).
  - [ ] **Metrics export** (Prometheus / OpenTelemetry) and structured-log wiring,
        not just in-process counters.
  - [ ] **PKI / cert management** — issuance, rotation, per-node identities (the
        TLS tests use a self-signed shared cert).
  - [ ] **Format & upgrade story** — versioning for the journal/wire `CommandState`
        encoding and a rolling-upgrade path.
  - [ ] **Snapshot-at-scale** for large aggregates (the `ExecutorDataStore`
        `version`/`snapshot` reads cap at `u16::MAX` events per aggregate today).
  - [ ] **Backpressure/rate-limiting policy** beyond drop-on-full, and bounded
        `Replica.commands` recovery memory under sustained partition.

## Production-readiness verdict

**Not production-ready.** This is a faithful, well-tested **reference
implementation** — verified in a deterministic fault-injection simulation,
durable, bounded, geo-hardened, observable, and benchmarked (Phases A–C complete,
Phase D partial) — and a strong base to *take* to production. It is suitable for
prototypes, demos, and controlled/low-stakes use. It is **not** yet safe for
production: external/adversarial verification has only just begun (a Jepsen/Elle
harness now exists and its first partition run already found that **reads are not
linearizable** — writes are serializable & atomic, but a read off a lagging replica
can break real-time order), and there is still no independent review, no real-cluster
soak mileage, an unoptimized throughput ceiling, and a stand-in membership/metadata
layer. The gating items are the unchecked boxes in Phases D and E above, in roughly
that order (verification and soak first). The crate version (`2.0.0-alpha.*`)
reflects this.

The event-data path is already backend-agnostic (`AccordExecutor` runs on any
`evento_core::Executor`); Phase B extends that to the consensus state.
