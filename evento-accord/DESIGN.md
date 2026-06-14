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
- **M4 — Topology changes.** Epoch transitions, bootstrap/sync of joining nodes,
  range movement. Large and intricate.
- **M5 — Executor wiring.** ✅ `AccordExecutor` is a real `evento_core::Executor`:
  **writes** are coordinated through the Accord cluster (mapping the outcome to
  `Ok(())` / `WriteError::InvalidOriginalVersion`); **reads, subscriptions, and
  snapshots** delegate to a local evento backend (`Fjall`) that each replica keeps
  current by applying committed transactions via `ExecutorDataStore` (the bridge
  `DataStore` — `version` reads the aggregate's current version, `apply` appends).
  The coordinator waits for its own local apply (read-your-writes). Scope:
  single-shard cluster (a node's backend holds the whole log); routing reads to
  owning shards is a later layer. `tests/executor.rs` (fjall-backed 3-node
  cluster) proves cross-node replication, read-your-writes, and cluster-wide
  optimistic concurrency through the standard evento API.
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

**M0–M3 + M5 complete + production TCP transport.** Multi-shard Accord: quorum
progress (tolerates `f` failures per shard), ballot-fenced recovery, atomic
cross-shard conditional appends via the Read→Apply split with **per-shard
dependencies**, a real framed-TCP `MessageSink`, and `AccordExecutor` — a drop-in
`evento_core::Executor` backed by the cluster. 21 tests pass (7 unit, 6 cluster,
3 multi-shard, 3 executor, 2 TCP), clippy clean, stable across repeated runs.
Remaining: **M4 — topology changes** (epochs, joining/leaving nodes, range
movement) for elastic clusters, and multi-shard read routing for the executor.
