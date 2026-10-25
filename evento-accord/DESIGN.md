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
  **t ≥ t0** and the dependency set — each replica's AcceptOk names every conflict
  with a `t0` below `t`, so a conflict that arrived after PreAccept (and may have
  fast-committed in between) becomes a dependency.
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
- Per-peer lazy writer tasks connect on demand and reconnect after a drop.
  Nothing waits on the network unbounded: a dial (connect + TLS handshake)
  gives up after 2 s and a batch write after 5 s (counted as a failed write,
  so the writer redials), and a failed dial backs off 100 ms → 1 s before the
  next — a peer behind a DROP partition cannot pin its writer for the
  kernel's SYN retries or TCP's retransmit backoff while every frame to it is
  shed. Delivery is best-effort (loss tolerated by quorums/recovery). `serve`
  runs the inbound accept loop, decoding frames into `Envelope`s for a node's
  inbox.
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
  (`Journal::record_acceptor`; a vote it could not persist is withheld, like a batch
  whose flush failed), and the committed log is persisted
  (`Journal::append_metadata`) before an epoch is installed — one the log did not
  take is not installed, and the commit or the metadata sweep retries it, so a
  restart never falls back below an epoch the node acted on; `recover_state`
  restores both, so a restarted acceptor keeps its promised ballots (Paxos
  crash-safety) and re-installs its epoch prefix. A proposer counts no reply older
  than `collect_timeout` (each phase collects until then): an acceptor that lost
  its journal relies on it to vote again (#338, below).
- **Automatic completion.** The recovery sweep finishes any accepted-but-uncommitted
  epoch itself (the automatic equivalent of `recover_topology`).
- **Valid layouts only** (`validate_layout`): at least one shard, none empty, every
  node in exactly one shard, once. Without shards every key's shard index divides by
  zero; an empty shard owns keys no replica can commit (and `vec![vec![]]` leaves no
  acceptor for the next epoch). `change_topology`/`propose_topology` refuse one, the
  proposer checks the value it carries to phase 2 (an adopted or fallback one too),
  an acceptor never votes for one, and a node neither persists nor installs one it is
  sent: the log keeps the first entry per epoch, so a persisted bad entry would shut
  out the real one, and an entry an older build persisted is never installed.

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
  *Since hardened (2026-10 review):* recovery sends `BeginRecovery` carrying the
  keys/events the recoverer holds, so a replica that never witnessed the txn
  pre-accepts it under the recovery ballot (or, owning none of it, keeps a
  ballot-only promise) — PreAccept and Accept are refused below any promise, so the
  original coordinator, if merely slow, can no longer win a replica the recovery
  counted; promises are journaled before they are revealed; the Accepted branch
  adopts the highest *accepted* ballot only (a pre-accepted report's `Ballot(t0)` is a
  placeholder); a re-proposed value is committed only once a quorum accepted it; and
  an earlier conflict committed past `t0` without the txn rules the fast path out
  (one only accepted makes recovery wait). A replica holding a read slot answers any
  recovery's Commit with the read it took.
  *Fast-path votes (#285):* the superseding check alone could keep `t0` for a txn
  that never fast-committed: a later conflict fast-committed without the txn may
  still be only pre-accepted at every replica recovery hears from, which proves
  nothing there. Recovery now also counts the replicas' own PreAccept votes (a
  pre-accepted report's `execute_at`), as accord-core's `RecoveryTracker` does:
  once more than `e − F` members of a shard's fast-path electorate are heard
  voting above `t0`, no fast quorum can have voted `t0`, and recovery re-proposes
  above every vote (`Node::votes_reject_fast_path`). The count is sound both ways
  for any electorate `f + 1 ≤ e ≤ n`: a fast commit at `t0` leaves at most `e − F`
  votes elsewhere, and a conflict fast-committed without the txn puts
  `F + S − n > e − F` of its fast quorum among any `S` reports (`2F > e + f`),
  each of which saw the conflict first and so voted above `t0`. Counting a
  conflict held only pre-accepted as superseding instead would be unsound: the
  replica holding it is merely outside the txn's fast quorum, and the txn may
  still have fast-committed through the others.
  *Multi-shard (routes, 2026-10):* each replica stores only its shard's keys, so a
  recovery used to plan the shards the reports revealed within `fast_timeout` — a
  shard that never witnessed the txn (coordinator died mid-broadcast) or answered
  late was left out, and the txn committed (or aborted) on the others alone. Now a
  txn spanning shards carries its **route** — the full key set (keys survive
  re-sharding, shard ids don't) — in a `Message::Routed { route, inner }` wrapper on
  PreAccept / Accept / Commit / BeginRecovery / BeginRecoveryOk; each replica keeps it
  with the command (`Replica.routes`, `Journal::save_route`, record kind `Route`,
  dropped with the command). A **routed** recovery asks every replica of every route
  shard and waits (up to `collect_timeout`) until each shard is accounted for. A
  replica that never witnessed the txn does **not** pre-accept it there (unlike the
  unrouted path): it journals a promise (`Journal::save_promise`, record kind
  `Promise` — the blind promise is no longer in-memory only) and answers
  `NotWitnessed`. This is what makes the decision sound, after Accord's
  *invalidation*: a slow quorum of shard S answering "never witnessed" under ballot b
  proves the txn never committed (a commit needs a PreAccept quorum on S, which meets
  it — a witness journals before it votes and keeps the command until it is applied,
  when it reports "below the watermark", not "never witnessed") and never will (those
  replicas refuse the original coordinator below b, and no recovery can assemble a
  witnessed quorum on S, since nothing but a PreAccept makes a replica witness before
  a decision). Pre-accepting instead would let a later recovery that learned S's part
  from a straggler turn the proof into a commit after it was acted on. The recovery
  then **invalidates** the txn (`Message::Invalidate`): every replica holding it
  un-committed records it applied as an abort without executing it (an abort changes
  nothing, so needs no place in the order), marked by `execute_at = INVALIDATED_AT`
  (`Timestamp::MIN` — no field added to `CommandState`), which a later recovery
  recognizes as decided and which recovery's superseding check ignores (its deps are
  one replica's PreAccept view, never agreed). A reply from below a replica's
  redundancy watermark is no evidence of "never witnessed" (the txn may have been
  applied and compacted); with a `Committed` report anywhere the decision exists and
  is re-driven on the shards still holding it; with none, a compacted shard plus a
  slow quorum elsewhere still below `Committed` means the low shard never witnessed it
  (invalidate, relying on messages not outliving `compaction_margin`). Anything else —
  a shard split between witnessed and never-witnessed below a quorum — decides
  nothing until more replicas answer: a liveness cost only for a multi-shard txn whose
  coordinator died *and* whose shard lost a replica. A legacy (unrouted) recovery that
  meets a routed report switches to the routed path. Skipping a compacted shard drops
  its read, so a decided txn re-driven without it takes the decision from a replica
  that applied it (`execute` waits for that replica's answer to the Commit — it is the
  decision) and decides nothing while none is heard from. That replica may be a
  straggler of the skipped shard itself, not compacted yet: it is sent the Commit and
  its answer counts like a read, though its shard needs no read or apply quorum (#305).
  Once a whole shard has compacted the txn, no such replica is left, so compaction
  keeps a **decision record** of every applied multi-shard txn it drops
  (`Replica.decisions`: `execute_at`, the decision and the route, journaled as
  `RecordKind::Decision` before the truncate that deletes the command). A replica
  holding one answers a recovery as an applied replica would (`Applied` at the
  recorded `execute_at`, `INVALIDATED_AT` kept for an invalidated txn), and a Commit
  or Apply with the recorded outcome, so the decision comes from that shard and the
  txn no longer pins the other shard's watermark, subscriptions and backlog (#286).
  A record is dropped once every replica of every other route shard reports a
  watermark above the txn — each applied it — using the `Watermark` gossip every
  node already sends every other.
  A replica never re-creates a txn below its watermark that it does not hold
  (`Replica::compacted`): PreAccept, Accept and Commit leave it alone, and an
  unrouted recovery hears it is below the watermark, so a decider that compacts
  mid-recovery, or a replayed message, cannot execute an applied txn again.
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
  fetches a peer's committed state (`JoinHeaderRequest` / `SyncData`, then the serialized
  `CommandState`s page by page through `CommandPageRequest` / `CommandPage`, then the
  data-store snapshot page by page through `SnapshotPageRequest` / `SnapshotPage`), imports each command into its conflict graph
  (`Replica::import_applied`) and applies its events, then **replays the buffered
  messages** — so a transaction that commits *during* the join still lands
  (sync-point-style). A node rejoining after it was given up on, or with a lost
  store, keeps its journal and recovers it first (`Node::recover_state_for_join`,
  see Phase B). A node drives an epoch change with `Node::change_topology`,
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
  change, and the new shard's nodes copy *only the slots they gain* while the
  change drains (below); `tests/resharding.rs` moves a key to a new 3-node shard
  and shows writes route to — and version state is enforced by — its new owners,
  and an existing member moving to another shard copies that shard's keys. The
  config-Paxos **acceptor set is the current members** (a node proposing the next
  epoch is still on this one), so reconfiguration keeps working after the founders
  have left (`config_changes_outlive_the_founders` removes a founder majority and
  still reconfigures). *Remaining:* garbage-collecting a moved range from its old
  owners (and so giving a node back slots it replicated before), and multi-shard
  read routing for the executor. A node that gains slots copies their events with
  their original stamps, so its subscriptions, if they already read past those
  stamps, do not see them (as broad scans on a multi-shard cluster are local
  today, a subscription that must see every key runs on a single-shard cluster).

  **Range movement with transactions in flight (#288, #359, #360; shipped).**
  A write in flight while its keys change replicas was exposed four ways:
  (i) a replica that no longer owned a key voted `t0` with no dependencies on an
  empty command, so the write could report success with its events applied
  nowhere;
  (ii) new owners answer "never witnessed", and routed recovery could invalidate
  a committed transaction (its blind check precedes the committed check);
  (ii′) the same inside one shard (#359): recovery counted the replicas of the
  installed layout, so after `{1,2,3}` became `{1,2,4}` it could count 2 and 4 as
  fenced and invalidate a transaction `{1,3}` committed, or re-commit it at
  another timestamp;
  (iii) new owners bootstrap before old owners finish the writes in flight, and a
  quorum of the new layout need not intersect a commit quorum of the old one.

  *Shipped: transactions carry their epoch.*
  - Every PreAccept, Accept, Commit, BeginRecovery and read probe carries the
    epoch of the layout its coordinator planned under, and `CommandState` and
    `DecisionRecord` journal it (format 4). A coordination reads one `Layout`
    snapshot for its whole life. (accord-core: the epoch is part of every
    `TxnId`.)
  - Every installed layout is kept (`Topology::layout_at`), the starting one
    included: the metadata log journals it with the first epoch decided after it,
    so a node rebuilt on a later layout after a restart still holds it.
  - A replica judges ownership by the transaction's epoch, so old owners keep
    voting, applying and answering recovery for it after a change, and a recovery
    plans, collects and counts by the transaction's epoch. This fixes (i), (ii)
    and (ii′).
  - A PreAccept or read probe planned under a layout retired since for one of its
    keys (the key's replica set changed), for a transaction this replica never
    saw, gets no answer: a coordinator that planned before a change cannot commit
    through the old replicas once a quorum of them installed it. A message
    planned under an epoch not installed yet is not answered either; the replica
    fetches the metadata it misses from the sender first.
  - A change for an epoch past the next one is refused before any vote.
  - G1 from #313 stays: a coordinator counts replies (votes, reads, apply acks,
    recovery reports) for the shard it *planned* their sender in. G2 now reads
    ownership under the transaction's epoch: a replica that owns none of the keys
    a PreAccept, Accept or Commit carries there, and holds nothing of the
    transaction, casts no vote, answers nothing and records nothing.

  *Shipped: two-phase layout changes,* for (iii) (`reconfig.rs`). A change
  that only adds replicas, after which every quorum of each changed shard still
  meets every quorum it had (one replica added to a shard of three), installs at
  once: the epochs above suffice, and a joiner catches up through the replicas
  its shard kept. Any other change (removing or replacing a replica, adding two)
  takes two decided entries, so that each key has one live layout at any time —
  a removal too: two installed at once in a row could leave a transaction in
  flight from before them without a read quorum of replicas that stay, its
  execution waiting on writes the replicas that left never get (#370):
  1. A *transition* keeps the old layout live — for ownership, watermarks,
     coverage, anti-entropy, compaction and recovery — and names the new one.
     Coordinators refuse writes to the keys whose replica set changes
     (`TopologyChanging`).
  2. Installing it, a replica computes a *fence point* under its replica lock:
     above every `t0` it holds or compacted, its clock reading plus two skews
     (a coordinator may date a transaction that far ahead), and its amnesia fence
     if it lost a journal. From then on it never votes for a transaction it never
     witnessed, planned before the transition ended, on a moved key, above that
     point; below it, it votes as before, so a write only a dead replica and one
     live replica hold is still recovered. The fence is journaled before it is
     reported (in `Watermark` gossip), and never lifted.
  3. The *drain point* is the highest fence reported by a slow quorum of every
     changing shard: no write on a moved key above it ever commits.
  4. The change has *drained* once every shard has a slow quorum whose watermark
     reports are past the drain point. A report is below the reporter's vote
     fence, and the reporter holds nothing unapplied below it, so every write on
     a moved key that ever commits is applied there, and no other will. Every
     shard is waited for, so a write that also touches an unchanged shard is
     applied there too. So is every replica that keeps a moved slot (a
     replacement's other replicas): one that lagged would catch up through
     anti-entropy under the new layout, where a replica that copied the slot holds
     its writes as events, with no command to ship, and vouches for them all the
     same.
  5. An *activation* entry installs the new layout, with the drain point and a
     clock floor: the proposer's reading plus three skews, which it waits past,
     and which every replica raises its clock to as it installs the entry. A
     replica votes for no transaction planned under the new layout on a moved
     key below the floor, so the new writes' events stamp after the moved ones';
     and it invalidates what it holds unapplied on a moved key above the drain
     point, which never commits.
  `change_topology` decides the transition, waits for the drain (about the
  compaction margin plus a few anti-entropy rounds, run by the recovery sweep),
  and decides the activation; the sweep activates a drained transition whose
  driver is gone. A recovery of an epoch nobody accepted a value for repeats the
  installed entry, so it never activates anything. A change while a transition
  is in progress replaces it; a change back to the live layout cancels it. Sync
  coverage, join proofs and vouches are keyed by the epoch the live layout became
  live at, so a transition does not reset them.

  *Shipped: moving keys between shards.* A key's shard is its slot's: a stable
  hash of the key modulo 4096 slots, and a slot table in the layout
  (`ShardMap`). `change_topology` rebalances the live table over the new shards:
  with as many shards no slot moves; with another count each shard keeps as many
  of its slots as its even share allows, and only the slots a new shard needs, or
  a removed one leaves, move. A change that moves a slot, or gives a replica of
  the old layout a slot (a replica moving to another shard), is a transition, and
  every node that gains slots — new or not — copies them during it: from each
  shard that holds some, through a replica whose watermark report is past that
  shard's own drain point, it copies those slots' events (`RangePageRequest`, a
  store page filtered by slot), leaving out what its store already holds. The
  keys are drained there and take no new write, so the copy is final: every
  write on them that commits is applied at that replica, and no other will. The
  activation waits for every gainer to report its copy (`Watermark::copied`),
  so a new layout goes live with every replica holding its keys. A change that
  only adds a replica to a shard that keeps others installs at once, and the new
  replica joins through one of them; `join` refuses a contact of another shard
  (#360: a joiner adopted such a contact's floor, its own shard's, and treated
  its new shard's later writes below it as compacted). A change that would give a
  node a slot it replicated before is refused: its store still holds that slot's
  events from then, and nothing drops them yet.
- **M5 — Executor wiring.** ✅ `AccordExecutor` is a real `evento_core::Executor`:
  **writes** are coordinated through the Accord cluster (mapping the outcome to
  `Ok(())` / `WriteError::InvalidOriginalVersion`); **reads, subscriptions, and
  snapshots** delegate to a local evento backend (`Fjall`) that each replica keeps
  current by applying committed transactions via `ExecutorDataStore` (the bridge
  `DataStore` — `version` reads the aggregate's current version, `apply` appends).
  The coordinator waits for its own local apply (read-your-writes). On a
  **multi-shard** cluster, a single-aggregate read for a key this node does not
  own is **routed** to an owner (`ReadForward` / `ReadReply`, served from the
  owner's backend; an owner whose store refused the query, or that is still joining
  and holds part of its store, answers `ReadFailed`, so the read fails instead of
  returning an empty or partial page that reads as "no such events"); broad scans
  are served locally. `tests/executor.rs`
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
electorate on `DynamicTopology`). **100 tests in the consensus crate**
(43 unit, 6 cluster, 5 electorate, 3 multi-shard, 12 membership, 1 resharding, 6 executor,
1 linearizable-stress, 1 shard-executor, 1 journal-faults, 2 TCP, 3 mTLS, 13 simulation, 3 restart),
clippy clean, stable across repeated runs (the simulation suite is deterministic — see
Phase A). The durable-journal tests live with their backends: 3 `FjallJournal` tests in
`evento-fjall` (`--features accord`) and 12 `SqlJournal` tests in `evento-sql`
(`--features accord` × sqlite/postgres/mysql).
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
  commit and recovery + anti-entropy must catch the node back up). The chaos,
  partition and restart scenarios also run over **lossy links** (#315): the
  in-memory network drops, duplicates and delays individual messages from a
  seeded generator of its own (`InMemoryNetwork::set_message_faults`), so
  messages on a link overtake each other, and the partition plan also cuts
  single directions (`InMemoryNetwork::cut`). That reaches the paths whole-node
  faults barely touch — a late or doubled PreAccept, Accept, Commit or Apply, a
  reply after its ballot moved on, a recovery racing a slow coordinator. The
  restart scenario also runs over **failing storage** (#316): the journal
  double (`tests/common`, shared with the journal- and store-fault suites)
  keeps staged writes pending until a flush succeeds, so a restart loses its
  unflushed tail; flushes, truncates, direct writes and applies fail at seeded
  rates; and a restarted node may keep its data store — possibly ahead of its
  journal — or, on the production `ExecutorDataStore` over an in-memory evento
  executor, come back **behind** it, having lost writes it had not persisted.
  That reproduced #309 deterministically, and showed what it is: a **split
  decision**. A replica whose kept store already held a transaction's own events
  — its apply forgotten across a restart, or brought by an import, which was not
  journaled then (it is since #331) — read that write as a version conflict, and recovery ANDed that read
  with the applied replicas' "commit": the replicas yet to apply it enacted an
  abort the others had committed, and the aborting store froze, every later event
  past the gap swallowed as "already present". Now recovery adopts the decision an
  applied replica reports, a replica reads its own stored events as its condition
  holding (`DataStore::holds`), `ExecutorDataStore` takes a version conflict for a
  re-apply only when the very events are stored, and a startup replay the store
  cannot take is handed back un-applied rather than swallowed. A decision-agreement
  oracle checks every run: no transaction enacted as a commit on one replica and an
  abort on another. Every seeded scenario then ends with a **liveness phase**
  (#317, `assert_live`): everything healed and any node told to rejoin rejoined,
  each node must coordinate a write that commits, pass a read barrier on every key
  (its store then at the highest acknowledged version), and compact its consensus
  state away, within bounds derived from its `NodeConfig`. It holds everywhere.
  A healed cluster used to take up to ~50 s of virtual time under message faults to
  recover the transactions left stalled, one `collect_timeout` at a time; it now
  recovers them within a few (#326). A replica that missed applied transactions
  catches up through anti-entropy, up to 16 pages a round (#348). An **absence**
  scenario (#310) has minority nodes go silent past `peer_absence_limit` — crashed,
  or cut one way — while the majority writes and compacts past them, then come back
  (rejoined if told to); an outside coordinator pre-accepts a write at a node just
  before its absence and vanishes, so it may come back holding a transaction alone.
  The workloads **read** too (#318): readers on any node — crashed, cut off,
  restarted or rejoined — fence each read with a read barrier and read the node's
  store, as `AccordExecutor` does with `linearizable_reads` on, and a read oracle
  checks every read per key: none sees fewer events than one that ended before it
  started, misses a write acknowledged before it started, or shows a write that
  returned a conflict or was invoked after it ended, and each is a prefix of the
  converged history. With the barrier switched off it reports stale reads, and the
  violation replays from its seed. Across all
  seeds, **safety holds** — no split-brain, no double-commit — and after healing
  the cluster **converges**. The harness already paid off: it found two real gaps
  (non-convergence from stalled transactions and from missed transactions), both
  now fixed (see Phase B). An `#[ignore]`d soak (`sweep_for_bugs`) runs every scenario
  across a wide seed range (250 seeds × all scenarios, ~8 min) for deeper hunting —
  last run clean. Every seeded test also takes its seeds from the environment
  (#319): `SIM_SEED` replays one, `SIM_SEEDS` or `SIM_DURATION` run fresh ones, and
  a failure prints the line that replays it. **Swarm testing** (`swarm`, ignored)
  draws each seed's whole shape — 3, 5 or 7 nodes, which faults are on and how hard,
  the store, the workload and a `NodeConfig` within OPERATIONS' constraints — so
  turning some faults off lets a run go deeper into the others; a nightly workflow
  runs it on fresh seeds for 45 minutes and reports a failing seed on an issue. ✅ **Deterministic runtime:** the tests run under
  `#[tokio::test(start_paused = true)]` — a single-threaded, virtual-time runtime —
  with a virtual physical-time source injected into the HLC
  (`HybridLogicalClock::with_physical`) and deterministic events, so a given
  seed reproduces **exactly** — checked on the *execution*, not only the outcome:
  every sim traces its network (`InMemoryNetwork::enable_trace` folds each
  message, its ends, its fate and its virtual time into one hash), and
  `every_scenario_kind_replays_its_trace` requires two runs of a seed to trace
  identically, for every scenario kind including restarts, churn and joins (#320).
  The protocol core was already order-independent (every map iteration feeding a
  decision is sorted or keyed; `Replica::stuck` now sorts too). #320 removed the
  last known leaks — the one `select!` was unbiased, a config reply fanned out in
  hash order, a multi-shard retry took the first Nack in hash order, and the
  sim's events carried random metadata ids — and planting either kind back (a
  random payload, a hash-ordered broadcast) fails the check at its first step.
  The message faults of #315 then surfaced one more, inside tokio: a `watch`
  channel parks each waiter on one of eight notifiers picked by an unseeded RNG,
  so writes waiting on one coordinator's applied state resumed in a random
  order; that signal is now a `Notify`, which wakes its waiters in a fixed
  order.
  Virtual time also made the suite ~85×
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
  cleanly and *while writes are in flight*. **The durable `Journal` impls live in
  their backend crates, not here** — mirroring how `evento-core` defines `Executor`
  and the backends implement it. `evento-accord`'s core depends only on its own
  `Journal` trait + `evento-core` (no `fjall`/`sqlx`); each backend opts in via an
  `accord` cargo feature: ✅ **Disk-backed journal:** `evento_fjall::FjallJournal`
  (bitcode-serialized `CommandState`s in a fjall database, `evento-fjall`'s `accord`
  feature) is a genuinely durable `Journal`; its test proves records survive a full
  close/reopen (a real process restart). ✅ **SQL-backed journal:**
  `evento_sql::SqlJournal<DB>` (`evento-sql`'s `accord` feature) is a drop-in `Journal`
  over `sqlx` + `sea-query`, portable across **sqlite/mysql/postgres** exactly like
  `evento-sql`'s `Sql<DB>` — so a deployment already on SQL keeps its consensus state
  in the **same database** as its events. It shares the tagged-bitcode format with the
  fjall journal, group-commits
  a staged batch in one transaction, and uses an order-preserving `TxnId` key so
  `truncate` is a single range `DELETE`. It is **integration-tested on all three
  backends** — round-trip of every record type, truncation, group-commit batching, and
  reopen/reconnect durability run against SQLite *and* live MySQL + PostgreSQL
  (`evento-sql/tests/sql_journal{,_postgres,_mysql}.rs`, shared scenarios, gated by the
  `accord` + `sqlite`/`postgres`/`mysql` features; servers via the repo's `make up`). The
  MySQL run already paid off — it caught a real dialect bug (sea-query's bare
  `do_nothing` emits an invalid `ON DUPLICATE KEY IGNORE`; fixed with `do_nothing_on`).
  The schema is managed the canonical way — through **`evento-sql-migrator`**
  (opt-in `accord` feature: a versioned, reversible migration registered in `new()`),
  so an operator runs one migrator for both their event schema and the consensus
  journal; `SqlJournal::migrate` is the equivalent self-contained convenience for
  standalone/test use (identical tables — `VARBINARY(20)` txn / `VARCHAR(64)` meta
  keys keep the primary keys portable to MySQL). ✅ **Group-commit fsync:** the
  `Journal` trait splits into `stage` (buffer a write) + `flush` (one fsync), and
  the node's inbox loop drains up to `MAX_JOURNAL_BATCH` queued messages, stages
  each, then flushes **once** — so a burst of consensus messages costs a single
  fsync instead of one per message, with no weakening of durability (a reply is
  still deferred until the decision behind it is durable). `FjallJournal::flush`
  is the `persist(SyncAll)`; the handler refactor returns deferred, durability-
  gated sends (`handle_staged`/`drive_staged`). A node-level test
  (`inbox_drains_a_burst_into_one_group_commit`) proves 50 staged records collapse
  to one flush, and the deterministic simulation confirms safety/convergence are
  unchanged. **Flush-failure gate:** if the `flush` (fsync) itself *fails* (a disk
  error / full disk), the node now **withholds** that batch's durability-gated
  replies rather than acking a decision it could not persist — so a node that can't
  fsync can never claim durability (a quorum of such nodes restarting would otherwise
  lose an acked write). The withheld replies are exactly the loss quorums/recovery
  already tolerate, so the cluster proceeds via durable peers
  (`flush_failure_withholds_the_durability_gated_ack`). The cluster-level invariant —
  **a write reported committed is durable on a quorum** — is guarded by a storage-fault
  test (`tests/journal_faults.rs`): with a `FaultJournal` failing fsync on a quorum, a
  write must fail to commit rather than ack non-durably (the test fails on the pre-fix
  code, durable on only 1 node). ✅ **Snapshots + compaction + log truncation** (bounds both the
  journal and the in-memory `Replica.commands`/`by_key`, previously unbounded —
  the blocker for long-running deployments). A **redundancy watermark** (mirroring
  Accord's `redundantBefore`): below it every replica has applied everything, so
  the state is redundant. Each node gossips its `applied_through` (lagged by
  `COMPACTION_MARGIN` so a not-yet-propagated commit is never skipped); the
  recovery sweep computes the **per-shard minimum** and `Node::compact`s below it —
  dropping redundant commands (`Replica::compact`, pruning `by_key`) and
  truncating the journal (`Journal::truncate` + a persisted watermark); an applied
  multi-shard transaction leaves a decision record behind until its other shards
  have applied it (see M2). `is_ready`
  treats a dependency missing below the watermark as satisfied (a `Commit`'s
  quorum-unioned deps can name a compacted-away transaction). A still-behind or
  partitioned peer holds the min down (and an unheard-from peer blocks compaction)
  — unless `NodeConfig::peer_absence_limit` is set: a same-shard peer silent that
  long is left out of the min (never below a slow quorum of counted nodes, so a
  stranded minority cannot compact alone). Its last report is kept: a report is a
  proof the peer holds every lower `t0`, so once compaction drops a transaction at
  or above it the peer may have a gap, and the newest such `t0` is persisted as
  the bound it must report above to be counted again (`Journal::save_rejoin_bounds`).
  A message from a peer under such a bound is answered with `Message::Rejoin`
  rather than handled — a stale replica recovering a compacted transaction would
  find no one who knows it and invalidate it — and the told node stops taking
  part (`Node::rejoin_required`, `Node::rejoin_signal`) until its host
  rejoins it from a snapshot; a node that can prove it holds everything (its own
  report is above the bound) answers with that report instead. A peer that was
  silent while nothing was compacted past it is simply counted again
  (`an_absent_peer_stops_holding_compaction_back`,
  `a_minority_never_drops_its_way_to_compaction`, `rejoin_bounds_survive_a_restart`).
  A node that rejoins over its kept journal still holds its own bounds, so a
  contact it gave up on must prove it before the join copies its state: the
  joiner, about to discard its store, would otherwise take a store without a write
  compacted past the contact, which no peer holds as a command any more. It sends
  the contact `Message::Rejoin` first and waits for the report that counts it
  again (`Node::await_reinstated`); asked straight for the join header, it would
  screen the answer as a stale peer's and time out
  (`a_rejoin_through_a_contact_it_gave_up_on_asks_it_for_proof_first`,
  `a_rejoin_never_copies_a_contact_that_cannot_prove_what_was_compacted`).
  A rejoined node is counted again by its first report: the contact follows the
  join header with `Message::JoinProof` — its own report before the holds it keeps
  for joiners (below), taken in the same cut as
  the exported commands — which the joiner, once every page is installed, counts as
  proven coverage (it now holds what the contact held below it; same shard and
  epoch only, never persisted). Without it the joiner's first report would precede
  the `slow_q − 1` peers' coverage a shard of four or more needs, and it would be
  told to rejoin again. Until the joiner reports, the contact holds its own report
  at that vouch (a `JoinHold`, refreshed by every page request and by the
  `JoinKeepalive` the joiner sends every `collect_timeout` while its join runs,
  and lapsing a few page timeouts after the last), so no peer's compaction — and
  bound — passes the joiner's first report while pages cross the wire under a
  write load or install on a slow disk. A hold says nothing of what the contact
  holds, so its vouch ignores them: capped by the hold kept for a joiner's last
  join, the vouch for its next could never rise above the last one, and a joiner
  vouched for below a bound — by a contact that restarted and had not yet applied
  or re-proven what it had compacted past — was told to rejoin again on every
  rejoin (#349). A node still joining ignores `Rejoin`,
  sweeps nothing (a report it gossiped would end its contact's hold), and answers
  no anti-entropy and no read-barrier probe (its coverage and its conflict graph
  lack what the join is importing). Once joined, the coverage it hands an
  anti-entropy requester stays capped at its proven coverage until its own
  anti-entropy has covered everything below the join's completion: a transaction
  committed before its inbox started, not yet applied by the contact when it
  vouched, is in neither the snapshot nor the replayed inbox. A stale replica that
  never rejoined still gets only `Rejoin`
  (`a_rejoined_peer_is_counted_again_in_a_group_of_four`/`_five`,
  `a_peer_rejoining_under_writes_is_counted_again`,
  `a_joiner_with_a_slow_disk_keeps_its_contact_holding`,
  `a_rejoin_through_a_contact_that_vouched_too_low_is_counted_next_time`,
  `a_joined_node_vouches_for_no_more_than_its_join_proved`,
  `a_stale_replica_cannot_sync_its_way_back_in`).
  `applied_through` alone is not enough, though: a replica partitioned *during*
  consensus never witnessed the missed transactions at all, so after healing its
  report would vouch for a window it wasn't connected for and prematurely unblock
  compaction — anti-entropy then races truncation and, losing, leaves that replica
  **permanently** divergent (jepsen `G-single-item-realtime`, issue #228). So each
  node also **clamps its report to proven sync coverage**: a completed anti-entropy
  round records the contact's applied-through as covered, and a node only vouches
  up to the `slow_q − 1`-th best coverage among its shard peers — `{self} ∪
  covered` then intersects every commit quorum (fast quorums meet every majority),
  so every committed transaction below the report is provably held. Until a healed
  or restarted replica re-proves coverage it reports `MIN`, holding cluster
  compaction back (`accord_watermark_clamps_total` counts this), which keeps the
  missing commands exportable and truncating always safe (regression:
  `partitioned_minority_cannot_unblock_compaction_and_diverge`). **Durable
  reports** (#331): a peer may compact past what a report vouches for, and ship it
  no more, so the reporting node must hold it across a restart too. Everything a
  report can count as applied is journaled first: an anti-entropy import is staged
  before it is recorded (unjournaled, a restart restored only the Commit a command
  had been journaled with, and the command waited for good on a dependency the
  node had imported and its peers had since compacted); a local apply's record is
  staged ahead of its store apply, so the store and the in-memory mark stay
  adjacent (a record ahead of a failed apply is only replayed into the store on a
  restart, as any applied record is); and a report the node sends on its own —
  the periodic watermark, a report answering `Rejoin`, a joiner's first report —
  flushes the journal before it leaves (`Node::vouch_durably`). A reply to a
  handled batch already waits for the batch's flush
  (`a_restart_keeps_what_anti_entropy_imported`). **The vote fence**
  (#310): a report — and the floor and coverage built on it — stays true only if
  the replica never votes below it later. Without that, a transaction only R holds
  is compacted past by P, which gave R up; Q, whose floor lags, pre-accepts it on
  R's recovery; R and Q commit it, and P, which ignores a commit below its floor,
  never applies it. So a replica **never newly votes** — PreAccept, Accept, or the
  pre-accept an unrouted recovery asks for — for a transaction it does not hold
  whose `t0` is below its **vote fence** (`Replica::fenced`,
  `accord_votes_fenced_total`): to a recovery it answers "never witnessed"
  (`NotWitnessed { redundant: false }`) and journals the ballot. The fence is
  **what the replica vouched for** (#337): every report, every sync coverage it
  hands a peer (a peer counts it as proof the replica holds everything below,
  and a subscription reads past it), every join it vouches for and the stable
  point its subscriptions read lies at or below it. Raised under the replica
  lock in the same critical section as the vouch — so no vote slips in below a
  report already cut — it is journaled (`Journal::save_vote_fence`, staged and
  flushed with the report or batch it gates) before the vouch leaves, and a
  restart restores it: none goes stale, a wall clock stepped back included. A
  journal that keeps no fence restarts it at the clock bound less the margin,
  where it stood before it was journaled. The fence is no higher than that:
  pinned at its clock less the margin, a replica that never saw a transaction
  fenced it a margin after its `t0`, and once the replica that lost its journal
  was one of the slow quorum that accepted it, nothing could decide it any more
  (#337) — its coordinator gone, the others fenced. So a replica also **vouches
  nothing past a transaction a slow quorum of its shard holds unapplied**: each
  report gossips the sender's applied-through unclamped by coverage
  (`Watermark::applied`), and the sync coverage a replica hands out never passes
  the shard's **applied floor**, the slow-quorum-th lowest of them (a replica not
  heard from counting as nothing applied; `Node::applied_floor`). A replica that
  never saw such a transaction can then still vote for it, however late, and a
  recovery decides it. Coverage never clamps an applied-through, so the floor
  rises with the shard from its first gossip; every replica's reports and stable
  points trail it by one gossip round at most (`min(applied, coverage) ≤ floor`
  for any node). Before deciding a recovery's vote for a transaction it does not
  hold, a replica raises its fence to what it could vouch for now — its
  applied-through, capped at the floor — so it votes late only for one a slow
  quorum may hold, and fences the rest. The original coordinator is held to a
  deadline instead: its PreAccept, and its Accept at `Ballot(t0)`, get no vote
  from a replica that does not hold the transaction past the clock less
  `compaction_margin` (`Replica::late`, `accord_votes_late_total`). No
  coordinator's message outlives the margin, and a replica that voted on its
  coordinator's word did so while its applied-through still lay below the
  transaction — so its gossip shows it holding it. Past that deadline, and
  below no fence, a replica settles a recovery in **two rounds**
  (`RecoveryRound`): to the first it answers `Late`, promising the ballot, and
  neither votes nor refuses. The recovery, which sees every reply, then asks
  it to **refuse** the transaction for good if that proves it never committed
  (`never_commits`, the late replies counted as fenced) — a promise at the
  reserved `REFUSED` ballot, journaled like any promise, that no ballot
  outbids — and invalidates it. If the replies show the transaction may have
  committed instead — a slow quorum accepted it, a replica that lost its
  journal counted as one that did (`Node::may_have_committed`) — it asks it
  to **vote**, and decides it. Otherwise what the replicas it did not hear
  from hold decides it, and a next attempt hears them: no write too few
  replicas hold is committed on late votes. So a write too few replicas hold
  is invalidated once its deadline passes, not once the vote fences pass it
  — across a partition, the gossip that moves them waits for the heal — and
  one an amnesiac and fewer than a slow quorum of the others accepted is
  decided with the late votes
  (`a_recovery_has_late_replicas_refuse_what_too_few_hold`,
  `a_recovery_asks_no_late_vote_for_what_may_not_have_committed`,
  `a_refusal_for_good_outlives_a_restart`). A coordinator's
  messages, and recoveries of a transaction still in flight, land within the
  margin (OPERATIONS: margin ≥ `recovery_timeout` + 2 × skew + delay). Past it, a
  transaction only a minority holds can no longer gather a quorum, and a recovery
  proves it never committed (`Node::never_commits`): with **more than `n − slow_q`
  replicas fenced**, too few are left that ever voted or ever will; with **a replica
  compacted past it and more than `n − slow_q` fenced or holding it uncommitted**,
  that replica's floor rests on at least `slow_q` counted replicas that reported
  above `t0`, a holder reported below it, so a fenced replica is among them — one
  that never held the transaction, which its sync coverage would have brought in
  had it committed. The latter rests on every replica fencing, so it sits behind
  `NodeConfig::fence_invalidation` for rolling upgrades. When neither holds and
  fewer than `slow_q` replicas could still vote — the rest fenced or compacted past
  it — no recovery can decide it, since a compacted replica may have applied it and
  dropped it. Its holder is then **stale**, and requires a rejoin through that
  replica (`Node::rejoin_contact`): its floor is past `t0`, so `forget_below`
  drops the transaction, and the snapshot carries its effect if it had one. A
  holder that holds it committed and decided is not stale, as a replay its
  store refused leaves it (#351): the decision its peers applied before they
  compacted it is here, and only the apply is left. A peer
  that never held what it compacted past sets no rejoin bound, so this is how a
  holder the others gave up and compacted past gets back
  (`a_lone_holder_cannot_commit_what_a_peer_compacted_past`,
  `a_lone_holder_cut_off_from_a_compacted_peer_cannot_commit`,
  `a_lone_holder_stranded_below_its_peers_compaction_rejoins`,
  `a_vote_fence_outlives_a_restart_with_a_clock_stepped_back`,
  `a_replica_vouches_no_coverage_past_a_write_a_slow_quorum_holds`,
  `a_late_coordinator_gets_no_vote_where_a_recovery_does`,
  `a_fenced_answer_waits_for_its_fence_to_be_journaled`, and the simulator's
  absence scenario with orphan votes). **Catch-up:**
  only a brand-new joining node can be
  below the watermark, so `join` transfers a **data-store snapshot** for the
  truncated prefix plus the recent commands and the watermark; anti-entropy stays
  command-based. The header (watermark, coverage) comes first, then the joiner pulls
  the contact's applied commands in `t0`-ordered **command pages**, then **the
  snapshot in pages** (`DataStore::snapshot_page`; both bounded by
  `NodeConfig::snapshot_page_events` / `snapshot_page_bytes`), installing each under
  the watermark's synthetic id as it lands, so a store and a command backlog of any
  size cross in frames far under the transport's cap. Taking the header and the
  command pages before the snapshot pages means every command listed has its events
  in a page. The contact may compact between two command pages: each carries its
  floor, the joiner adopts the highest and drops the commands below it, whose events
  the later snapshot pages hold — so the header's vouch (`JoinProof`) stays sound.
  No join path ships commands or events inline, and anti-entropy replies are
  bounded the same way, so no frame grows with the store. A transaction the contact applies
  meanwhile is in the joiner's buffer too, and its replay finds the events already
  there (an idempotent re-apply). A page overlapping existing events would be
  skipped whole, so `join` refuses a non-empty store, and a join that fails once a
  page is installed is tagged `PartialSnapshot` and not retried on another contact:
  the host wipes the store (not the journal) and joins again. `recover_state` restores the floor
  and trusts the durable data store for the prefix, replaying only the journal
  tail. A replica that lost its store (new disk, deleted
  file) is a joiner too: its host calls `begin_join` before the inbox loop starts
  and `join`s the first contact that answers (a contact still
  joining stays silent, so a half-bootstrapped replica never hands out a partial
  snapshot); `join` fails if the snapshot cannot be installed, and persists the
  adopted floor so a restart keeps dependencies on compacted transactions
  satisfied. **A rejoin keeps the journal** (#287): a replica told to rejoin, or
  that lost its store, rebuilds only the store. Its journal holds its votes and
  ballots, blind recovery promises, config-Paxos acceptor state and clock bound;
  forgetting them would let it vote against a recovery's fence, accept a config
  ballot below one it promised, or mint an id it minted before. `open_joining`
  therefore recovers the journal with `Node::recover_state_for_join`: everything
  `recover_state` restores, but nothing replayed into the empty store, and each
  transaction the journal holds applied as a commit restored decided but
  un-applied. The contact ships the ones it applied too (imported applied again),
  those below its floor are dropped, and the rest are applied again once the join
  completes, since no snapshot page holds their events. Adopting the contact's
  floor `W`, the joiner first drops everything it holds below `W`, whatever its
  status (`Replica::forget_below`): the shard's watermark passed `W`, so each such
  transaction was applied on every counted replica or can no longer commit (a
  counted slow quorum intersects any commit quorum, and a counted replica holding
  it un-applied would have held the watermark below it), and an un-applied command
  there would otherwise clamp the floor for good (`tests/rejoin.rs`). **A replica
  whose journal is lost is amnesiac** (#312): it forgot its votes, promises,
  config-Paxos state, watermark reports and minted ids. Fencing only the old `t0`s
  is not enough — voting on new writes, it would leave what it forgot out of their
  dependencies (T' commits on {A, R}; R and B then commit a conflicting T without
  it), and its last report, still counted by its peers, lets them compact what it no
  longer holds. So it comes back **by joining, gated**: opened joining over an
  empty journal (`recover_state_for_join`), it journals an `AmnesiaFence` at its
  wall clock plus three skews — above every `t0` it can have voted on (no replica
  votes past its clock's reading plus two skews), every ballot it promised and
  every report it made — floors its clock one skew past the wall clock, above every
  id it minted, and joins only through a same-shard, same-epoch contact whose own
  report passed the fence and whose `JoinProof` vouches past it. That vouch rests on
  the contact and `slow_q − 1` peers whose coverage passed the fence: every write
  that committed below it is applied in the state the joiner copies, and none that
  did not can commit any more (those `slow_q` replicas hold nothing un-applied
  below it, and their #310 vote fences refuse it; the forgotten vote leaves fewer
  than a quorum). Its stale reports lie below the fence too. Once joined it is an
  ordinary replica that never votes below its fence for what it does not hold, and
  it abstains from config Paxos — an epoch's promises have no bound in time to
  fence — until it **read the other acceptors' state back** (#338,
  `Message::ConfigRead`, `AmnesiaFence::votes_restored`): what plays for config
  Paxos the role the fence plays for transactions. It asks every other acceptor
  for every epoch's promised ballot and accepted value, and needs answers from a
  majority of *all* the acceptors among those that vote themselves — one that lost
  its journal and has not restored its votes answers `voting: false`, and counts
  for nothing: counting it would let two such replicas each read past the other's
  forgotten promise. Any majority that promised or accepted a ballot meets those
  answers in a sound acceptor, so the merged state (per epoch, the highest ballot
  promised or accepted, and the value accepted under the highest ballot, adopted
  as this node's own) lies above everything it forgot as part of a majority. A
  vote of its own that no majority yet shared is covered by *when* it reads: no
  earlier than `collect_timeout` past its fence, since a proposer counts no reply
  older than that, so every round that counted a reply made before the loss ended
  before the read, and whatever it accepted is journaled at a sound acceptor the
  read meets. The adopted state is journaled before the fence records the
  restoration, so a restart keeps it. The metadata sweep retries the read until
  enough voting acceptors answer; a group that lost a majority of its journals at
  once never restores (two overlapping losses in a group of three: each needs the
  other's answer), and those nodes come back under new ids. While it waits, it
  answers a recovery of a transaction below its
  fence with `Message::Amnesiac` (holds nothing, may have voted, never will): a
  write minted during the rejoin and held only by a minority would otherwise stay
  undecided once the other replicas' vote fences passed it, pinning the very
  report the join waits on; a recovery invalidates on it only when, counting every
  replica not heard from as one that may have voted, too few may have accepted it,
  too few of the electorate pre-accepted it, and too few can still vote — a holder
  that accepted it may hold half a decision, and keeps it undecided. A plain open
  over an empty journal and a non-empty store is refused (`LostJournal`), and a
  fresh one leaves its clock bound as a birth mark. A transaction accepted by the
  lost replica and by fewer than a slow quorum of the others needs the vote of a
  replica that never saw it: the lost replica's journal held it unapplied, so
  its last gossip still pins the shard's applied floor below it, none of the
  others vouched past it, and their vote fences let a recovery decide it (#337,
  `a_recovery_decides_what_an_amnesiac_and_one_replica_accepted`). A
  transaction only the lost replica held is no one's to recover, yet a write
  committed with it as a dependency waits on it: the replicas that wait learn
  its decision with a keyless round on their own shard
  (`RecoveryRound::Dependency`, `Node::learn_decision`), which nothing can vote
  for once its deadline passed, so it never committed. They invalidate it, and
  an `Invalidate` records it aborted on a replica that never witnessed it,
  keeping the ballot it promised, so what waits on it goes on
  (`a_dependency_only_an_amnesiac_held_is_invalidated`). Two cases
  still stay undecided for good, and the join never finds a contact past them
  (OPERATIONS §7): a **multi-shard** transaction of that shape — a routed
  recovery never lets a replica that never witnessed it pre-accept it (#377) —
  and a replica whose only vote for it was a recovery's late pre-accept, lost
  before its next gossip showed it holding the transaction. See `tests/amnesia.rs` and
  the simulator's `run_amnesia`, which wipes a node at any moment. **A journal rolled
  back to an older copy** (#339) — a backup or VM snapshot restored, a disk that lost
  writes it acknowledged — forgot the votes and promises made since, like a lost one,
  yet holds something, so no amnesia fence covers it. Every watermark report carries
  its sender's **durable mark** (`DurableMark`): its journaled clock bound and vote
  fence, which only rise and are journaled before anything cut below them leaves the
  node, so a journal reopened as it was left never holds less than a peer saw — a
  journal that keeps no fence restarts it at the bound less the margin, still above
  every fence it reported. Each node keeps the highest mark it saw from each peer,
  journaled with its reports (`Journal::save_peer_marks`). A node opened over a
  journal that holds something, in a group of more than one node, asks every other
  node to confirm it (`Message::Opened`): until a slow quorum of its shard, itself
  included, did, and every other node answered or `collect_timeout` passed, it
  buffers consensus messages and mints, votes and vouches for nothing
  (`Node::confirmed`; the guard sits in `bound_clock_past`, which every mint, vote and
  vouch passes). Nodes answer from the marks their journals kept, confirmed or not,
  so a full-cluster restart confirms them all. A peer that saw more answers
  `RolledBack`, however late, and the node stops (`Node::rolled_back`): its host
  opens it again over an empty journal and store, joining, as a lost journal comes
  back. A replica that lost its journal starts a new life, and its mark (the life is
  its amnesia fence) supersedes every mark of an earlier one. A copy that lost only
  what followed the last report its peers heard escapes, and so does one taken before
  the node first journaled its fence that lost only what followed the bound's last
  rise: the restored fence covers every fence the node reported. See the rollback tests in
  `tests/amnesia.rs` and the simulator's `run_rollback`; the swarm counts any node
  found rolled back as a false alarm. Validated by the deterministic simulation: a **bound oracle**
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
  ✅ **The clock never runs back across a restart.** Every id a node mints — a
  transaction, a ballot, a read probe — goes through `Node::mint`, which keeps it
  below a **clock bound** persisted in the journal (`Journal::save_clock_bound`),
  raised durably before a stamp reaches it to `2 × max_skew` past the *wall clock*
  (so restarting from it cannot compound, and it costs at most one write per skew
  of wall time). `recover_state` starts the clock at the bound — or, for a journal
  without one, past the node's own journaled transactions — so a restarted or
  reopened node never mints an id it used before, even when its wall clock stepped
  back: replicas would take such a write for the old transaction and ack it with
  the old outcome, its events never applied. A restart within a skew of the last
  mint waits out what puts the clock further ahead than `max_skew`; a larger lead
  means the wall clock stepped back, and the clock leads it until it catches up,
  as it would without the restart. A mint is also never below the group's
  compaction floor (a lagging wall clock, a fresh joiner): a transaction under it
  would count as applied at a replica that never saw it. A joiner that lost its
  journal starts past its own transactions a peer ships back. The one id not
  covered is a ballot outbidding a peer's from beyond the skew bound (a faulty,
  far-future clock): covering it would drag this node's clock out with the peer's.
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
  Three guards keep a live but slow coordinator from being suspected: messages
  closer than 50 ms count as one beat (a write's burst would otherwise teach the
  detector a millisecond rhythm), `NodeConfig.acceptable_pause` is added to the
  expected interval, and a peer's silence runs only until another peer was last
  heard from (a node whose own inbox or host stalled hears from no one and so
  blames no one). Sim tests pin both: a coordinator quiet for 700 ms keeps its
  transaction, and a node cut off from every peer recovers nothing.
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
        unapplied command. **Re-validated after the Phase D/E work** (fast-path
        electorate + recovery-quorum gate, config-Paxos liveness, per-node TLS, and the
        journal relocation to `evento-fjall`/`evento-sql`): a fresh `make jepsen` run is
        **strict-serializable / `Everything looks good!` at conc 10** with
        `--linearizable-reads`, and the default (no read barrier) reproduces exactly the
        documented `:G-single-item-realtime` read anomaly with **no write anomaly** — so
        the cumulative changes preserved write serializability and, with the barrier,
        strict serializability. *Remaining:* clock-skew (off by default — bumps the shared
        kernel clock, needs real VMs; the node image now ships a compiler for jepsen's
        time helper and `run.sh` refuses it under Docker without `ALLOW_CLOCK_SKEW=1`);
        then the box can be checked.
  - [ ] **Independent expert review** of the protocol and implementation.
  - [~] **Code review (2026-10-20).** A full read of the crate found, and fixed with
        a failing test first:
        a transaction left `Reading` when its decision was lost could never be decided
        (replicas never re-answered a recovery's Commit);
        Accept echoed the coordinator's deps instead of reporting conflicts below the
        raised timestamp (divergence at N ≥ 5 or with a shrunk electorate);
        recovery fenced neither unknown replicas nor late PreAccepts, did not journal
        its promise, could commit a value no quorum accepted, and let a pre-accept
        vote tie the original coordinator's accepted ballot;
        a failed data-store apply was marked applied (lost events), and a failed
        version read voted "condition holds";
        subscriptions on a cut-off replica skipped events (now clamped to proven sync
        coverage, like the compaction report);
        anti-entropy always contacted the same peer (a shared counter's parity);
        a dropped `write` future pinned the watermark for good;
        duplicated replies could complete a PreAccept or config-Paxos quorum;
        concurrent `GroupHost::open`s of one group left a deaf node;
        an anti-entropy import applied past a failed command;
        events kept their client-side provisional stamps, so two hosts' skewed clocks
        could order an aggregate's versions backwards in subscriptions (each event is
        now re-stamped at apply from its txn's `execute_at`, floored to the
        millisecond, and the watermark bounds an un-applied command by `t0` floored
        likewise — a release note, not a format bump: a store applied by an older
        build keeps its client stamps);
        a host rejoining a group of N ≥ 4 was told to rejoin again (its first report
        predated the two-peer coverage proof — now the contact vouches for it), and
        at any N a long join under writes was too (peers compacted past the vouch
        meanwhile — now the contact holds its report until the joiner's; and a
        `Rejoin` answering the joiner's mid-join replies aborted the join);
        multi-shard recovery could not see a shard that never witnessed the txn
        (or answered after `fast_timeout`) and committed it on the others only —
        a multi-shard txn now carries its **route** (full key set) on every
        message (`Message::Routed`, single-shard traffic unchanged), journaled with
        the command; recovery waits for a slow quorum of every route shard, and
        invalidates the txn when a slow quorum of one shard never witnessed it
        (`tests/recovery.rs`, see M2 below); and a recovery re-driving a decided
        txn that one shard had applied and compacted re-read the other shards
        alone, which could contradict the decision — it now takes the decision
        from a replica that applied it;
        an anti-entropy reply or join header over the 64 MiB frame cap was shed
        forever (now an anti-entropy reply is a budgeted execution-order prefix whose
        coverage stops below the oldest `t0` it left out, a join pulls its commands in
        pages, and a write whose command could not fit a frame is refused,
        `CommandTooLarge`).
        A second round (2026-10-21) fixed what the first only listed, and what it
        turned up: a layout with no shard, an empty one or a node in two could be
        decided, persisted and installed (a divide by zero in every inbox loop, or
        keys no replica can commit) — now refused before a vote, and never persisted
        or installed; a config vote went out although the journal refused it, and
        an epoch was installed although the metadata log refused it; a forwarded
        read the owner's store failed, or that a joining owner served from a partial
        store, came back as an empty or partial page (now `ReadFailed`); a restart,
        or a group reopened by `GroupHost`, could mint a transaction id or ballot it
        had used before, and a node with a lagging clock minted below the compaction
        floor (now the clock bound, see Phase C); `GroupHost::close` returned while a
        sweep or the inbox loop still used the store and journal (now async, and the
        closed node mints nothing more); and the mux could unregister a reopened
        group's inbox for its closed predecessor.
        A third round fixed what #277 and #281 left open on the join paths: a node
        told to rejoin dropped a forwarded read, which failed only at
        `collect_timeout` (now `ReadFailed` at once); a joining node answered a
        read barrier's probe from a partial conflict graph (now silent, so it
        is not counted); a joined node's anti-entropy coverage could vouch for a
        transaction it never received (now capped at its join proof until its own
        anti-entropy covers the join); a join whose installs outlasted the
        contact's hold lost it (now kept alive by `JoinKeepalive`); and a
        `GroupHost::open` dropped mid-join orphaned the joining node's inbox loop
        (now unregistered and stopped).
        A fourth fix closed two inbox-loop leftovers of #281: a reply went out
        although the journal refused the stage behind it — a vote, promise,
        route or apply (now withheld, like a batch whose flush failed; a refused
        route is forgotten so the next routed message stages it again); and two
        config changes for one epoch on one node shared a reply slot keyed by the
        epoch, so one took or tore down the other's replies (now one slot per
        proposal, every reply fanned out, and a nack counted only by the proposal
        whose ballot it outbids).
        A further fix made the sweep scheduler fair: it handed its
        `max_concurrent` slots out in fixed map order, so under saturation the
        same groups won every tick and the rest went unswept (no recovery,
        anti-entropy or compaction), and an idle group that missed its slot
        waited a whole `idle_interval`; now a deferred group stays due and, when
        slots are short, those swept longest ago go first.
        Another fix let a replica that fell behind catch up: the node that
        answered nothing for the rest of a Jepsen run (#291) had been cut off
        while the others kept committing, not stalled by its TCP writer. Every
        commit it then received waited on what it had missed, and each Commit
        or Apply re-walked every waiting command's dependencies (the execution
        scan was O(waiting × deps)), so its inbox fell ~400× behind and the
        replies its repair needed timed out in the queue; its recovery sweep
        meanwhile re-drove each waiting command, up to `collect_timeout`
        apiece, before reaching anti-entropy, the only way back. Now a pending
        command found waiting on a dependency is parked until that dependency
        changes (`Replica.blocked`), the sweep skips parked commands, and
        anti-entropy runs between recoveries on its own cadence: a node cut off
        under load serves again within ~2 s of the heal (`isolated_catch_up`).
        Anti-entropy imports a dependency only once it is applied somewhere,
        though, and where it is held it may itself wait on this replica — on the
        read of an earlier transaction parked here on it — committed and parked
        there too, so its holders never recover it: the swarm found such a
        deadlock (#330). So the sweep now learns, for a few parked dependencies
        never witnessed here each pass, the oldest first, their decision from
        the shard's replicas (`Node::learn_decision`): one committed report
        carries it whole, and it is committed here as any Commit is. A
        dependency no replica reports committed is left to those holding it,
        and one invalidated is applied there, so anti-entropy brings it in
        (`a_replica_recovers_a_dependency_it_never_witnessed`).
        Recovery's fast-path check now counts the replicas' own PreAccept
        votes (#285, see M2). Before, a txn whose later conflict had
        fast-committed without it, but was still only pre-accepted at every
        replica heard from, was re-proposed at `t0`. It then ordered before a
        write that never waited for it.
        A recovery that skipped a compacted shard dropped the Applied report
        of that shard's straggler, and re-read the other shards alone: it
        could commit what the compacted shard aborted. It now takes the
        decision from that straggler (#305, see M2).
        A decided multi-shard txn whose shard compacted it on every replica
        stayed undecided on its other shards, pinning their watermark and
        backlog; compaction now keeps its decision until every route shard
        applied it (#286, see M2).
        A rejoin discarded the journal with the store, so the replica forgot
        its in-flight votes, recovery promises, config-Paxos promises and clock
        bound; a rejoin now keeps the journal and rebuilds only the store
        (#287, see Phase B).
        A replica that lost its journal had to come back under a new node id.
        Rejoining under
        its old id over an empty journal, it voted with what it remembered
        (a new write could commit without the forgotten one among its
        dependencies, and a recovery invalidate a decided write on its
        "never witnessed"), and its stale report let peers compact what it no
        longer held. Its join now waits for a contact vouching past an
        amnesia fence (#312, see Phase B).
        A healed cluster with transactions in flight took tens of seconds to
        serve again (#326). Each node's sweep recovered them one at a time,
        in `t0` order, and a recovery whose read a replica held back on a
        dependency still stalled waited out `collect_timeout` for it: often
        one the slow path had placed before it with a higher `t0`, or one read
        and never applied, its coordinator gone. Each link of such a chain
        cost a `collect_timeout` (5 s by default), and a read taken once the
        chain cleared reached a recovery that had given up. Now a sweep
        recovery gives a read up as soon as this replica holds it back on a
        stalled transaction, takes that one over first (recovered, or its
        decision learned if never witnessed here), then comes back
        (`Node::recover_stalled`). And as a replica answers the reads and the
        apply to whoever committed last, a coordinator or a recovery waits for
        the reads only until this node applied the transaction, and a recovery
        for the acks too: of two recoveries racing, one heard nothing
        (`tests/stalled_recovery.rs`). Over 60 000 swarm seeds, those needing
        more than 20 `collect_timeout`s to serve again went from 30 to none.
        A replica given up while absent could be told to rejoin on every
        rejoin, for good (#349). Its contact had restarted: its sync coverage
        re-proven through a peer still applying again an old transaction, or
        its store failing what its journal replayed, it reported below its own
        bound for the joiner, so it vouched that, and told the joiner to rejoin
        once more. The hold it kept on its report for that join then capped
        the vouch for every next one, as the vouch was the report, holds
        included. A join header now vouches for the contact's report before
        its holds (`Node::vouch_locked`). The two swarm seeds that stalled this
        way are pinned (`a_replica_rejoining_through_a_restarted_contact_is_counted_in`).
        A replica back from an absence caught up one anti-entropy page per
        round (#348): with the smallest pages, a round for each write it
        missed, the slowest heals the swarm found. A round now asks its peer
        for the next page at once while each brings something in, up to 16
        pages and for one `anti_entropy_interval`
        (`a_healed_peer_catches_up_a_backlog_in_a_few_rounds`).
        A replica restarted over a store that refused its journal's replay
        was sent to rejoin by its own sweep (#351). The replay hands such a
        write back decided but un-applied. Once it was older than
        `recovery_timeout`, the sweep took it over, and with the peers
        compacted past it the recovery found too few replicas left that could
        vote, and required a rejoin: a full snapshot transfer, when all the
        node lacked was the apply. A transaction held committed and decided
        needs nothing a recovery brings (`Replica::decided`): the sweep now
        leaves it to the retried apply, and follows a read waiting on one to
        what that one waits on, and no recovery of one sends its holder to
        rejoin (`a_replayed_write_held_decided_waits_for_its_store`).
        A node rejoining over its kept journal through a contact it had given
        up on lost the contact's join header (found while retiring the inline
        join paths, #296). The journal still held the contact under a rejoin
        bound, so the joiner screened the header's answer as a stale peer's:
        answered `Rejoin` and dropped it. The contact's report then counted it
        again, but only after the attempt had timed out (`collect_timeout`,
        5 s by default); until #296 an inline-header retry to the same
        contact hid it. The join now asks such a contact for that report
        first (`Node::await_reinstated`): one round trip for a contact that
        caught up, and nothing copied from one that cannot prove it holds
        what was compacted
        (`a_rejoin_through_a_contact_it_gave_up_on_asks_it_for_proof_first`).
        Over 60 000 swarm seeds, headers lost this way went from 38 to none.
        A transaction carries the epoch it was coordinated under, and is
        voted on and recovered under that epoch's layout: before, a recovery
        after a layout change counted the replicas installed since, and could
        invalidate a committed transaction, across shards (#288) or within one
        (#359) (`recovery_asks_the_replicas_a_transaction_was_coordinated_under`,
        `a_routed_recovery_counts_shards_under_the_transactions_epoch`).
        A change after which a new quorum could miss an old commit quorum
        drains the writes in flight on the keys it moves before it installs
        the new layout (two-phase changes, #359;
        `replacing_a_dead_replica_drains_a_write_in_flight`).
        Keys move between shards: a slot table places them, a split moves
        only the slots the new shard takes, and every node gaining slots
        copies them while the transition drains
        (`a_key_range_moves_to_a_new_shard`,
        `an_existing_member_gains_a_range_with_its_versions`); a join goes
        through a replica of the joiner's own shard only (#360). #288 is
        closed.
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

- **Phase E — Production readiness.** ✅ The items that make it safe to run, not
  just correct in a lab — **all six done** (cert *issuance* stays an external-CA
  concern; see PKI below):
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
        holds. **Liveness under contention** (concurrent proposals for one epoch used to
        duel ballots and all fail — the sweep's drive-to-completion runs on every node)
        is provided **without a leader or lease**: a deterministic *distinguished
        proposer* per epoch (`config_rank` = `members_sorted[epoch % n]`, members sorted
        by `NodeId`) proposes first while non-preferred automatic recoveries defer by
        `rank * config_defer_step` and skip if it already committed; and `run_config_paxos`
        **yields on a `ConfigNack`** — backs off (rank/attempt-asymmetric, so racers
        desync) and adopts the committed layout instead of escalating ballots forever.
        Safety is untouched (acceptor logic, ballot monotonicity, and adopt-highest-accepted
        are unchanged; this only reorders *who proposes when*), and the delays are
        deterministic so the simulation stays bit-reproducible. `tests/membership.rs`
        proves N-node concurrent recovery of one epoch and two racing operator changes
        both converge (they fail without this). `append_metadata` is now genuinely
        idempotent (first decided layout wins) per its contract.
  - [x] **Metrics export** — `MetricsSnapshot::to_prometheus` /
        `to_prometheus_labeled` render the counters in **Prometheus text exposition
        format** (`accord_*_total` counters, optional labels e.g. the node id),
        hand-rolled so the crate stays **dependency-free** — the operator wires the
        `/metrics` HTTP endpoint and serves the string, alongside the existing
        `tracing` events. (A live `prometheus`-crate registry behind an optional cargo
        feature is the future path if ever wanted; unwarranted for 9 plain counters.)
  - [x] **PKI / per-node identities** — mutual TLS now **authenticates each peer's
        identity by certificate pinning** (`serve_tls_verified` + `TlsClient::with_peer_certs`,
        `tcp.rs`): the operator supplies a `NodeId → leaf-cert` map (`PeerCerts`), and a
        connection is bound to the `NodeId` whose pinned leaf it presents — matched by
        DER byte-comparison, so **no x509-parsing dependency**. On a verified inbound
        connection the authenticated id, **not** the self-declared wire `from`, is
        stamped on every `Envelope`, so a node can only act as itself (no forged
        ballots/votes/commits); a CA-valid but **un-pinned** certificate is refused
        outright. The client side pins each peer too, defeating a CA-valid impostor at
        a peer's address. `tests/tls_cluster.rs` gives every node its **own** leaf and
        proves replication + one-winner conflict resolution over per-node-pinned mTLS,
        and that an un-pinned outsider's frames are never handled while a pinned peer's
        are. The trusted-network `serve_tls`/`TlsClient::new` paths are unchanged.
        *External/operator concerns (not code here):* cert **issuance** (an external CA)
        and **hot rotation** (swap the acceptor/connector + pin map; a possible future
        helper).
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
        A new payload *type* is **additive** instead — a fresh `RecordKind`, no version
        bump (the version byte is shared with the journal, so bumping it for a wire-only
        addition would invalidate every journal on disk): `GroupFrame` (kind 6, the
        multiplexed wire frame of the many-groups transport, since removed) was added
        this way, and its byte stays reserved so it is never reused. Format 2: events
        carry their tenant (unreleased, so no reader for format 1).
  - [x] **Snapshot-at-scale** — `ExecutorDataStore::snapshot_page` reads the backend
        one `SNAPSHOT_PAGE_SIZE`-event page at a time and trims it to a byte budget,
        and a joiner pulls those pages one request at a time
        (`SnapshotPageRequest` / `SnapshotPage`), so a store of any size is handled
        without ever building it in memory or in one frame. (`version` is an indexed
        lookup; `snapshot` still folds the pages for the legacy inline bootstrap.)
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
**Phase E complete** — format-versioning, Prometheus metrics export, snapshot-at-scale,
backpressure, the durable replicated metadata log, and per-node TLS identities (cert
pinning) are all in; cert issuance/rotation stay external concerns — with Phase D
partial) — and a strong base to *take* to production. It is suitable for
prototypes, demos, and controlled/low-stakes use. It is **not** yet safe for
production: external/adversarial verification has only just begun (a Jepsen/Elle
harness now exists and its first partition run already found that **reads are not
linearizable** by default — writes are serializable & atomic, but a read off a lagging
replica can break real-time order; an opt-in `linearizable_reads` read-index barrier
makes reads linearizable and the cluster strict-serializable under quorum-preserving
partition — Jepsen-validated to concurrency 10, after the harness found and fixed an
anti-entropy convergence bug), and there is still no independent review and no
real-cluster soak mileage. The membership/metadata layer is now a durable, replicated
metadata log (Phase E) with replay, restart-durability, and contention liveness (a
deterministic distinguished proposer + yield-on-conflict — no leader/lease needed).
The gating items are the remaining unchecked
boxes in Phase D above — independent expert review and real-cluster soak (verification
and soak first). The crate
version (`2.0.0-alpha.*`) reflects this.

The event-data path is already backend-agnostic (`AccordExecutor` runs on any
`evento_core::Executor`); Phase B extends that to the consensus state.
