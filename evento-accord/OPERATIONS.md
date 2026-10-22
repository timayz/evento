# evento-accord — Operations Runbook

How to deploy, configure, monitor, scale, and recover an `evento-accord` cluster.
This is the *ops* companion to [`DESIGN.md`](./DESIGN.md) (which covers the protocol).

> **Status.** `evento-accord` is a well-tested **reference implementation**
> (`2.0.0-alpha.*`), **not yet production-hardened** — it still lacks independent
> review and real-cluster soak (see the verdict in `DESIGN.md`). Treat this runbook as
> the path for prototypes, demos, and controlled/low-stakes deployments.

Worked, copy-pasteable deployments live in the examples:
- `examples/bank-axum-accord` — a node over TCP (single-node or a 3-node localhost
  cluster), with `/metrics` and `/health` endpoints.
- `examples/bank-axum-accord-tenants` — **many consensus groups per process**: one
  SQLite file and one Accord group per tenant, tenants created at runtime through a
  replicated catalog, over one multiplexed transport (§10).
- `examples/jepsen-node` — a node over real TCP with a durable `FjallJournal`.

---

## 1. Cluster model

- **Replication:** events are replicated across `N = 2f + 1` nodes; the cluster
  tolerates `f` node failures (writes need an `f+1` quorum). Use **N = 3** (f=1) or
  **N = 5** (f=2). Even `N` gives no extra fault tolerance — always use odd `N`.
- **A node** = one process wiring six injected pieces (all `api`-trait objects):
  `Topology` (membership), `Clock` (`HybridLogicalClock`), `MessageSink` (transport),
  `DataStore` (the event store, via `ExecutorDataStore` over an evento backend),
  `Journal` (the durable consensus log), and a `NodeConfig`.
- **Sharding (optional):** `ShardedTopology` partitions the key space into disjoint
  shards, each its own `N=2f+1` replica set; a transaction reaches a quorum in every
  shard it touches.

---

## 2. Bootstrapping a cluster

Each node needs: its `NodeId`, the membership, a transport bound to its address, a
durable journal, and a local event store. Minimal shape (see
`examples/bank-axum-accord/src/main.rs::build_cluster_node`):

```rust
let topology = Arc::new(StaticTopology::new(id, all_ids));      // fixed membership
let sink     = Arc::new(TcpTransport::new(id, peer_addrs));     // outbound
serve(listener, inbox_tx);                                      // inbound accept loop
let store    = Arc::new(ExecutorDataStore::new(fjall.clone())); // evento backend
let journal  = Arc::new(FjallJournal::open(journal_path)?);     // durable (see §8)
let node = Node::new(id, topology, clock, sink, store, journal).with_config(cfg);
node.start(inbox_rx);        // inbox loop (consensus)
node.start_recovery();       // periodic recovery + anti-entropy + compaction sweep
let executor = AccordExecutor::new(node, fjall);                // drop-in evento::Executor
```

- **Durable journal is required for production.** `InMemoryJournal` (used by the demos)
  loses consensus state on restart. Use `evento_fjall::FjallJournal` (`evento-fjall`'s
  `accord` feature) or `evento_sql::SqlJournal` (`evento-sql`'s `accord` feature).
- **App code is unchanged:** `AccordExecutor` is a drop-in `evento_core::Executor`, so
  `evento::create()/append()`, reads, subscriptions, and snapshots work as usual.
  An event's stored `timestamp`/`timestamp_subsec` is its transaction's agreed
  execution time (milliseconds), the same on every replica — not the writing host's
  clock, so a skewed host cannot reorder an aggregate's versions.
- **The sweep is required, not optional:** besides recovery and compaction, it runs
  the anti-entropy rounds that prove what a node holds, and a node's subscriptions
  only process events below the window it has proven (§5). A node that never sweeps
  (`start_recovery`, or a `SweepScheduler` for many groups) never releases events
  to its subscriptions. A single-node group needs no proof.
- **Static vs dynamic membership:** use `StaticTopology` for a fixed cluster;
  `DynamicTopology` if you need online membership/shard changes (§6).

---

## 3. Configuration (`NodeConfig`)

`Node::with_config(NodeConfig { .. })`. Defaults target a low-latency LAN; **raise the
timeouts for geo/WAN** deployments.

| Field | Default | Raise when / meaning |
|-------|---------|----------------------|
| `fast_timeout` | 50 ms | Wait for the fast-path quorum before falling back to slow path. Keep ≳ one cross-region RTT for WAN. |
| `collect_timeout` | 5 s | Cap on the later phases (Accept/Read/Apply). Only bounds a genuinely stuck phase. |
| `recovery_interval` | 100 ms | How often the recovery/anti-entropy/compaction sweep runs. |
| `recovery_timeout` | 300 ms | A txn unapplied this long is presumed stalled and recovered. Keep well above normal write latency. |
| `anti_entropy_interval` | 250 ms | Pace of the anti-entropy rounds (one peer per round, in turn). Each round also proves the window a node holds, which both its compaction report and its subscriptions wait for: subscriptions trail the present by about `compaction_margin` plus `(slow quorum − 1)` rounds. Keep it well below `compaction_margin`. |
| `compaction_margin` | 1 s | Lag on the redundancy watermark; keep above clock-skew bound + propagation latency. |
| `max_journal_batch` | 128 | Messages drained per group-commit fsync (latency vs. fsync amortization). |
| `phi_threshold` | 8.0 | Phi-accrual suspicion level; higher tolerates more latency variance before suspecting a coordinator. |
| `acceptable_pause` | 500 ms | How long a live peer may go quiet beyond its usual heartbeat interval (scheduler stall, slow fsync, loaded host) before suspicion starts to rise. |
| `linearizable_reads` | false | Opt-in linearizable reads (§10) — one consensus round per read. |
| `max_commands` | 100 000 | Backpressure: refuse *new* local writes (`BacklogAtCap`; `WriteError::Unavailable` through `AccordExecutor`) once the un-compacted backlog hits this (e.g. compaction stalled under a partition, or a peer down with `peer_absence_limit` unset). |
| `peer_absence_limit` | none | Stop waiting for a same-shard peer silent this long: compaction then proceeds without it (§7, down node). Set it well above any restart or maintenance window; a peer given up on while writes went through must rejoin from a snapshot on return. Never leaves fewer than a slow quorum counted. |
| `snapshot_page_events` | 4096 | Most events this node puts in one snapshot page when a joiner bootstraps from it (§7, lost store). |
| `snapshot_page_bytes` | 4 MiB | Byte budget of one snapshot page, command page or anti-entropy reply (a single larger event or command still crosses alone). Keep it far below the transport's 64 MiB frame cap and small enough that one group's bootstrap does not hold the link it shares with every other group for long. |
| `config_defer_step` | 100 ms | Control-plane anti-dueling delay for concurrent reconfigurations. |

Clock-skew tolerance is on the clock: `HybridLogicalClock::with_max_skew(..)` (default
`MAX_SKEW_MICROS`); keep `recovery_timeout`/`compaction_margin` above it. The node
persists a clock bound in its journal (raised at most once per `max_skew` while it
mints) and a restart starts its clock there, so a node never reuses a transaction id
or ballot across a restart, even if its wall clock stepped back. A restart within
`max_skew` of the last write waits out at most `max_skew`; after a backward step the
node logs that its wall clock is behind its clock bound and its stamps lead the wall
clock until it catches up. Writes fail rather than use an id the bound does not cover
(the journal refused it).

---

## 4. Security (inter-node TLS)

Inter-node traffic is plaintext by default (fine for a trusted network). For untrusted
networks, use **mutual TLS with per-node certificate pinning**:

- Server side: `serve_tls_verified(listener, inbox, acceptor, peer_certs)` — binds each
  connection to the `NodeId` whose pinned leaf cert it presents (authenticated identity
  overrides the wire sender, so a node can only act as itself; a CA-valid but un-pinned
  cert is refused).
- Client side: `TcpTransport::with_tls(id, peers, TlsClient::new(connector, name).with_peer_certs(pins))`.
- `PeerCerts` = `HashMap<NodeId, CertificateDer>` — the same map on every node.

Cert **issuance/rotation** is an external-CA concern; supply your own rustls
connector/acceptor. See `tests/tls_cluster.rs` for a full per-node-cert setup.

---

## 5. Monitoring

**Metrics.** `node.metrics()` returns a `MetricsSnapshot`; `to_prometheus()` /
`to_prometheus_labeled(&[("node", id)])` render the Prometheus text exposition format.
Serve it on an HTTP endpoint (see `bank-axum-accord`'s `/metrics`). Counters:

| `accord_*_total` | Watch for |
|------------------|-----------|
| `writes_committed` / `writes_conflicted` | throughput; a high conflict ratio = contention on the same aggregate/version. |
| `fast_path` / `slow_path` | a rising `slow_path` share = conflicts or an electorate that can't form a local fast quorum (check placement/latency). |
| `recoveries` | should be ~0 in a healthy cluster; sustained > 0 = a coordinator is flapping/dead. |
| `compactions` | should advance over time; **flat** = compaction stalled (a partitioned/behind peer holds the watermark) → backlog grows toward `max_commands`. |
| `watermark_clamps` | > 0 while a node re-proves sync coverage (post partition-heal/restart — compaction waits for anti-entropy to repair it); **sustained** growth = coverage never accrues (anti-entropy rounds failing) and compaction stays stalled. |
| `journal_flushes` | group-commit activity; **0 while writing** = the journal can't fsync (see §9). |
| `messages_handled` | liveness/traffic. |
| `messages_shed` | inbound backpressure drops; sustained > 0 = a node is overloaded or a peer is flooding. |
| `messages_unrouted` | (`MuxTransport` only) inbound frames for a group this host has not registered that were dropped — parked past the pending-buffer TTL or over its bounds. Steady > 0 = peers address groups this host never opens (a tenant evicted here and never re-opened, or a stale/unknown group id). |
| `peers_absent` | shard peers this node stopped waiting for (`peer_absence_limit`); each is a node that was silent for that long. |
| `rejoins_required` | a peer told this node to rejoin: it had been given up on while writes it never applied were compacted. The node is stale until its host rejoins it (§7). |
| `snapshot_pages` | snapshot pages this node served to joining peers; a joiner pulls one per `snapshot_page_events` / `snapshot_page_bytes` of the contact's store. |
| `command_pages` | command pages this node served to joining peers: a joiner pulls the contact's un-compacted applied commands one page at a time, before its snapshot pages. |
| `joins_partial` | joins of this node that failed after a page was installed: the store holds a prefix no contact can complete, and must be emptied before the next join (§7). Steady > 0 = a contact that cannot read its backend, or a link losing pages faster than the per-page retries cover. |
| `execution_failures` | data-store applies and commit-condition reads that failed. The transaction is neither marked applied nor acknowledged, and is retried on the next drive (a message, or the sweep). Occasional = transient lock contention; **sustained** = this replica's store cannot take writes (full disk, broken connection), so it falls behind until it can. |

**Health/readiness.**
- *Liveness:* a simple `200` while the process is up (`bank-axum-accord`'s `/health`).
- *Readiness* (is this node caught up enough to serve fresh reads?): derive from
  metrics — `recoveries` not climbing, `compactions` advancing, `messages_shed` ~0, and
  the local backend's applied version tracking the cluster. For strict freshness, use
  `linearizable_reads` (§10) rather than a readiness gate.

**Subscription lag.** A node's subscriptions process events below its *stability
watermark*: the present less `compaction_margin`, below any un-applied transaction it
knows, and below the window anti-entropy has proven with enough peers (a slow quorum,
counting itself). A node cut off from part of its peers' writes cannot vouch for them,
so after a start, a restart or a heal its subscriptions wait for a sync round instead
of advancing past events still on their way. Sustained `watermark_clamps` growth
means those rounds keep failing, and the subscriptions on that node are stalled.

Suggested alerts: `recoveries` rate > 0 for several minutes; `compactions` flat while
`writes_committed` climbs; `messages_shed` rate > 0; backlog approaching `max_commands`;
`execution_failures` rate > 0 for more than a few seconds.

---

## 6. Scaling (membership & re-sharding)

Requires `DynamicTopology`. Membership changes are decided by a durable, replicated
metadata log (config-Paxos); concurrent reconfigurations converge without a leader
(distinguished proposer + yield). A layout must have at least one shard, no empty
shard, and every node in exactly one shard (`validate_layout`): `change_topology`
refuses anything else before a vote, and a node never installs it.

- **Add a node:** start the new process with `begin_join()` (it buffers consensus
  messages), drive `change_topology(epoch, new_layout)` from any current member, then
  the joiner calls `join(contact)` to pull a snapshot + recent commands and replay the
  buffer. (See `tests/membership.rs`, `tests/resharding.rs`.)
- **Remove a node:** `change_topology(epoch, smaller_layout)`; the smaller cluster keeps
  serving. Reconfiguration survives the coordinator crashing mid-change (another node
  completes it via `recover_topology`) and works after founders have left (the acceptor
  set tracks current membership).
- **Re-shard (move a key range):** a config change into more shards; the new shard's
  nodes bootstrap only their range. *Note:* GC of a moved range from old owners is not
  implemented (evento's event store is append-only) — old replicas simply stop being
  contacted for it.
- **No transaction in flight across a layout change (until #288 ships).** An epoch
  change does not yet hand over the transactions in flight across it: replicas judge
  ownership, and recoveries plan, by the layout installed when a message lands, not
  by the one the transaction was coordinated under. So, before `change_topology`:
  1. stop writes to the keys that move (for a group, its writes);
  2. let compaction pass them: every replica's `command_count()` back to zero;
  3. change the layout, then resume writes.

  Re-sharding among existing members (a member gaining a range it did not hold) is
  unsupported: only a joining node bootstraps, so the member gets no data for the
  range. Meanwhile two guards narrow the exposure: a coordinator counts replies for
  the shards it planned their senders in, and a replica that owns none of a
  message's keys, and holds nothing of the transaction, casts no vote.

---

## 7. Recovery & failure handling

- **Automatic:** `start_recovery()` runs a sweep that (a) takes over any transaction
  stalled past `recovery_timeout` or whose coordinator the phi-accrual detector
  suspects, (b) pulls committed transactions a peer missed (anti-entropy), and (c)
  compacts redundant state. A healthy cluster recovers nothing.
- **Restart a crashed node:** rebuild the `Node` against the **same durable journal**
  and call `node.recover_state().await?` *before* serving — it restores the replica
  (status/ballots/decision), replays committed transactions into the data store, and
  restores the metadata-log/acceptor state and redundancy watermark. Then
  `start()`/`start_recovery()`. In-flight transactions resume via the sweep, and
  anti-entropy catches the node up. (See `tests/restart.rs`.)
- **A write the caller abandons** (a client that hung up, a `timeout`) is not undone:
  its coordinator may already have reached the replicas. The node stops coordinating
  it, and the transaction is finished by recovery, like one whose coordinator
  vanished. A retry is a new write: if the abandoned one committed, the retry loses
  the version condition.
- **A coordinator overtaken by recovery:** a node can take over a write whose
  coordinator is alive but slow or wrongly suspected, or which reached it late (a
  group it had not opened yet). The coordinator's own rounds then fail, but the write
  may still commit. Before `Node::write` reports such a failure, the coordinator
  recovers its own transaction, or failing that waits for it to apply locally, and
  returns the outcome the cluster decided. An error from `write` therefore means the
  outcome could not be established, not that a decided write was lost. No replica
  compacts a transaction while its coordinator is still settling it.
- **Multi-shard transactions** (`ShardedTopology`, a write spanning shards) carry
  their **route** — every key they touch — on PreAccept/Accept/Commit and on
  recovery (`Message::Routed`), and each replica journals it with the command
  (`Journal::save_route`). A recovery therefore hears from every shard the
  transaction touches, waiting up to `collect_timeout` for a slow one rather than
  deciding on the shards that answered first. If a slow quorum of some shard never
  witnessed the transaction (its coordinator died mid-broadcast), it never committed:
  recovery **invalidates** it — every replica records it aborted, and the write
  reports a conflict. A recovery that cannot hear a slow quorum of every route shard
  (or that quorum is split between "witnessed" and "never witnessed") decides nothing
  and the sweep retries — a multi-shard transaction whose coordinator died waits
  for the shard's replicas to come back rather than guessing. A replica answering
  "never witnessed" journals its promise (`Journal::save_promise`), so after a
  restart it still refuses the original coordinator's late PreAccept. A shard can
  compact a transaction its other shards have not applied yet (each shard's
  watermark is its own), so compaction keeps a small **decision record** of every
  applied multi-shard transaction (`Journal::save_decision`, journaled before the
  truncate that deletes the command): a recovery on another shard learns the
  decision from it. A record is dropped once every replica of every other shard the
  transaction touched reports a watermark above it. A replica of such a shard that
  never reports again (down for good) keeps that shard's records on its peers in
  other shards until it is removed from the topology (`change_topology`); a joiner
  does not import records, since its shard peers still hold them. Single-shard
  traffic is unchanged on the wire. **Rolling upgrade:** a node predating routes
  sheds routed frames, so a multi-shard write needs a slow quorum of upgraded nodes
  in every shard it touches while the upgrade is in progress; single-shard writes
  are unaffected. A custom `Journal` that does not implement `save_route` /
  `save_promise` / `save_decision` loses them on restart (the fjall and SQL journals
  implement them, in their existing keyspace/tables — no migration).
- **Down node:** with `f+1` survivors, writes continue; the down node catches up on
  return. Lose more than `f` and writes block (by design — no split-brain) until enough
  return.
  A down node also holds compaction back: peers compact only below the minimum
  watermark every shard peer has reported, so the backlog grows until it returns
  (watch it against `max_commands`) — unless `peer_absence_limit` is set. Then a
  peer silent that long is **given up on** (`accord_peers_absent_total`, a warning
  names it) and compaction proceeds below the minimum of the peers still heard
  from; a node never gives up so many peers that fewer than a slow quorum stay
  counted, so a stranded minority keeps waiting. Back, the absent node is counted
  again as soon as it reports a watermark above everything compacted past its last
  report (it was merely quiet, or down while nothing was written). If writes it
  never applied were compacted meanwhile, every message it sends is answered with
  `Message::Rejoin` instead: it logs an error, drops every message, refuses writes
  (`RejoinRequired`; `WriteError::Unavailable` through `AccordExecutor`) and stays
  like that until its host rejoins it — the `GroupHost`
  `with_rejoin_handler` hook is told, and should `close(group).await` the group (it
  returns once nothing touches its store or journal), discard its event store —
  **keep its journal** — and `open_joining` it over that journal (next item). The bound a given-up peer must
  report above is persisted (`Journal::save_rejoin_bounds`), so it holds across a
  restart of the node that gave it up. A rejoin streams the snapshot in pages
  (`snapshot_page_bytes`), so a group of any size can be given up on and rejoined.
  The rejoined node is counted again from its first report, at any group size: the
  contact vouches for the state it copied. While the joiner pulls pages, the contact
  holds its own watermark report at that vouch, so the shard's compaction pauses
  for the length of the join (it resumes on the joiner's first report, or a few
  `collect_timeout`s after its last page request if it gives up); size
  `max_commands` for the writes a join of your largest store spans.
- **Node back with a lost store** (new disk, deleted file, a store restored from a
  backup older than its peers' compaction), its journal intact: anti-entropy can
  ship only the commands peers still hold, and the compacted prefix never comes
  back — the `sync contact truncated above this node's proven coverage` warning
  names this case. Empty the store, **keep the journal**, and open the group by
  **joining** instead: `GroupHost::open_joining(…, contacts)` (or
  `recover_state_for_join` + `begin_join` + `join` on a bare `Node`) recovers the
  journal without replaying it into the store, installs a contact's data-store
  snapshot page by page, adopts and persists its floor, replays the traffic
  buffered meanwhile, and applies again what it had applied that the contact had
  not. The journal keeps the node's votes, recovery promises, config-Paxos
  promises and clock bound, so the rejoined node never votes against them. When
  the store and journal share a database (one SQLite file per tenant through
  `evento-sql`, or Fjall through `FjallJournal::from_database`), empty only the
  event store — with `evento-sql`, its `event`, `snapshot` and `subscriber`
  tables; the `accord_*` tables are the journal. Contacts are tried in order; one
  that is itself joining does not answer. Join only over an empty store (`join`
  refuses otherwise): a page that overlaps existing events is skipped whole as an
  idempotent re-apply, which would leave a gap. For the same reason a join that
  fails after a page was installed (a contact that cannot read its backend, a link
  that keeps losing pages) is not retried on the next contact: the error carries
  `PartialSnapshot` (`accord_joins_partial_total`), nothing is left registered, and
  the store (not the journal) must be emptied before opening again.
- **Node back with a lost or rolled-back journal** (a dead disk that held it, a
  deleted file holding both store and journal, a journal restored from a backup):
  the node has forgotten votes, recovery promises and config-Paxos promises it
  made, and ids it minted. Back under its old node id it could vote against them,
  breaking the quorum intersection recovery and reconfiguration rely on. It must
  come back as a **new member under a new node id**: remove the old id from the
  topology (`change_topology`) and add the new one, which joins with an empty store
  and journal (`open_joining`). Under a `GroupHost` the node id is the host's (its
  `MuxTransport` id), shared by every group it hosts, so a new id means a new host
  identity.

---

## 8. Durability & backups

- **Quorum durability:** a write is durable once `f+1` nodes have fsynced it. A single
  node's journal is not a single point of failure.
- **Per-node durability — group commit:** `stage` + one `flush` (fsync) per batch. If a
  flush **fails** (disk error/full disk) the node *withholds* that batch's acks — it
  never claims durability it doesn't have — so a node that can't fsync stops acking
  (treat a node logging `journal flush failed` as failed; investigate its disk).
- **What to back up:** the **event store** (Fjall dir / SQL `event` table — the
  materialized state and read source), per node. A store restored from a backup
  re-syncs its missing tail from peers via anti-entropy — unless its peers have
  compacted past the backup, in which case empty it and rejoin over the node's
  journal (§7, lost store). **Never restore the journal** (Fjall `accord_*`
  keyspaces, or the `accord_*` SQL tables) from a backup: an older journal forgets
  the votes and promises the node made since, so a node whose journal was rolled
  back comes back as a new member (§7, lost journal). The journal is bounded by
  compaction; the event store grows with history.
- **SQL backend:** run the schema via `evento-sql-migrator` with its `accord` feature
  (the `accord_commands/meta/metadata_log/acceptors` tables), alongside your event
  schema — one migrator for both.
- **Clock bound:** the journal holds the node's clock bound (Fjall `meta` key /
  `accord_meta` row `clock_bound`), so a restarted or rejoined node never mints an
  id it minted before, even after its wall clock stepped back. A journal written
  before the bound existed starts the clock past the node's own journaled
  transactions on its first restart. (A journal restored from a backup holds an
  older bound, among everything else it forgets: §7, lost journal.)
- **Upgrading past the apply-time re-stamp** (release note; the journal and wire
  formats are unchanged): a build before it stored each event with the stamp its
  writing host gave it, and later builds stamp it from the transaction's agreed
  execution time. Events a store already holds keep their old stamps — so an
  aggregate written by skewed hosts before the upgrade can still read with versions
  out of stamp order — and nothing re-stamps them, not even a rejoin (a join copies
  the contact's stamps). Do not mix builds in one group: an old and a new replica
  applying the same transaction stamp it differently, and their subscription cursors
  stop lining up for good. Upgrade a group's nodes together (stop all, upgrade, start).
  If a rolling upgrade let both builds apply, pick one upgraded replica and rejoin
  every other from an empty store, keeping its journal, against it (§7, lost
  store), so all share its stamps.

---

## 9. Linearizable reads

Reads are served locally and are **serializable** by default, but a read off a lagging
replica can break real-time order (a non-linearizable read). Set
`NodeConfig.linearizable_reads = true` for a **read-index barrier** (`Node::read_barrier`):
it probes a quorum for the deps a read would witness and waits for them to apply locally
before serving — making reads linearizable (Jepsen-validated strict-serializable under a
quorum-preserving partition). Cost: one consensus round per read. Leave it off where
serializable-but-possibly-stale reads are acceptable; turn it on for read-your-writes /
real-time guarantees across nodes.

---

## 10. Many groups per process (multi-tenant)

One `Node` is one consensus group over one data store and one journal. To give every
tenant its own database (hard isolation) you run **one group per tenant**, and a
process hosts *all* of its tenants' groups at once. Three pieces make that cheap at a
thousand-plus groups per process; none of them changes the protocol or the `Node`:

- **`MuxTransport`** — one connection set per process shared by every group. Each
  frame carries a `GroupId` (an additive wire record kind; a classic `TcpTransport`
  node still interoperates in `GroupId::DEFAULT`). `mux.sink(group)` is a group's
  `MessageSink`; `mux.register(group)` its inbox; `mux.serve(listener)` the single
  accept loop (`serve_tls` / `serve_tls_verified` for mTLS — the pinned identity
  applies to every group on a connection).
- **`SweepScheduler`** — one task drives every node's recovery/anti-entropy/
  compaction sweep instead of a 100 ms task per node. A node with un-applied work, or
  whose applied state moved within `active_hold`, sweeps every tick (the recovery
  bound is unchanged); an idle one sweeps once per `idle_interval`, staggered. Without
  it, 1000 idle groups at N=3 gossip ~28k frames/s per process; with the defaults,
  ~800.
- **`GroupHost`** — `open(group, topology, datastore, journal, config)` does the
  per-group wiring (register → sink → `Node::new` → `recover_state` → `start` →
  schedule) and `close(group).await` undoes it: it returns once the group's sweep and
  inbox loop have stopped (whatever they were doing is dropped, as a crash would drop
  it), so its store and journal may be closed or discarded right after, and the
  returned node mints nothing more (`NodeClosed`; `WriteError::Unavailable` through
  `AccordExecutor`). An open of the same group waits for a close in progress. Never
  close a group from one of its own tasks. `open_joining(…, contacts)` opens a group
  whose store was lost, or that was told to rejoin, by bootstrapping it from a peer's
  snapshot over its kept journal (§7).

```rust
let mux = Arc::new(MuxTransport::new(id, peer_addrs).with_unrouted_handler(reopen));
mux.serve(listener);
let scheduler = SweepScheduler::new(SweepConfig::default());
scheduler.start();
let host = GroupHost::new(Arc::clone(&mux), scheduler);
// per tenant:
let node = host.open(group, topology, store, journal, cfg).await?;
let executor = AccordExecutor::new(node, sql);
```

**Tenants created at runtime.** Every host must learn about a new tenant on its own,
and the wire must never be able to *create* one. The example does this with a
**catalog group** (`GroupId::DEFAULT`, its own `system.db`) holding a `Tenant`
aggregate: creating a tenant is a replicated write (duplicate slugs are rejected
cluster-wide by the version condition), and each host runs a subscription on the
catalog that opens the tenant's database + group when it sees `TenantCreated`. The
**pending-register buffer** (`with_pending_buffer`, default 64 frames × 1024 groups ×
5 s) parks frames for a group that is still being opened and replays them on
`register`, so the first write to a brand-new tenant keeps its fast path. The
**unrouted handler** (`with_unrouted_handler`) is told (rate-limited) about frames for
an unregistered group — use it to re-open an *evicted* tenant from your catalog; ignore
ids the catalog does not know. Sweep gossip (`Message::is_sweep`: watermarks and
anti-entropy, which an idle group's peers keep emitting every `idle_interval`) never
reaches the handler, so a tenant closed for inactivity on one host is not reopened by
its peers' heartbeats; the next real write or read wakes it. A joining replica's requests
(`JoinHeaderRequest`, `CommandPageRequest`, `SnapshotRequest`, `SnapshotPageRequest`)
are not sweep gossip: they wake the group on
a contact that has it closed. The pages it is answered with are, so a page arriving
late, after a failed join unregistered the group, does not reopen it.

**SQLite footprint.** sqlx-sqlite runs one OS thread per *open* connection, so bound
each tenant pool (`max_connections(2–3)`, `min_connections(0)`, an `idle_timeout`) and
cap the number of tenants open per process (LRU; `host.close(group).await`, then
`pool.close()`, on eviction — under the tenant's open lock, so a request for it waits
and opens it afresh; a closed tenant catches up via anti-entropy when re-opened). The tenant
file is also the consensus journal, so keep `synchronous(Full)` under WAL. Build
**one** `Sql` per tenant and clone it into both `ExecutorDataStore` and
`AccordExecutor` — two `.into()`s from one pool do not share the write-wake channel.

**Knobs and bounds.**

| | Default | Meaning |
|-|---------|---------|
| `SweepConfig.tick` | 100 ms | Busy/active cadence; match `recovery_interval`. |
| `SweepConfig.active_hold` | 3 s | Full cadence after applied state moved; must cover `compaction_margin` + a few anti-entropy rounds so a burst compacts before backing off. |
| `SweepConfig.idle_interval` | 5 s | Idle cadence; bounds an idle group's anti-entropy convergence and metadata catch-up. |
| `SweepConfig.max_concurrent` | 64 | Sweeps in flight at once; a due node that finds no slot is retried next tick (`SweepStats.sweeps_deferred`). Size above your simultaneously-active tenants. |
| `MuxTransport::with_queue_capacity` | 1024 | Per-peer outbound queue, shared by all groups — raise for many groups bursting at once (a full queue sheds for all of them). |
| `MuxTransport::with_inbox_capacity` | 1024 | Per-group inbound inbox. |

A join pulls the contact's un-compacted commands as `CommandPage`s and then its log as
`SnapshotPage`s, each of at most `snapshot_page_events` / `snapshot_page_bytes`
(4096 / 4 MiB), far below the transport's 64 MiB frame cap, and an anti-entropy reply
is bounded the same way (a peer back from a long partition repairs its backlog over
several rounds, its proven coverage trailing what is still on its way), so a group of
any size — and any backlog — can be bootstrapped and repaired — and, since a per-peer connection and
queue are shared, one group's bootstrap holds the link for a page at a time rather
than for its whole log. A frame over the cap is shed and counted (`messages_shed`,
with a warning) instead of being handed to the codec, whose refusal would tear the
connection down and take the frames batched with it. (A write that fails on a stale
connection — the peer restarted — is retried once over a fresh one, so one dead
connection does not cost every group a frame.) The one unit no page can split is a
single transaction: `Node::write` refuses one whose events weigh more than
`MAX_COMMAND_BYTES` (half the frame cap) with `CommandTooLarge`
(`WriteError::Unknown` through `AccordExecutor`) — split it into several writes.

**Rolling upgrade.** A joiner asks for the paged header first; a contact predating
command pages sheds the request, and the joiner asks again for the inline header
after `collect_timeout` — so a join against an old contact takes that long longer,
and its whole command backlog must still fit one frame. A joiner predating command
pages asks a current contact for the inline header, as before; one predating
snapshot pages (`SyncRequest { snapshot: true }`) still gets its snapshot inline,
read a page at a time and not answered at all once it outgrows a frame.

**A tenant's first write races its peers' opens.** The creating host opens the group
and writes at once; a peer that has not opened it yet parks the frames. If the peer's
open (file create + migrate) outlasts `fast_timeout` the write takes the slow path; if
it outlasts `collect_timeout` (5 s) on enough peers the write fails. So peers must learn
of a new tenant *fast*: the example's unrouted hook re-reads the catalog stream from the
local system store (applied within milliseconds of the commit) before deciding a group
is unknown, instead of waiting for the catalog subscription (which trails the stability
watermark by about a second). Keep per-tenant opens cheap, or raise `collect_timeout`.

**CPU-bound test rigs.** Every committed write is two fsynced SQLite transactions per
replica (journal flush, event apply), and a debug build multiplies the encode and
query-building cost roughly tenfold — six or more replicas in one debug process can
exceed `collect_timeout` under load. Measure in release before tuning; for a load-test
project, `[profile.dev.package."*"] opt-level = 2` builds dependencies optimized while
your own code stays debuggable.

---

## 11. Pre-production checklist

- [ ] Odd `N` (3 or 5); replicas in independent failure domains.
- [ ] **Durable** journal (`FjallJournal`/`SqlJournal`), not `InMemoryJournal`.
- [ ] Timeouts tuned for your network (raise for WAN/geo).
- [ ] mutual TLS (`serve_tls_verified` + pinned `PeerCerts`) on untrusted networks.
- [ ] `/metrics` scraped + alerts on recoveries / compaction-stall / shed / backlog.
- [ ] `peer_absence_limit` decided: unset keeps a down node's writes shippable on
      return but lets the backlog grow toward `max_commands`; set, a node away
      longer rejoins from a snapshot (host hook `with_rejoin_handler` in place).
- [ ] `snapshot_page_bytes` sized for the link (default 4 MiB): a rejoin moves a
      page per round trip, so a WAN link wants larger pages and a shared, busy
      link smaller ones.
- [ ] Backups of the event store (and journal) per node; restore drill rehearsed.
- [ ] `linearizable_reads` decision made (freshness vs. read latency).
- [ ] Load/soak test on representative hardware before trusting it — and note the
      project's verdict: independent review + multi-day soak are still outstanding.
