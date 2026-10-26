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
  (`start_recovery`) never releases events to its subscriptions. A single-node
  cluster needs no proof.
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
| `anti_entropy_interval` | 250 ms | Pace of the anti-entropy rounds (one peer per round, in turn; while each page of what the node misses brings something in, a round pulls the next from it, up to 16 pages and for one interval). Each round also proves the window a node holds, which both its compaction report and its subscriptions wait for: subscriptions trail the present by about `compaction_margin` plus `(slow quorum − 1)` rounds. Keep it well below `compaction_margin`. |
| `compaction_margin` | 1 s | Lag on the redundancy watermark, and the original coordinator's **deadline**: a node never votes for a coordinator's PreAccept, or its Accept at the transaction's own ballot, for a transaction it never witnessed whose `t0` is older than its clock less this margin (`votes_late`). A recovery past the deadline asks such a node twice: first, it answers that it is late; then the recovery asks it to refuse the transaction for good, when that proves it never committed — one only a minority holds, which the recovery then invalidates — or to vote, when the replies show it may have committed (a replica that accepted it lost its journal, §7). It votes unless the transaction is below its **vote fence**, what it vouched for (§5, `votes_fenced`), so one a slow quorum of its shard held unapplied all along is still decided, however late (#337). A multi-shard transaction a shard never saw is invalidated only past the deadline too, where that shard's replicas can be refused for good (#377): within it they only promise the ballot and wait to be handed its part. Keep it ≥ `recovery_timeout` + 2 × the clock-skew bound + worst-case message delay, so a coordinator's messages and a recovery of a transaction still in flight land inside it. |
| `max_journal_batch` | 128 | Messages drained per group-commit fsync (latency vs. fsync amortization). |
| `phi_threshold` | 8.0 | Phi-accrual suspicion level; higher tolerates more latency variance before suspecting a coordinator. |
| `acceptable_pause` | 500 ms | How long a live peer may go quiet beyond its usual heartbeat interval (scheduler stall, slow fsync, loaded host) before suspicion starts to rise. |
| `linearizable_reads` | false | Opt-in linearizable reads (§9) — one consensus round per read. |
| `max_commands` | 100 000 | Backpressure: refuse *new* local writes (`BacklogAtCap`; `WriteError::Unavailable` through `AccordExecutor`) once the un-compacted backlog hits this (e.g. compaction stalled under a partition, or a peer down with `peer_absence_limit` unset). |
| `peer_absence_limit` | none | Stop waiting for a same-shard peer silent this long: compaction then proceeds without it (§7, down node). Set it well above any restart or maintenance window; a peer given up on while writes went through must rejoin from a snapshot on return. Never leaves fewer than a slow quorum counted. |
| `snapshot_page_events` | 4096 | Most events this node puts in one snapshot page when a joiner bootstraps from it (§7, lost store). |
| `snapshot_page_bytes` | 4 MiB | Byte budget of one snapshot page, command page or anti-entropy reply (a single larger event or command still crosses alone). Keep it far below the transport's 64 MiB frame cap and small enough that a bootstrap does not hold the link for long. |
| `config_defer_step` | 100 ms | Control-plane anti-dueling delay for concurrent reconfigurations. |
| `fence_invalidation` | true | Lets a recovery invalidate a transaction one replica compacted past while the others fenced it or hold it uncommitted. Sound only when every replica fences its votes: during a rolling upgrade from a build without the vote fence, set it `false` until the last node is upgraded. |
| `amnesia_join_timeout` | 30 s | How long a node joining over a lost journal waits for its contact's watermark report to pass its amnesia fence before the join gives up with `FenceNotCovered` (§7, lost journal). Allow `compaction_margin` + three clock skews + a few anti-entropy rounds. |

Clock-skew tolerance is on the clock: `HybridLogicalClock::with_max_skew(..)` (default
`MAX_SKEW_MICROS`); keep `recovery_timeout`/`compaction_margin` above it. The node
persists a clock bound in its journal (raised at most once per `max_skew` while it
mints or reports) and a restart starts its clock there, so a node never reuses a transaction id
or ballot across a restart, even if its wall clock stepped back. It also persists its
vote fence (§5, `votes_fenced`) before anything it vouches for leaves it, so the fence
never goes back either; a custom journal that keeps no fence (`Journal::save_vote_fence`
on its default) restarts it at the clock bound less the margin. A restart within
`max_skew` of the last write waits out at most `max_skew`; after a backward step the
node logs that its wall clock is behind its clock bound and its stamps lead the wall
clock until it catches up. Writes fail rather than use an id the bound does not cover
(the journal refused it). A node votes for no transaction whose `t0` lies more than two
skews past its clock (`accord_votes_future_dated_total`): an honest coordinator's ids
never do, and the bound is what lets a node that lost its journal fence everything it
voted on (§7). A node whose clock runs further ahead than the bound gets no votes for its
writes until the others' clocks catch up.

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
| `journal_flushes` | group-commit activity; **0 while writing** = the journal can't fsync (see §8). |
| `messages_handled` | liveness/traffic. |
| `messages_shed` | inbound backpressure drops; sustained > 0 = a node is overloaded or a peer is flooding. |
| `peers_absent` | shard peers this node stopped waiting for (`peer_absence_limit`); each is a node that was silent for that long. |
| `rejoins_required` | this node must rejoin: a peer told it to, having given it up while writes it never applied were compacted; or its own recovery found it holding a transaction its peers compacted past, undecided, which no recovery can decide any more. The node is stale until its host rejoins it (§7) — through `Node::rejoin_contact()`, the peer that compacted past what it holds. |
| `votes_fenced` | votes refused for a transaction this node never witnessed, below its vote fence — what it vouched for (a watermark report, sync coverage, a join's vouch, its subscriptions' stable point) or could vouch for now: typically a recovery by a replica that alone holds the transaction (§3, `compaction_margin`). It never vouches past a transaction a slow quorum of its shard holds unapplied, so such a one still gets its vote (#337). Spikes after a partition heals are expected. |
| `votes_late` | votes refused to an original coordinator older than the compaction margin, for a transaction this node never witnessed (§3, `compaction_margin`). Spikes after a partition heals are expected; a steady rate means coordinators slower than the margin. |
| `dependencies_learned` | decisions this node learned from its peers for a transaction it never witnessed, but a command committed here waited on (missed while it was cut off or restarting). Counts after a partition heals are expected; it keeps a stalled execution from waiting on anti-entropy for a transaction no replica can apply yet. |
| `votes_future_dated` | votes refused for a transaction dated more than two clock skews past this node's clock: its coordinator's clock runs beyond the skew bound (§3). Any count = a clock out of bounds; fix its time source. |
| `amnesia_fences` | times this node opened joining over an empty journal and fenced itself (§7, lost journal). |
| `amnesia_joins_refused` | joins over a lost journal that found no contact vouching past the fence (`FenceNotCovered`, nothing installed). A few while the contact's report catches up; **steadily rising** = a transaction below the fence no replica can decide any more (§7). |
| `journals_rolled_back` | times a peer found this node's journal rolled back to an older copy at its open (§7). Any count = the node stopped: delete its journal and store, and open it joining. |
| `config_votes_withheld` | config-Paxos promises and accepts this node withheld: it lost a journal under its node id, and abstains from config Paxos until it restored its votes (§7). **Still rising** long after its join = it cannot hear a majority of the voting acceptors (a partition, or too many acceptors lost their journals at once). |
| `config_votes_restored` | times this node, back over a lost journal, read a majority of the other acceptors' state and voted in config Paxos again (§7). One per loss. |
| `snapshot_pages` | snapshot pages this node served to joining peers; a joiner pulls one per `snapshot_page_events` / `snapshot_page_bytes` of the contact's store. |
| `command_pages` | command pages this node served to joining peers: a joiner pulls the contact's un-compacted applied commands one page at a time, before its snapshot pages. |
| `joins_partial` | joins of this node that failed after a page was installed: the store holds a prefix no contact can complete, and must be emptied before the next join (§7). Steady > 0 = a contact that cannot read its backend, or a link losing pages faster than the per-page retries cover. |
| `execution_failures` | data-store applies and commit-condition reads that failed. The transaction is neither marked applied nor acknowledged, and is retried on the next drive (a message, or the sweep). Occasional = transient lock contention; **sustained** = this replica's store cannot take writes (full disk, broken connection), so it falls behind until it can. |

**Health/readiness.**
- *Liveness:* a simple `200` while the process is up (`bank-axum-accord`'s `/health`).
- *Readiness* (is this node caught up enough to serve fresh reads?): derive from
  metrics — `recoveries` not climbing, `compactions` advancing, `messages_shed` ~0, and
  the local backend's applied version tracking the cluster. For strict freshness, use
  `linearizable_reads` (§9) rather than a readiness gate.

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

- **Add a node:** start the new process (with `start_recovery`) and `begin_join()` it
  (it buffers consensus messages), drive `change_topology(epoch, new_layout)` from any
  current member, then, once that returns, call `join(contact)` on the new node with a
  contact of its shard. The new node may not have installed the new epoch yet: `join`
  fetches it from the contact and waits, failing if the node is still no replica after
  a few `collect_timeout`s. A node the change gave slots to copies them while the change
  drains, and its `join` then returns `Ok(0)`: call it either way. (See
  `tests/membership.rs`, `tests/transitions.rs`, `tests/resharding.rs`.)
- **Remove a node:** `change_topology(epoch, smaller_layout)`; the smaller cluster keeps
  serving, once the change drained the writes in flight on the node's shard (below).
  Reconfiguration survives the coordinator crashing mid-change (another node
  completes it via `recover_topology`, and the recovery sweep) and works after founders
  have left (the acceptor set tracks current membership).
- **Replace a node** (a dead one included): one change, `change_topology(epoch,
  layout)`.
- **Re-shard:** add or remove shards, or move a node to another shard, with one change.
  Keys map to 4096 slots, and the layout's slot table maps slots to shards: a change
  rebalances it, moving only the slots a new shard needs or a removed one leaves (a
  split from two shards to three moves a third of the slots). Every node that gains
  slots copies their events while the change drains, from a replica that held them:
  plan for the copy's time on top of the drain. A node drops the slots a change takes
  from it once the change installs (`Executor::delete_stream`, stream by stream: watch
  `accord_streams_dropped_total`), and before copying a slot it gains, drops whatever
  it still holds for it — so a change may give a node back slots it replicated before
  (a merge after a split, a replica moved back). A node restarted over its journal
  drops again what it had not finished dropping, and replays no write of a slot it
  lost into its store (`accord_applies_not_owned_total` counts the applies that left
  such events out). A node that gains slots copies their events with their original
  stamps: its own subscriptions do not see them if they already read past those
  stamps (below).
- **What a change does with writes in flight.** Every transaction is voted on and
  recovered under the layout it was coordinated under. A change that only adds a node
  to a shard of three or four installs at once, and writes go on. Any other change —
  removing or replacing a node, adding two at once, moving slots or nodes between shards
  — takes two epochs (two removals at once in a row could leave a write in flight from
  before them without a read quorum of the replicas that stay): a *transition* keeps the old layout live
  while the writes in flight on the keys it moves drain, and the nodes that gain slots
  copy them; then the next epoch installs the new layout. Meanwhile, writes to the
  moving keys fail with `TopologyChanging` (`WriteError::Unavailable` through
  `AccordExecutor`): plan for about `compaction_margin` + two clock skews + a few
  `anti_entropy_interval`s — about 2 s with the defaults — plus the tail of the
  copies. Before it proposes the change, `change_topology` has every node that
  gains slots copy them while writes still flow (a *pre-seed*, up to
  `preseed_timeout`, 10 s by default: raise it for a large range), so the copy under
  the fence brings only what landed since, from the sources' stable point on. A
  gainer that does not answer in time does not hold the change back: the fenced copy
  brings its slots whole. Watch `accord_preseed_events_total` against
  `accord_range_copy_events_total`.
- **Subscriptions on a node that gains slots.** The copied events keep their
  original stamps, and a subscription reads forward from a stamp cursor: one on the
  gainer that already read past those stamps never sees the gained history (writes
  after the activation are stamped above it, so the gap is the history alone). The
  node warns at each copy (`copied history lies below this node's stable point`) and
  counts the events in `accord_copied_below_stable_total`. A projection on that node
  that must hold the gained range is rebuilt, or its cursor rewound below the lowest
  copied stamp (the log line gives the range) and run again. Today a subscription
  that must see every key runs on a single-shard cluster, where no node gains slots;
  this is settled with cross-shard subscriptions (#374). The
  transition is driven by the recovery sweep's watermark gossip, anti-entropy and
  copies: run `start_recovery` on every node, new ones included. It waits for a slow
  quorum of every shard, for every node that gains slots, and for every node that
  keeps a slot the change moves (a replacement's other replicas). A dead node the new
  layout drops does not hold it back, with the default `peer_absence_limit` too; a
  dead one it gives or keeps slots does: replace it too, with another change.
  `change_topology` returns once the new layout is
  installed, or fails after a while, leaving the sweep to install it. While a
  transition drains, a change to another layout replaces it, and a change back to the
  live layout cancels it.
- A change must be for the next epoch: `change_topology` refuses one that skips an
  epoch.
- A `join` goes through a replica of the joiner's own shard only.

---

## 7. Recovery & failure handling

- **Automatic:** `start_recovery()` runs a sweep that (a) takes over any transaction
  stalled past `recovery_timeout` or whose coordinator the phi-accrual detector
  suspects, and first any stalled one its read waits on, (b) pulls committed
  transactions a peer missed (anti-entropy), and (c) compacts redundant state. A
  healthy cluster recovers nothing.
- **Restart a crashed node:** rebuild the `Node` against the **same durable journal**
  and call `node.recover_state().await?` *before* serving — it restores the replica
  (status/ballots/decision), replays committed transactions into the data store, and
  restores the metadata-log/acceptor state and redundancy watermark. Then
  `start()`/`start_recovery()`. In-flight transactions resume via the sweep, and
  anti-entropy catches the node up. A replayed transaction the store neither holds
  nor can take — it lost the write (§8), or misses an earlier one the journal never
  recorded — is handed back decided but un-applied and applied again once what
  precedes it is in place. The sweep retries that apply, and never takes the
  transaction over: its peers may have compacted past it, and the node holds
  their decision (#351). (See `tests/restart.rs`.) Over a journal that holds
  something, in a group of more than one node, the node first asks every other node
  to confirm the journal is no older copy of itself (#339, below): until a slow
  quorum of its shard, itself included, confirmed it, and every other node answered
  or `collect_timeout` passed, it buffers consensus messages, refuses writes
  (`OpenUnconfirmed`), and mints, votes and vouches for nothing (`Node::confirmed()`).
  A node whose peers are down stays unconfirmed until enough of them are back; a
  full-cluster restart confirms every node, each answering the others from the marks
  its own journal kept.
- **A write the caller abandons** (a client that hung up, a `timeout`) is not undone:
  its coordinator may already have reached the replicas. The node stops coordinating
  it, and the transaction is finished by recovery, like one whose coordinator
  vanished. A retry is a new write: if the abandoned one committed, the retry loses
  the version condition — unless it carries the very same events (same ids and
  payloads), which then read as already applied: the retry commits, changing
  nothing.
- **A coordinator overtaken by recovery:** a node can take over a write whose
  coordinator is alive but slow or wrongly suspected, or which reached it late. The
  coordinator's own rounds then fail, but the write
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
  deciding on the shards that answered first. A replica that never witnessed the
  transaction promises the recovery's ballot and, handed its shard's part from a
  holder's report, votes for it — past the coordinator's deadline
  (`compaction_margin`) only once the recovery asks it to, as for a single-shard
  transaction (#337, #377). If a slow quorum of some shard never witnessed the
  transaction (its coordinator died mid-broadcast), it never committed: past the
  deadline, the recovery has that shard's replicas refuse it for good and
  **invalidates** it — every replica records it aborted, and the write reports a
  conflict. Fewer refusals do the same beside a replica of that shard that lost
  its journal and a holder that never accepted the transaction: no quorum could
  have committed it, and none can ever vote for it (#386). A recovery that cannot hear a slow quorum of every route shard decides
  nothing and the sweep retries — a multi-shard transaction whose coordinator died
  waits for the shard's replicas to come back rather than guessing. A replica's
  promise and refusal are journaled (`Journal::save_promise`), so after a restart it
  still refuses the original coordinator's late PreAccept. A shard can
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
  like that until its host rejoins it. Watch `Node::rejoin_signal()` (or poll
  `rejoin_required()`); when it turns `true`, `retire()` the node, abort **and await**
  its inbox loop and recovery task (the handles `start` and `start_recovery`
  returned), so nothing touches its store or journal any more, discard its event
  store — **keep its journal** — and open it by joining over that journal
  ([below](#opening-a-node-by-joining)), listing `Node::rejoin_contact()` first among
  the contacts: the peer that told it
  to rejoin (or that its recovery found compacted past a transaction it holds)
  compacted past what it holds, so joining through it drops everything
  below that floor, where a contact whose floor lags could leave the node holding a
  transaction the others compacted past (and have it told to rejoin again). The bound a given-up peer must
  report above is persisted (`Journal::save_rejoin_bounds`), so it holds across a
  restart of the node that gave it up — and across its own rejoin: a node joining
  over its kept journal through a contact it still holds under a bound first asks
  that contact to prove it holds what was compacted (a `Message::Rejoin`; a contact
  that caught up answers with its report at once, a round trip). It copies nothing
  from a contact that cannot: that one is told to rejoin, and the join fails within
  `collect_timeout`, so the host goes on to its next contact. A rejoin streams the snapshot in pages
  (`snapshot_page_bytes`), so a node of any store size can be given up on and
  rejoined. The rejoined node is counted again from its first report, at any store
  size: the
  contact vouches for the state it copied. While the joiner pulls pages, the contact
  holds its own watermark report at that vouch, so the shard's compaction pauses
  for the length of the join (it resumes on the joiner's first report, or a few
  `collect_timeout`s after its last page request if it gives up); size
  `max_commands` for the writes a join of your largest store spans.
- **Node back with a lost store** (new disk, deleted file, a store restored from a
  backup older than its peers' compaction), its journal intact: anti-entropy can
  ship only the commands peers still hold, and the compacted prefix never comes
  back — the `sync contact truncated above this node's proven coverage` warning
  names this case. Empty the store, **keep the journal**, and open the node by
  **joining** instead ([below](#opening-a-node-by-joining)): it recovers the
  journal without replaying it into the store, installs a contact's data-store
  snapshot page by page, adopts and persists its floor, replays the traffic
  buffered meanwhile, and applies again what it had applied that the contact had
  not. The journal keeps the node's votes, recovery promises, config-Paxos
  promises and clock bound, so the rejoined node never votes against them. When
  the store and journal share a database (one SQLite file through `evento-sql`, or
  Fjall through `FjallJournal::from_database`), empty only the event store — with
  `evento-sql`, its `event`, `snapshot` and `subscriber` tables; the `accord_*`
  tables are the journal. Kept in files of their own (the events in one SQLite
  file, the journal through `SqlJournal` in another), the store goes with its file
  and the journal stays. Contacts are tried in order; one
  that is itself joining does not answer. Join only over an empty store (`join`
  refuses otherwise): a page that overlaps existing events is skipped whole as an
  idempotent re-apply, which would leave a gap. For the same reason a join that
  fails after a page was installed (a contact that cannot read its backend, a link
  that keeps losing pages) is not retried on the next contact: the error carries
  `PartialSnapshot` (`accord_joins_partial_total`), and the store (not the journal)
  must be emptied before joining again.
- **Node back with a lost journal** (a dead disk that held it, a deleted or corrupt
  journal file, with or without its store): the node forgot the votes, recovery
  promises, config-Paxos promises and watermark reports it made, and the ids it
  minted. Voting again on what it forgot, or on new writes without what it forgot
  among their dependencies, it could split the stores. It comes back **under its own
  node id, by joining** (#312): empty its event store and open it by joining
  ([below](#opening-a-node-by-joining)). Over an empty journal the node sets an
  **amnesia fence**: its wall
  clock plus three clock skews, above everything it voted on, promised or reported
  before, journaled before anything else (`Journal::save_amnesia_fence`; the fjall and
  SQL journals keep it — a custom journal on the default no-op is refused, and such a
  node must come back under a new node id), with its clock floored past every id it
  can have minted (`accord_amnesia_fences_total`).
  - **The join waits for a contact past the fence.** It asks for a contact's header
    only once the contact's own watermark report passes the fence, and installs
    nothing unless the vouch that comes with the header does too, under the same
    epoch and from the same shard: every write that committed before the loss is
    then in the state it copies, and none that did not can commit any more. That
    takes about `compaction_margin` plus three skews and a few anti-entropy rounds; a
    contact that cannot vouch within `amnesia_join_timeout` fails the join with
    `FenceNotCovered`, nothing installed — open joining again, through it or another
    (`accord_amnesia_joins_refused_total`).
  - **Until then it takes no part**: it votes on nothing, serves no read and
    coordinates no write (`Joining`; `WriteError::Unavailable` through
    `AccordExecutor`), as any node still joining (#388). A
    recovery of a transaction below its fence gets `Message::Amnesiac` — it holds
    nothing of it, may have voted for it before, and never will — which lets the
    recovery rule out a transaction only the lost replica could have helped decide
    (a write that reached only a minority while the replica was away). One of a
    transaction above the fence that it does not hold it answers at once as a
    **late** replica would, as does any node still joining (#390): a journaled
    promise and `Late` when asked, a refusal for good (`NotWitnessed`) when the
    recovery asks for one, and a vote only once joined (a `Vote` round waits for
    the snapshot). Buffered until the join, the question would wait on the
    watermark the transaction pins, which the join itself waits on.
  - **Once joined it is an ordinary replica**, and a restart opens it plainly. It never
    votes for a transaction below its fence that it does not hold, and it **abstains
    from config Paxos** under this node id (`accord_config_votes_withheld_total`)
    **until it restored its votes** (`accord_config_votes_restored_total`, #338): a
    `collect_timeout` after its fence, it reads every other acceptor's state and
    adopts it, once a majority of all the acceptors among the *voting* ones
    answered — about a `collect_timeout` plus a sweep after the join. Config
    changes meanwhile need a majority of the other acceptors. An acceptor that
    itself lost a journal and has not restored its votes counts for nothing in that
    read, so **acceptors that lose their journals at once** — a majority of them,
    or two of three — never restore: bring those back under new node ids (the
    withheld counter keeps rising on every config change). Losses one after
    another, each restored before the next, cost nothing.
  - **A plain open refuses a lost journal**: `recover_state` over an empty
    journal and a store that holds events fails with `LostJournal`
    (or `LostJournal { join_pending: true }` after a join over a lost
    journal that never completed) — empty the store and open joining. A cluster of a
    single node is exempt: no peer holds its votes, and it reopens over its store.
  - **What the node cannot tell**: an empty journal over an empty store, opened
    plainly, is a fresh replica (its first open leaves its clock bound in the journal,
    so the journal is never empty again while the store holds anything). A host that
    lost a node's journal *and* store must therefore know, from elsewhere, that it
    was a member, and open it joining — or bring the node back under a new node id,
    as before: remove the old id from the topology (`change_topology`) and add the new
    one, which joins with an empty store and journal.
  - **Decided below the fence**: a transaction still undecided at the loss — accepted
    by the lost replica and by fewer than a slow quorum of the others, its coordinator
    gone — needs the vote of a replica that never saw it, however long ago its
    compaction margin passed. That replica never vouched past it: the replicas holding
    it unapplied, the lost one's last gossip included, keep their shard's applied floor
    below it, and no replica hands out sync coverage past that floor (#337). A recovery
    gets its vote and decides the transaction, and the join goes through. One only the
    lost replica held, which a write committed with as a dependency, is invalidated by
    the replicas that wait on it: past its deadline nothing can vote for it, so it never
    committed.
  - **Stuck below the fence**: one case can still never be decided — a transaction
    whose lost replica voted for it only in a recovery and lost its journal before its
    next report showed it holding the transaction. (A **multi-shard** transaction of
    the decided shape is decided too: the routed recovery hands the late replica its
    shard's part, #377.) The join then never finds a contact past its fence
    (`accord_amnesia_joins_refused_total` keeps rising), and the transaction holds
    compaction back; bringing the node back under a new id leaves it just as
    undecided.
  - **Assumes** the host's wall clock did not step back across the loss by more than
    the skew bound (NTP), and that every clock stays within `max_skew` of the others
    (a node votes for no transaction dated past its clock's bound, §3).
- **Node back with a rolled-back journal** (restored from a backup or a VM snapshot, or
  a disk that lost writes it had acknowledged): its peers catch it at its open (#339).
  Every watermark report carries the node's **durable mark** — its journaled clock
  bound and vote fence, which only rise — and every node keeps the highest it saw
  from each peer, journaled with its reports. Reopened over a journal below the mark
  a peer saw, the node is told so (`RolledBack`) and stops taking part:
  `Node::rolled_back()` and `rolled_back_signal()` turn on, writes fail with
  `JournalRolledBack`, and `accord_journals_rolled_back_total` counts it. **Retire it,
  delete its journal and its store**, and bring it back as above (lost journal).
  - **What escapes it:** a copy that lost only what the node did after the last
    report its peers heard — about a sweep (`recovery_interval`); a copy taken before
    the node first journaled its vote fence, which a restore puts back at the clock
    bound less the margin, that lost only what followed the bound's last rise (two
    clock skews at most); and any copy when the journal keeps no clock bound
    (`Journal::save_clock_bound` on its default; the node logs it at open). So
    `fsync` must stay honest (§8), and a journal is never restored on purpose.

### Opening a node by joining

A node that rejoins, or comes back with a lost store or journal, is built over its
kept (or empty) journal and an **empty** event store, and opened by joining instead
of `recover_state`:

```rust,ignore
let node = Node::new(id, topology, clock, sink, store, journal).with_config(config);
node.recover_state_for_join().await?; // the journal, without replaying it into the store
node.begin_join(); // before the inbox loop: nothing is processed ahead of the snapshot
let inbox_task = node.start(inbox);
let mut outcome = Err(anyhow::anyhow!("no contact to join through"));
for &contact in &contacts {
    outcome = node.join(contact).await;
    match &outcome {
        Ok(_) => break,
        // Part of the snapshot is installed: another contact's pages would overlap
        // it and be skipped, leaving a gap. Stop here.
        Err(err) if err.is::<PartialSnapshot>() => break,
        // FenceNotCovered, an unreachable contact, a contact itself joining: next.
        Err(_) => {}
    }
}
if let Err(err) = outcome {
    inbox_task.abort();
    let _ = inbox_task.await;
    // Retry later; after a PartialSnapshot, empty the store first (keep the journal).
    return Err(err);
}
let recovery_task = node.start_recovery();
```

To rejoin a node that is running (told to rejoin, or a store found lost), first
`retire()` it, abort and await `inbox_task` and `recovery_task` — once both
returned, nothing touches its store or journal — then empty the store and open it as
above. A retired node mints nothing more: a clone the application still holds fails
to write with `NodeClosed` (`WriteError::Unavailable` through `AccordExecutor`), so it
can never mint an id the reopened node mints too.

**Sizes.** A join pulls the contact's un-compacted commands as `CommandPage`s and
then its log as `SnapshotPage`s, each of at most `snapshot_page_events` /
`snapshot_page_bytes` (4096 / 4 MiB), far below the transport's 64 MiB frame cap, and
an anti-entropy reply is bounded the same way (a peer back from a long partition
repairs its backlog over several rounds, its proven coverage trailing what is still on
its way), so a store of any size — and any backlog — can be bootstrapped and repaired,
a page at a time. A frame over the cap is shed and counted (`messages_shed`, with a
warning) instead of being handed to the codec, whose refusal would tear the connection
down and take the frames batched with it. (A write that fails on a stale connection —
the peer restarted — is retried once over a fresh one.) The one unit no page can split
is a single transaction: `Node::write` refuses one whose events weigh more than
`MAX_COMMAND_BYTES` (half the frame cap) with `CommandTooLarge` (`WriteError::Unknown`
through `AccordExecutor`) — split it into several writes.

---

## 8. Durability & backups

- **Quorum durability:** a write is durable once `f+1` nodes have fsynced it. A single
  node's journal is not a single point of failure.
- **Per-node durability — group commit:** `stage` + one `flush` (fsync) per batch. If a
  flush **fails** (disk error/full disk) the node *withholds* that batch's acks — it
  never claims durability it doesn't have — so a node that can't fsync stops acking
  (treat a node logging `journal flush failed` as failed; investigate its disk).
- **The event store must persist an apply before it returns:** evento-fjall's
  default (`PersistMode::SyncAll` per write), SQL with synchronous commits. The
  journal records a transaction applied once the store took it, and that record is
  the node's promise that the effect is stored. A store that acknowledges first
  (`Fjall::persist_mode(PersistMode::Buffer)`) can come back from a crash behind its
  journal: the node applies again what its journal still holds, and what
  anti-entropy can ship, but a gap older than both leaves it stalled on that
  aggregate (a store refuses an event past a missing version) — rejoin it over its
  journal (§7, lost store).
- **What to back up:** the **event store** (Fjall dir / SQL `event` table — the
  materialized state and read source), per node. A store restored from a backup
  re-syncs its missing tail from peers via anti-entropy — unless its peers have
  compacted past the backup, in which case empty it and rejoin over the node's
  journal (§7, lost store). **Never restore the journal** (Fjall `accord_*`
  keyspaces, or the `accord_*` SQL tables) from a backup: an older journal forgets
  the votes and promises the node made since. Its peers catch it at the node's next
  open and stop it (§7, rolled-back journal) — unless it lost only the node's last
  moments, which go unnoticed — and it must come back over an empty journal anyway.
  Delete it instead, and bring the node back over an empty journal by joining (§7,
  lost journal). The journal is bounded by
  compaction; the event store grows with history.
- **SQL backend:** run the schema via `evento-sql-migrator` with its `accord` feature
  (the `accord_commands/meta/metadata_log/acceptors` tables), alongside your event
  schema — one migrator for both.
- **Clock bound:** the journal holds the node's clock bound (Fjall `meta` key /
  `accord_meta` row `clock_bound`), so a restarted or rejoined node never mints an
  id it minted before, even after its wall clock stepped back. A journal written
  before the bound existed starts the clock past the node's own journaled
  transactions on its first restart. (A journal restored from a backup holds an
  older bound, among everything else it forgets: its peers catch it, and it must be
  deleted, §7.)
- **Upgrading past the apply-time re-stamp** (release note; the journal and wire
  formats are unchanged): a build before it stored each event with the stamp its
  writing host gave it, and later builds stamp it from the transaction's agreed
  execution time. Events a store already holds keep their old stamps — so an
  aggregate written by skewed hosts before the upgrade can still read with versions
  out of stamp order — and nothing re-stamps them, not even a rejoin (a join copies
  the contact's stamps). Do not mix builds in one cluster: an old and a new replica
  applying the same transaction stamp it differently, and their subscription cursors
  stop lining up for good. Upgrade a cluster's nodes together (stop all, upgrade, start).
  If a rolling upgrade let both builds apply, pick one upgraded replica and rejoin
  every other from an empty store, keeping its journal, against it (§7, lost
  store), so all share its stamps.
- **Tenants** (record format 2): events carry their tenant, a stream is
  `(tenant, type, id)`, and a key lives in its tenant — the default tenant's keys
  are the bare routing key or aggregate id. A node of format 2 refuses journals and
  frames of format 1: evento-accord is unreleased, so a cluster of an older build is
  wiped and re-bootstrapped. A replica applies — and a joiner copies — every
  tenant's events.
- **Paged joins only** (record format 3): the inline join paths are gone. A join
  is always the paged header followed by command pages and snapshot pages, so no
  join frame grows with the store or the backlog. A node of format 3 refuses
  journals and frames of format 2, so upgrade a cluster's nodes together, or wipe
  and re-bootstrap it as for format 2.
- **A journaled vote fence** (record format 9): each watermark report gossips its
  sender's applied-through, the journal keeps the vote fence (`accord_meta` row /
  Fjall `meta` key `vote_fence`, no migration), and a recovery settles a replica past
  the original coordinator's deadline in two rounds (`BeginRecovery` names its round,
  a new `Late` answer; a refusal for good is a journaled promise at the highest
  ballot). A node of format 9 refuses journals and frames of format 8: wipe and
  re-bootstrap, as for format 2.
- **A confirmed open** (record format 10): each watermark report carries its sender's
  durable mark, every node journals the highest mark it saw from each peer
  (`accord_meta` row / Fjall `meta` key `peer_marks`, no migration), and a node opened
  over a journal that holds something asks the others to confirm it (`Opened`,
  answered `OpenedOk` or `RolledBack`). A node of format 10 refuses journals and
  frames of format 9: wipe and re-bootstrap, as for format 2.

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

## 10. Pre-production checklist

- [ ] Odd `N` (3 or 5); replicas in independent failure domains.
- [ ] **Durable** journal (`FjallJournal`/`SqlJournal`), not `InMemoryJournal`.
- [ ] Timeouts tuned for your network (raise for WAN/geo).
- [ ] mutual TLS (`serve_tls_verified` + pinned `PeerCerts`) on untrusted networks.
- [ ] `/metrics` scraped + alerts on recoveries / compaction-stall / shed / backlog.
- [ ] `peer_absence_limit` decided: unset keeps a down node's writes shippable on
      return but lets the backlog grow toward `max_commands`; set, a node away
      longer rejoins from a snapshot (the host watches `rejoin_signal` and rejoins
      it, §7).
- [ ] `snapshot_page_bytes` sized for the link (default 4 MiB): a rejoin moves a
      page per round trip, so a WAN link wants larger pages and a shared, busy
      link smaller ones.
- [ ] Backups of the event store (and journal) per node; restore drill rehearsed.
- [ ] `linearizable_reads` decision made (freshness vs. read latency).
- [ ] Load/soak test on representative hardware before trusting it — and note the
      project's verdict: independent review + multi-day soak are still outstanding.

**CPU-bound test rigs.** Every committed write is two fsynced SQLite transactions per
replica (journal flush, event apply) on the SQL backends, and a debug build multiplies
the encode and query-building cost roughly tenfold — six or more replicas in one debug
process can exceed `collect_timeout` under load. Measure in release before tuning; for
a load-test project, `[profile.dev.package."*"] opt-level = 2` builds dependencies
optimized while your own code stays debuggable.
