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
| `compaction_margin` | 1 s | Lag on the redundancy watermark; keep above clock-skew bound + propagation latency. |
| `max_journal_batch` | 128 | Messages drained per group-commit fsync (latency vs. fsync amortization). |
| `phi_threshold` | 8.0 | Phi-accrual suspicion level; higher tolerates more latency variance before suspecting a coordinator. |
| `linearizable_reads` | false | Opt-in linearizable reads (§10) — one consensus round per read. |
| `max_commands` | 100 000 | Backpressure: refuse *new* local writes once the un-compacted backlog hits this (e.g. compaction stalled under a partition). |
| `config_defer_step` | 100 ms | Control-plane anti-dueling delay for concurrent reconfigurations. |

Clock-skew tolerance is on the clock: `HybridLogicalClock::with_max_skew(..)` (default
`MAX_SKEW_MICROS`); keep `recovery_timeout`/`compaction_margin` above it.

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

**Health/readiness.**
- *Liveness:* a simple `200` while the process is up (`bank-axum-accord`'s `/health`).
- *Readiness* (is this node caught up enough to serve fresh reads?): derive from
  metrics — `recoveries` not climbing, `compactions` advancing, `messages_shed` ~0, and
  the local backend's applied version tracking the cluster. For strict freshness, use
  `linearizable_reads` (§10) rather than a readiness gate.

Suggested alerts: `recoveries` rate > 0 for several minutes; `compactions` flat while
`writes_committed` climbs; `messages_shed` rate > 0; backlog approaching `max_commands`.

---

## 6. Scaling (membership & re-sharding)

Requires `DynamicTopology`. Membership changes are decided by a durable, replicated
metadata log (config-Paxos); concurrent reconfigurations converge without a leader
(distinguished proposer + yield).

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
- **Down node:** with `f+1` survivors, writes continue; the down node catches up on
  return. Lose more than `f` and writes block (by design — no split-brain) until enough
  return.

---

## 8. Durability & backups

- **Quorum durability:** a write is durable once `f+1` nodes have fsynced it. A single
  node's journal is not a single point of failure.
- **Per-node durability — group commit:** `stage` + one `flush` (fsync) per batch. If a
  flush **fails** (disk error/full disk) the node *withholds* that batch's acks — it
  never claims durability it doesn't have — so a node that can't fsync stops acking
  (treat a node logging `journal flush failed` as failed; investigate its disk).
- **What to back up:** the **event store** (Fjall dir / SQL `event` table — the
  materialized state and read source) and, if you want fast restart without re-syncing,
  the **journal** (Fjall dir, or the `accord_*` SQL tables). The journal is bounded by
  compaction; the event store grows with history. Back up per node; a restored node also
  re-syncs missing tail from peers via anti-entropy.
- **SQL backend:** run the schema via `evento-sql-migrator` with its `accord` feature
  (the `accord_commands/meta/metadata_log/acceptors` tables), alongside your event
  schema — one migrator for both.

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
- [ ] Backups of the event store (and journal) per node; restore drill rehearsed.
- [ ] `linearizable_reads` decision made (freshness vs. read latency).
- [ ] Load/soak test on representative hardware before trusting it — and note the
      project's verdict: independent review + multi-day soak are still outstanding.
