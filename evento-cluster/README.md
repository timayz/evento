# evento-cluster

The clustered evento `Executor`: wrap a local store and get a **leaderless,
strictly-serializable, highly-available** replicated event store. Replication
runs on the **Accord** consensus protocol (Apache Cassandra [CEP-15]): events
are replicated across `N = 2f + 1` nodes with a single global serial order and
no elected leader, tolerating `f` failures.

> **Status: alpha.** A well-tested reference implementation, not yet
> production-hardened (pending independent review + real-cluster soak). See
> [`DESIGN.md`](./DESIGN.md) for the architecture and milestone plan, and
> [`OPERATIONS.md`](./OPERATIONS.md) for the deployment/ops runbook.

Part of the [evento](https://github.com/timayz/evento#readme) event sourcing toolkit;
see [`examples/bank-axum-cluster`](../examples/bank-axum-cluster) for a runnable 1- or
3-node demo (`make cluster` / `make cluster.all`).

## What works

- **`ClusterExecutor`** — a drop-in `evento_core::Executor`. Writes are
  coordinated through the cluster (replicated, strictly serializable); reads,
  subscriptions, and snapshots are served from a local evento backend
  (`Fjall`/`Sql`) kept current by applying committed transactions.
- **Multi-shard, atomic transactions** — keys partition across disjoint shards,
  each with its own replica set and quorum. A write spanning aggregates commits
  all-or-nothing via a Read → Apply phase split (the basis for cross-aggregate
  transactions like a money transfer).
- **Fault tolerance** — quorum-based progress survives `f` failures per shard;
  a live node recovers a transaction whose coordinator died (ballot-fenced).
- **Real transport** — length-delimited framed TCP behind the `MessageSink`
  trait, plus an in-memory transport for deterministic tests.

## Layering

```
ClusterExecutor (evento_core::Executor)
        │
   Node (coordinator + replica) ── Replica (consensus state machine)
        │
   api traits: MessageSink · Topology · Clock · Journal · DataStore
```

The protocol depends only on the `api` traits; each has an in-memory
implementation for the test harness and a production one (framed TCP, static
membership, a Fjall/SQL-backed store).

## Also supported

- **Elastic membership** — epoch-versioned topology; nodes join (bootstrapping
  their key range) and leave; epoch changes are decided by Paxos over the current
  members, so they survive a coordinator crash and keep working after the founders
  have left; keys re-shard onto new owners.
- **Multi-shard reads** — a read for a key a node doesn't own is routed to an
  owner.

## Not yet

Garbage-collecting a moved key range from its old owners.

[CEP-15]: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15:+General+Purpose+Transactions
