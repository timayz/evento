# evento-accord — Jepsen harness

An **independent, adversarial** verification of evento-accord, complementing the
in-repo deterministic simulation (`crates/accord/tests/simulation.rs`). Jepsen runs
a real 5-node cluster, injects real OS-level faults, records an externally-observed
history of operations, and checks it with **Elle** for strict-serializability
anomalies. This is the top Phase-D sign-off item in `../DESIGN.md`.

## What it tests

Each node runs the `jepsen-node` binary (`examples/jepsen-node`): one Accord replica
over real TCP, a durable Fjall event store, a durable `FjallJournal`, and a small
HTTP API. The Clojure test (`src/evento_accord/`) drives it as an **Elle list-append**
workload:

| Elle              | evento-accord                                                         |
|-------------------|----------------------------------------------------------------------|
| key               | `aggregate_id`                                                        |
| list element      | one event (value in `Event.data`)                                    |
| `[:append k v]`   | conditional append at `version = current+1`                          |
| multi-key append  | one atomic `executor.write(Vec<Event>)` across aggregates           |
| `[:r k]`          | read the aggregate's events, values in version order                |
| conflict (`:fail`)| `WriteError::InvalidOriginalVersion`                                 |
| indeterminate (`:info`) | timeout / 5xx / dropped connection                            |

### Workload soundness

The workload is matched to what evento-accord *promises*:

* **WRITE** transactions append to **1..N distinct keys atomically** (a single
  consensus write — the cross-key edge Elle uses to relate keys).
* **READ** transactions read a **single** key. Reads are served from each replica's
  local backend *per aggregate* and are **not** coordinated across keys, so the
  harness never requests a multi-key snapshot the system does not provide.

Interpreting a failure:

* A **write-cycle** anomaly (G0/G1/lost-update among append txns) ⇒ a consensus
  **safety** bug.
* A **read-related** anomaly under partition ⇒ reads off a partitioned replica are
  not linearizable (evento-accord serves local reads without a read barrier). That
  is a real, expected-to-investigate property, not necessarily a consensus bug.

## First-run finding (2026-06-15)

The first clean run (5 nodes, partition nemesis) produced a precise result:

* **`--consistency-model serializable` ⇒ `:valid? true`.** No serializability
  anomalies: the atomic multi-key appends commit in a single global order, with no
  G0/G1c/lost-update. The consensus/write core is sound under partitions.
* **`--consistency-model strict-serializable` (default) ⇒ `:valid? false`**, with a
  `:G-single-item-realtime` cycle. A witnessed example: an atomic append committed
  `737→key2` and `738→key1` together; a later read (on one replica) saw `737`, then
  a still-later read (on another replica) did **not** see `738`. The violating edge
  is **real-time** — a read served off a *lagging* replica returned state
  inconsistent with wall-clock order.

So evento-accord, as exercised here, is **serializable with atomic, strictly-ordered
multi-key writes, but its local reads are not linearizable**: with no read barrier, a
read off a behind/partitioned replica can be stale. That is consistent with the
design ("reads are served locally from the applied `DataStore`"), and it is a genuine
gap against the "strictly-serializable" claim — exactly the kind of result this
adversarial harness exists to surface.

### Closing the gap: `--linearizable-reads`

A linearizable read path (`Node::read_barrier`, gated by `NodeConfig.linearizable_reads`)
addresses it. It is a lightweight **read-index** that stores nothing (reads never enter
the conflict graph or journal): a read of an owned key (1) probes a slow quorum (`f+1`)
of the key's replicas for the dependencies a read at a fresh timestamp would witness
(`ReadProbe`), then (2) waits until those deps are applied locally, then serves the
backend. Any write that committed before the read is committed on a quorum, which
intersects the probe quorum, so it is in the deps — and the local wait fences the read
behind it.

Validated (`./run.sh --linearizable-reads`, quorum-preserving `:one` partition unless
noted; Elle's `:cycle-search-timeout` raised to 60 s so high-contention runs reach a
*conclusive* verdict instead of timing out):

| Run | conc | Model | Verdict |
|-----|------|-------|---------|
| healthy | 10 | strict-serializable | **valid** (2920 ok) |
| `:one` partition | 2 | strict-serializable | **valid** (2080 ok) |
| `:one` partition | 5 | strict-serializable | **valid** (1734 ok) |
| `:one` partition | 10 | strict-serializable | **valid** (1922 ok) — *after the convergence fix below* |
| `:one` partition | 10 | serializable | **valid** (2788 ok) |

So with linearizable reads the cluster is **strict-serializable under quorum-preserving
partition** (and **always serializable**). Getting the high-concurrency case (10) to
pass took a real bug fix.

#### The bug the harness found: anti-entropy never converged a churned node

At concurrency 10 the run was first **invalid** with a *confirmed* (not a search-timeout)
`G-nonadjacent-item-realtime` — a multi-key cycle of read/write **anti-dependencies**
(stale reads), *not* the "premature visibility" we first guessed. A deterministic Rust
reproduction (`tests/linearizable_stress.rs`: concurrent writers/readers across nodes
with node crash/heal churn, a monotonicity + premature-read oracle, and a per-node
convergence probe) localized it: **churned nodes never caught up**, even after the
network fully healed (e.g. `[66,66,66,5,54]` value counts across nodes for one key).

Root cause in `Replica::import_applied`: a node that received a transaction's `Commit`
but missed its `Apply` (it was briefly down) holds it at status `Committed` with **no
decision**, so normal execution never applies it (that needs a decision) — *and*
anti-entropy skipped it, because `import_applied` returned "already present" for any
known command. So the command was stuck at `Committed` forever and the node diverged
permanently; reads served off it were stale. The fix: `import_applied` now adopts the
peer's *applied* state for a known-but-unapplied command (applying its events) instead
of skipping. With it, nodes converge and the conc-10 strict-serializable run is valid.

The barrier's cost is still real: one quorum round-trip per read, and it is CP — a
partition that leaves no quorum makes reads **unavailable** (`:info`) rather than stale.

## Running it

Requires Docker (with the `compose` plugin). Everything else (JVM, Leiningen, the
Rust toolchain) runs inside containers.

```bash
cd crates/accord/jepsen
./run.sh                                    # partition+kill+pause, 120s
TIME_LIMIT=600 ./run.sh                     # longer
./run.sh --consistency-model serializable   # check serializable instead of strict
./run.sh --faults partition                 # just one fault ("" for none)
./run.sh --linearizable-reads               # enable read barriers (linearizable reads)
./run.sh --concurrency 20                   # any extra arg passes through to lein run test
```

`run.sh` generates a throwaway SSH keypair, builds the node image (which compiles
`jepsen-node`) and the control image, brings up `control` + `n1..n5`, and runs the
test. Results land under `store/<test-name>/latest/` — `results.edn` (look for
`:valid? true`), `timeline.html`, and `elle/` graphs. Tear down with
`docker compose -f docker/docker-compose.yml down -v`.

## Faults (`--faults`, default `partition,kill,pause`)

Built on `jepsen.nemesis.combined`, composed by hand (so only the requested packages
are constructed — `nemesis-package` also builds a file-corruption package whose setup
downloads a tool and fails offline):

* **`partition`** — network partitions (`:one`, `:majority`, `:majorities-ring`) via
  iptables, then heal.
* **`kill`** — SIGKILL one node or **all** nodes, then restart from the durable
  journal (`db/Kill` → `evento-accord.db`, `pkill -KILL` + `start-daemon!`). Exercises
  restart/journal recovery.
* **`pause`** — SIGSTOP/SIGCONT one or all nodes (`db/Pause`).
* **`clock`** — *available but off by default.* Jepsen's clock nemesis bumps the
  kernel wall clock; a shared-kernel Docker cluster can't isolate that from the host,
  so it belongs on real VMs (and needs a C compiler on the node image for jepsen's
  time helper). Pair with raised `NodeConfig` timeouts on `jepsen-node` for geo runs.

A run with `--faults partition,kill,pause --consistency-model serializable` was
**valid** — the history stayed serializable even through full-cluster kills (all five
nodes down, then journal-recovered) and pauses, confirming the write/consensus core's
safety under compound faults. (Strict-serializable still fails on the read-linearizability
gap above, independent of which faults are active.)

## Layout

```
project.clj                     deps (jepsen bundles Elle), main = core
src/evento_accord/client.clj    HTTP client; ok/fail/info mapping
src/evento_accord/db.clj        start/stop/kill/pause the binary on each node
src/evento_accord/core.clj      workload, generator, nemesis, checker, CLI
docker/Dockerfile.node          builds jepsen-node + sshd + iptables
docker/Dockerfile.control       JVM + lein + the test project
docker/docker-compose.yml       control + n1..n5
run.sh                          build, up, run
```
