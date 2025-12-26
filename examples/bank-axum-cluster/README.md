# Bank Axum Cluster Example

A bank application running on a 3-node ACCORD consensus cluster, demonstrating distributed event sourcing with Evento.

## Overview

This example shows how to run a web application across multiple nodes with:

- **ACCORD Consensus**: All writes are coordinated across the cluster
- **Event Sourcing**: All changes are stored as immutable events
- **Per-Node Storage**: Each node maintains its own SQLite database
- **Read-Your-Writes Consistency**: Writes on a node are immediately visible to reads on that node

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Node 0      │    │     Node 1      │    │     Node 2      │
│  HTTP :3000     │    │  HTTP :3001     │    │  HTTP :3002     │
│  ACCORD :9000   │◄──►│  ACCORD :9001   │◄──►│  ACCORD :9002   │
│  SQLite DB 0    │    │  SQLite DB 1    │    │  SQLite DB 2    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Running the Cluster

Start each node in a separate terminal:

```bash
# Terminal 1 - Node 0
cargo run -p bank-axum-cluster -- --node 0

# Terminal 2 - Node 1
cargo run -p bank-axum-cluster -- --node 1

# Terminal 3 - Node 2
cargo run -p bank-axum-cluster -- --node 2
```

### Restarting Nodes

Nodes can be safely restarted and will automatically recover their state:

```bash
# Stop a node with Ctrl+C, then restart it
cargo run -p bank-axum-cluster -- --node 0
```

The node will:
1. Load previously executed transaction IDs from the `accord_txn` table
2. Restore its state to skip re-executing those transactions
3. Resume normal operation

### Fresh Start

To start with a completely clean database (deletes all data):

```bash
# Start all nodes fresh
cargo run -p bank-axum-cluster -- --node 0 --fresh
cargo run -p bank-axum-cluster -- --node 1 --fresh
cargo run -p bank-axum-cluster -- --node 2 --fresh
```

> **Note**: The `--fresh` flag deletes the node's database file before starting.
> Only use this when you want to reset all data.

## Accessing the Web UI

Once all nodes are running, access any node's web interface:

| Node | Web UI | ACCORD Port |
|------|--------|-------------|
| 0 | http://127.0.0.1:3000 | 9000 |
| 1 | http://127.0.0.1:3001 | 9001 |
| 2 | http://127.0.0.1:3002 | 9002 |

The UI includes links to switch between nodes so you can verify data replication.

## Features to Try

1. **Create an Account**: Create an account on Node 0
2. **View on Another Node**: Switch to Node 1 or 2 and see the account
3. **Deposit/Withdraw**: Make transactions from any node
4. **Transfer**: Transfer money between accounts

## How It Works

### Write Path

1. User submits a form (e.g., create account, deposit)
2. The node creates events for the action
3. Events go through ACCORD consensus:
   - PreAccept: Propose transaction to all nodes
   - Accept (if needed): Finalize dependencies
   - Commit: Mark transaction as committed
4. All nodes execute the transaction to their local databases
5. The originating node waits for local execution before responding

### Read Path

1. User requests data (e.g., view account)
2. Read goes directly to the local node's database
3. No consensus required for reads

### Consistency Model

- **Read-Your-Writes**: After a write completes on a node, reads on that same node see the write immediately
- **Eventual Consistency**: Other nodes will see the write after they execute the committed transaction

## Database Files

Each node stores its data in a separate SQLite file:

```
target/tmp/bank_cluster_node0.db
target/tmp/bank_cluster_node1.db
target/tmp/bank_cluster_node2.db
```

To start fresh, delete these files before running.

## Configuration

The cluster configuration is defined in `src/main.rs`:

```rust
const CLUSTER_NODES: &[(&str, u16, u16)] = &[
    // (host, accord_port, http_port)
    ("127.0.0.1", 9000, 3000),
    ("127.0.0.1", 9001, 3001),
    ("127.0.0.1", 9002, 3002),
];
```

## Graceful Shutdown

Press `Ctrl+C` to gracefully shut down a node. The ACCORD shutdown handle ensures all background tasks complete properly.

## Troubleshooting

### "Address already in use"

A previous instance may still be running. Kill it or wait for the port to be released:

```bash
# Find and kill processes on the ports
lsof -i :3000 -i :9000
```

### Nodes can't connect to each other

Ensure all nodes are started. ACCORD requires all nodes to be available for consensus.

### Data not appearing on other nodes

Check the logs for consensus errors. All nodes must participate in consensus for writes to succeed.
