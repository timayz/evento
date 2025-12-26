# evento-accord

ACCORD consensus protocol implementation for evento.

## Overview

This crate adds distributed consensus to evento event writes using the ACCORD protocol. It provides:

- **Strict Serializability**: All nodes observe the same order of conflicting transactions
- **Low Latency**: Single round-trip for non-conflicting transactions (fast path)
- **Fault Tolerance**: Survive f failures with 2f+1 nodes
- **Seamless Integration**: Drop-in replacement via `Executor` trait wrapper

## Quick Start

### Single Node (Development)

```rust
use evento_accord::AccordExecutor;
use evento_sql::SqlExecutor;

let sql_executor = SqlExecutor::new(pool).await?;
let executor = AccordExecutor::single_node(sql_executor);

// Use like any other executor
evento::create()
    .event(&MyEvent { ... })
    .commit(&executor)
    .await?;
```

### Cluster Mode (Production)

```rust
use evento_accord::{AccordExecutor, AccordConfig, NodeAddr};

let config = AccordConfig::cluster(
    NodeAddr { id: 0, host: "node1".into(), port: 5000 },
    vec![
        NodeAddr { id: 0, host: "node1".into(), port: 5000 },
        NodeAddr { id: 1, host: "node2".into(), port: 5000 },
        NodeAddr { id: 2, host: "node3".into(), port: 5000 },
    ],
);

let executor = AccordExecutor::cluster(sql_executor, config).await?;
```

## Documentation

See the `docs/` folder for detailed implementation plans:

- [PRD.md](docs/PRD.md) - Product requirements
- [phase-1-core-types.md](docs/phase-1-core-types.md) - Core types and data structures
- [phase-2-state-machine.md](docs/phase-2-state-machine.md) - Consensus state machine
- [phase-3-protocol.md](docs/phase-3-protocol.md) - Protocol engine
- [phase-4-transport.md](docs/phase-4-transport.md) - TCP transport layer
- [phase-5-integration.md](docs/phase-5-integration.md) - AccordExecutor integration
- [phase-6-testing.md](docs/phase-6-testing.md) - Testing and hardening

## References

- [ACCORD Whitepaper](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf)
- [Apache Cassandra Accord](https://github.com/apache/cassandra-accord)
