# Phase 3: Protocol Engine

## Objective

Implement the ACCORD protocol phases: PreAccept, Accept, Commit, Execute. This is the core consensus logic that coordinates between nodes.

## Files to Create

```
evento-accord/src/
├── protocol/
│   ├── mod.rs
│   ├── messages.rs     # Protocol messages
│   ├── preaccept.rs    # Phase 1: Fast path
│   ├── accept.rs       # Phase 2: Slow path
│   ├── commit.rs       # Phase 3: Commit decision
│   ├── execute.rs      # Phase 4: Apply transaction
│   └── recovery.rs     # Recovery protocol
```

## ACCORD Protocol Overview

```
Coordinator                    Replicas (quorum)
     │                              │
     │──── PreAccept(txn) ─────────▶│
     │                              │ compute deps
     │◀─── PreAcceptOK(deps) ───────│
     │                              │
     │  [if all deps agree: FAST PATH]
     │                              │
     │──── Commit(txn, deps) ───────▶│
     │                              │
     │  [if deps disagree: SLOW PATH]
     │                              │
     │──── Accept(txn, merged_deps) ▶│
     │◀─── AcceptOK ────────────────│
     │──── Commit(txn, deps) ───────▶│
     │                              │
     │  [Execute after deps satisfied]
```

## Tasks

### 3.1 Protocol Messages

**File**: `src/protocol/messages.rs`

```rust
use crate::{Transaction, TxnId, Timestamp, Ballot};

/// All protocol message types
#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub enum Message {
    // Phase 1: PreAccept
    PreAccept(PreAcceptMsg),
    PreAcceptOK(PreAcceptOKMsg),
    PreAcceptNack(PreAcceptNackMsg),

    // Phase 2: Accept (slow path only)
    Accept(AcceptMsg),
    AcceptOK(AcceptOKMsg),
    AcceptNack(AcceptNackMsg),

    // Phase 3: Commit
    Commit(CommitMsg),
    CommitOK(CommitOKMsg),

    // Recovery
    Recover(RecoverMsg),
    RecoverReply(RecoverReplyMsg),
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct PreAcceptMsg {
    pub txn: Transaction,
    pub ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct PreAcceptOKMsg {
    pub txn_id: TxnId,
    pub deps: Vec<TxnId>,
    pub ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct PreAcceptNackMsg {
    pub txn_id: TxnId,
    pub higher_ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct AcceptMsg {
    pub txn_id: TxnId,
    pub deps: Vec<TxnId>,
    pub execute_at: Timestamp,
    pub ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct AcceptOKMsg {
    pub txn_id: TxnId,
    pub ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct AcceptNackMsg {
    pub txn_id: TxnId,
    pub higher_ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct CommitMsg {
    pub txn_id: TxnId,
    pub deps: Vec<TxnId>,
    pub execute_at: Timestamp,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct CommitOKMsg {
    pub txn_id: TxnId,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct RecoverMsg {
    pub txn_id: TxnId,
    pub ballot: Ballot,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct RecoverReplyMsg {
    pub txn_id: TxnId,
    pub status: Option<TxnStatus>,
    pub deps: Vec<TxnId>,
    pub execute_at: Option<Timestamp>,
    pub ballot: Ballot,
}
```

**Acceptance Criteria**:
- [ ] All messages serialize/deserialize correctly
- [ ] Message types cover full protocol
- [ ] Unit tests for serialization round-trip

---

### 3.2 PreAccept Phase (Fast Path)

**File**: `src/protocol/preaccept.rs`

```rust
use crate::{
    state::ConsensusState,
    protocol::messages::*,
    transport::Transport,
    Transaction, TxnId, Ballot, Result, AccordError,
};

pub struct PreAcceptPhase<'a, T: Transport> {
    state: &'a ConsensusState,
    transport: &'a T,
    quorum_size: usize,
}

impl<'a, T: Transport> PreAcceptPhase<'a, T> {
    /// Run PreAccept as coordinator
    pub async fn run(&self, txn: Transaction) -> Result<PreAcceptResult> {
        let ballot = Ballot { round: 0, node_id: self.transport.local_node() };

        // 1. Local PreAccept
        let local_deps = self.state.preaccept(txn.clone()).await?;

        // 2. Send to replicas
        let msg = Message::PreAccept(PreAcceptMsg {
            txn: txn.clone(),
            ballot,
        });

        let responses = self.transport.broadcast(&msg).await;

        // 3. Collect responses
        let mut all_deps: Vec<Vec<TxnId>> = vec![local_deps];
        let mut ok_count = 1; // Include local

        for response in responses {
            match response {
                Ok(Message::PreAcceptOK(ok)) => {
                    all_deps.push(ok.deps);
                    ok_count += 1;
                }
                Ok(Message::PreAcceptNack(nack)) => {
                    // Higher ballot seen, need to retry with higher ballot
                    return Err(AccordError::BallotTooLow(nack.higher_ballot));
                }
                Err(e) => {
                    tracing::warn!("PreAccept failed: {}", e);
                }
            }
        }

        // 4. Check quorum
        if ok_count < self.quorum_size {
            return Err(AccordError::QuorumNotReached {
                got: ok_count,
                need: self.quorum_size,
            });
        }

        // 5. Check if fast path (all deps agree)
        if self.all_deps_agree(&all_deps) {
            Ok(PreAcceptResult::FastPath {
                deps: all_deps.into_iter().next().unwrap(),
            })
        } else {
            Ok(PreAcceptResult::SlowPath {
                merged_deps: self.merge_deps(all_deps),
            })
        }
    }

    /// Handle PreAccept as replica
    pub async fn handle(&self, msg: PreAcceptMsg) -> Result<Message> {
        // Check ballot
        if let Some(existing) = self.state.get(&msg.txn.id).await {
            if existing.ballot > msg.ballot {
                return Ok(Message::PreAcceptNack(PreAcceptNackMsg {
                    txn_id: msg.txn.id,
                    higher_ballot: existing.ballot,
                }));
            }
        }

        // Compute local deps
        let deps = self.state.preaccept(msg.txn).await?;

        Ok(Message::PreAcceptOK(PreAcceptOKMsg {
            txn_id: msg.txn.id,
            deps,
            ballot: msg.ballot,
        }))
    }

    fn all_deps_agree(&self, all_deps: &[Vec<TxnId>]) -> bool {
        all_deps.windows(2).all(|w| w[0] == w[1])
    }

    fn merge_deps(&self, all_deps: Vec<Vec<TxnId>>) -> Vec<TxnId> {
        let mut merged: BTreeSet<TxnId> = BTreeSet::new();
        for deps in all_deps {
            merged.extend(deps);
        }
        merged.into_iter().collect()
    }
}

pub enum PreAcceptResult {
    FastPath { deps: Vec<TxnId> },
    SlowPath { merged_deps: Vec<TxnId> },
}
```

**Acceptance Criteria**:
- [ ] Fast path when all replicas agree on deps
- [ ] Slow path when deps differ
- [ ] Proper quorum checking
- [ ] Ballot handling for concurrent coordinators
- [ ] Unit tests with mock transport

---

### 3.3 Accept Phase (Slow Path)

**File**: `src/protocol/accept.rs`

```rust
pub struct AcceptPhase<'a, T: Transport> {
    state: &'a ConsensusState,
    transport: &'a T,
    quorum_size: usize,
}

impl<'a, T: Transport> AcceptPhase<'a, T> {
    /// Run Accept as coordinator (only for slow path)
    pub async fn run(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        ballot: Ballot,
    ) -> Result<Timestamp> {
        // Compute execute_at as max(deps.execute_at) + 1
        let execute_at = self.compute_execute_at(&deps).await;

        // Update local state
        self.state.accept(txn_id, deps.clone(), execute_at).await?;

        // Send to replicas
        let msg = Message::Accept(AcceptMsg {
            txn_id,
            deps: deps.clone(),
            execute_at,
            ballot,
        });

        let responses = self.transport.broadcast(&msg).await;

        // Check quorum
        let ok_count = responses
            .iter()
            .filter(|r| matches!(r, Ok(Message::AcceptOK(_))))
            .count() + 1; // Include local

        if ok_count < self.quorum_size {
            return Err(AccordError::QuorumNotReached {
                got: ok_count,
                need: self.quorum_size,
            });
        }

        Ok(execute_at)
    }

    /// Handle Accept as replica
    pub async fn handle(&self, msg: AcceptMsg) -> Result<Message> {
        // Check ballot
        if let Some(existing) = self.state.get(&msg.txn_id).await {
            if existing.ballot > msg.ballot {
                return Ok(Message::AcceptNack(AcceptNackMsg {
                    txn_id: msg.txn_id,
                    higher_ballot: existing.ballot,
                }));
            }
        }

        // Update state
        self.state.accept(msg.txn_id, msg.deps, msg.execute_at).await?;

        Ok(Message::AcceptOK(AcceptOKMsg {
            txn_id: msg.txn_id,
            ballot: msg.ballot,
        }))
    }

    async fn compute_execute_at(&self, deps: &[TxnId]) -> Timestamp {
        let mut max = Timestamp::ZERO;
        for dep in deps {
            if let Some(txn) = self.state.get(dep).await {
                if txn.execute_at > max {
                    max = txn.execute_at;
                }
            }
        }
        max.increment()
    }
}
```

**Acceptance Criteria**:
- [ ] Correct execute_at computation
- [ ] Quorum checking
- [ ] Ballot handling
- [ ] State updated correctly
- [ ] Unit tests

---

### 3.4 Commit Phase

**File**: `src/protocol/commit.rs`

```rust
pub struct CommitPhase<'a, T: Transport> {
    state: &'a ConsensusState,
    transport: &'a T,
}

impl<'a, T: Transport> CommitPhase<'a, T> {
    /// Commit transaction (fire and forget to replicas)
    pub async fn run(
        &self,
        txn_id: TxnId,
        deps: Vec<TxnId>,
        execute_at: Timestamp,
    ) -> Result<()> {
        // Update local state
        self.state.commit(txn_id).await?;

        // Notify replicas (don't wait for response)
        let msg = Message::Commit(CommitMsg {
            txn_id,
            deps,
            execute_at,
        });

        self.transport.broadcast_no_wait(&msg);

        Ok(())
    }

    /// Handle Commit as replica
    pub async fn handle(&self, msg: CommitMsg) -> Result<Message> {
        // Ensure we have the transaction
        if self.state.get(&msg.txn_id).await.is_none() {
            // We missed earlier phases, create from commit
            self.state.create_from_commit(
                msg.txn_id,
                msg.deps,
                msg.execute_at,
            ).await?;
        } else {
            self.state.commit(msg.txn_id).await?;
        }

        Ok(Message::CommitOK(CommitOKMsg {
            txn_id: msg.txn_id,
        }))
    }
}
```

**Acceptance Criteria**:
- [ ] Commit is durable before returning
- [ ] Replicas handle missing PreAccept gracefully
- [ ] Unit tests

---

### 3.5 Execute Phase

**File**: `src/protocol/execute.rs`

```rust
use evento_core::{Executor, Event};

pub struct ExecutePhase<'a, E: Executor> {
    state: &'a ConsensusState,
    executor: &'a E,
}

impl<'a, E: Executor> ExecutePhase<'a, E> {
    /// Execute transaction (after all deps satisfied)
    pub async fn run(&self, txn_id: TxnId) -> Result<()> {
        let txn = self.state.get(&txn_id).await
            .ok_or(AccordError::TxnNotFound(txn_id))?;

        // Wait for all dependencies to execute
        for dep_id in &txn.deps {
            self.wait_for_execution(dep_id).await?;
        }

        // Decode events
        let events: Vec<Event> = bitcode::decode(&txn.events_data)?;

        // Write to underlying executor
        self.executor.write(events).await?;

        // Mark as executed
        self.state.mark_executed(txn_id).await?;

        Ok(())
    }

    async fn wait_for_execution(&self, txn_id: &TxnId) -> Result<()> {
        loop {
            match self.state.get_status(txn_id).await {
                Some(TxnStatus::Executed) => return Ok(()),
                Some(_) => {
                    // Still waiting, brief sleep
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
                None => {
                    // Unknown transaction, might need recovery
                    return Err(AccordError::TxnNotFound(*txn_id));
                }
            }
        }
    }
}
```

**Acceptance Criteria**:
- [ ] Dependencies execute before this transaction
- [ ] Events written to inner executor
- [ ] State marked as executed
- [ ] Unit tests with mock executor

---

### 3.6 Protocol Coordinator

**File**: `src/protocol/mod.rs`

Unified interface for running the full protocol.

```rust
mod messages;
mod preaccept;
mod accept;
mod commit;
mod execute;
mod recovery;

pub use messages::*;

use crate::{state::ConsensusState, transport::Transport, Transaction, Result};
use evento_core::Executor;

pub struct Protocol<E: Executor, T: Transport> {
    state: ConsensusState,
    executor: E,
    transport: T,
    quorum_size: usize,
}

impl<E: Executor, T: Transport> Protocol<E, T> {
    /// Run full ACCORD protocol for a transaction
    pub async fn submit(&self, txn: Transaction) -> Result<()> {
        // Phase 1: PreAccept
        let preaccept = PreAcceptPhase::new(&self.state, &self.transport, self.quorum_size);
        let result = preaccept.run(txn.clone()).await?;

        let (deps, execute_at) = match result {
            PreAcceptResult::FastPath { deps } => {
                // Fast path: use original timestamp
                (deps, txn.id.timestamp)
            }
            PreAcceptResult::SlowPath { merged_deps } => {
                // Phase 2: Accept (slow path only)
                let accept = AcceptPhase::new(&self.state, &self.transport, self.quorum_size);
                let execute_at = accept.run(txn.id, merged_deps.clone(), ballot).await?;
                (merged_deps, execute_at)
            }
        };

        // Phase 3: Commit
        let commit = CommitPhase::new(&self.state, &self.transport);
        commit.run(txn.id, deps.clone(), execute_at).await?;

        // Phase 4: Execute
        let execute = ExecutePhase::new(&self.state, &self.executor);
        execute.run(txn.id).await?;

        Ok(())
    }

    /// Handle incoming message from another node
    pub async fn handle_message(&self, msg: Message) -> Result<Message> {
        match msg {
            Message::PreAccept(m) => {
                PreAcceptPhase::new(&self.state, &self.transport, self.quorum_size)
                    .handle(m).await
            }
            Message::Accept(m) => {
                AcceptPhase::new(&self.state, &self.transport, self.quorum_size)
                    .handle(m).await
            }
            Message::Commit(m) => {
                CommitPhase::new(&self.state, &self.transport)
                    .handle(m).await
            }
            // ... other message types
        }
    }
}
```

**Acceptance Criteria**:
- [ ] Full protocol flow works end-to-end
- [ ] Message handling dispatches correctly
- [ ] Integration tests with multiple mock nodes

---

## Definition of Done

- [ ] All protocol phases implemented
- [ ] Message serialization works
- [ ] Fast path and slow path tested
- [ ] Recovery protocol handles crashed coordinators
- [ ] Integration test: 3-node cluster with conflicting transactions
