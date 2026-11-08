//! What a local backend offers the cluster beyond being an [`Executor`]: the
//! consensus journal colocated with it, and the two ways a node empties it
//! before opening by joining.
//!
//! A deployer never calls these directly; the host that opens a node does.
//! A backend crate implements the trait under its `cluster` feature
//! (`evento-turso`, `evento-sql`, `evento-fjall`).

use std::sync::Arc;

use async_trait::async_trait;
use evento_core::Executor;

use crate::Journal;

/// A local event store a cluster node can run over: its [`Executor`] serves
/// reads, subscriptions and snapshots, its [`Journal`] keeps the consensus
/// state, in the same database.
///
/// Both halves live together on purpose: a node whose journal and store
/// could be lost separately is a node that may vote twice, or hold events it
/// never voted for (see `OPERATIONS.md` §7, lost journal).
#[async_trait]
pub trait ClusterBackend: Executor + Clone + Send + Sync + 'static {
    /// Opens the consensus journal colocated with this store, creating its
    /// tables (or keyspaces) when they are missing. Called once per node
    /// open; idempotent.
    async fn open_journal(&self) -> anyhow::Result<Arc<dyn Journal>>;

    /// Empties the store of its events, snapshots and subscription cursors
    /// and keeps the consensus journal: what a node does before copying a
    /// peer's store (`OPERATIONS.md` §7, "Opening a node by joining"). The
    /// subscriptions start over from the copied events.
    async fn empty_for_join(&self) -> anyhow::Result<()>;

    /// Discards the consensus journal too: every command, decision, fence
    /// and clock bound it holds. Only for a node whose journal was found to
    /// be an older copy of itself (`JournalRolledBack`): it comes back
    /// joining over an empty journal, under the amnesia fence. Never call it
    /// on a node that is running.
    async fn discard_journal(&self) -> anyhow::Result<()>;
}
