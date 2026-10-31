//! A Turso-backed [`Journal`]: `evento_sql::SqlJournal` on the Turso engine,
//! for deployments that run evento on Turso (`evento-turso`) and want their
//! consensus state in the **same** database as their events — or in one of
//! its own.
//!
//! Statements are built with sea-query's SQLite dialect and run on one write
//! connection (transactions) and per-call read connections. Values reuse
//! evento-accord's tagged-bitcode encoding (`evento_accord::format`), so the
//! Turso journal shares the format-versioning story with the fjall and SQL
//! ones, and its four tables are those of `SqlJournal` (see
//! `migrations/0002_accord.sql`).
//!
//! **Group commit.** [`stage`](Journal::stage) buffers a command in memory;
//! [`flush`](Journal::flush) writes every buffered row in **one transaction**,
//! so a batch of consensus messages costs a single commit. (Staged rows become
//! durable — and query-visible — at `flush`, which satisfies the trait
//! contract; recovery reads via [`load_all`](Journal::load_all) only after a
//! restart, i.e. post-flush.)
//!
//! A multi-shard transaction's route and a recovery promise for a transaction
//! never witnessed are rows of `accord_meta` keyed `route/` / `promise/` + the
//! hex of the order-preserving transaction key, staged and flushed with the
//! commands, and truncated with them. A compacted multi-shard transaction's
//! decision record is a `decision/` row, staged the same way but kept by
//! `truncate` until the node drops it. The node's clock bound is the
//! `accord_meta` row `clock_bound`, written durably on its own and never
//! truncated. The replica's vote fence is the `accord_meta` row `vote_fence`,
//! and the marks it saw from its peers the row `peer_marks`, both staged and
//! flushed with the commands and never truncated.

use std::{path::Path, sync::Mutex};

use async_trait::async_trait;
use sea_query::{Expr, ExprTrait, Iden, OnConflict, Query};

use evento_accord::format::{decode_tagged, encode_tagged, RecordKind};
use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, CommandState, ConfigEntry, CopyState, DecisionRecord,
    DurableMark, FenceRecord, Journal, Key, NodeId, Timestamp, TxnId,
};

use crate::query::{self, Prepared};

/// `k` value under which the truncation watermark is stored in `accord_meta`.
const WATERMARK_KEY: &str = "redundant_before";
/// `k` value under which the rejoin bounds are stored in `accord_meta`.
const REJOIN_BOUNDS_KEY: &str = "rejoin_above";
/// `k` value under which the node's clock bound is stored in `accord_meta`.
const CLOCK_BOUND_KEY: &str = "clock_bound";
/// `accord_meta` key of the journal's identity (`Journal::save_born`).
const BORN_KEY: &str = "born";
/// `k` value under which the replica's amnesia fence is stored in
/// `accord_meta`. Untouched by `truncate`.
const AMNESIA_FENCE_KEY: &str = "amnesia_fence";
/// `k` value under which the replica's transition fences are stored in
/// `accord_meta`. Untouched by `truncate`.
const FENCES_KEY: &str = "fences";
/// `k` value under which the replica's vote fence is stored in
/// `accord_meta`. Staged like a command; untouched by `truncate`.
const VOTE_FENCE_KEY: &str = "vote_fence";
/// `k` value under which the marks the replica saw from its peers are stored
/// in `accord_meta`. Staged like a command; untouched by `truncate`.
const PEER_MARKS_KEY: &str = "peer_marks";
/// `accord_meta` key of the replica's copy state (`Journal::save_copy_state`).
const COPY_STATE_KEY: &str = "copy_state";
/// `accord_meta` key of the replica's slot GC epoch (`Journal::save_slot_gc`).
const SLOT_GC_KEY: &str = "slot_gc";
/// `k` prefix of a multi-shard transaction's route in `accord_meta`.
const ROUTE_PREFIX: &str = "route/";
/// `k` prefix of a blind recovery promise in `accord_meta`.
const PROMISE_PREFIX: &str = "promise/";
/// `k` prefix of a decision record in `accord_meta`. Untouched by `truncate`.
const DECISION_PREFIX: &str = "decision/";

/// Rows per multi-row statement: 2 binds per row keeps a chunk well under
/// SQLite's bind limit (32766).
const ROWS_PER_STATEMENT: usize = 8_000;

#[derive(Iden, Clone, Copy)]
enum AccordCommands {
    Table,
    Txn,
    Data,
}

#[derive(Iden, Clone, Copy)]
enum AccordMeta {
    Table,
    K,
    V,
}

#[derive(Iden, Clone, Copy)]
enum AccordMetadataLog {
    Table,
    Epoch,
    Layout,
}

#[derive(Iden, Clone, Copy)]
enum AccordAcceptors {
    Table,
    Epoch,
    State,
}

/// An order-preserving key for a timestamp: big-endian `(micros, logical, node)`,
/// matching [`Timestamp`]'s field-order `Ord`, so a byte-wise `<` on the BLOB key
/// equals `<` on the timestamp — letting `truncate` be a single range `DELETE`.
fn ts_key(ts: Timestamp) -> Vec<u8> {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&ts.micros.to_be_bytes());
    key.extend_from_slice(&ts.logical.to_be_bytes());
    key.extend_from_slice(&ts.node.0.to_be_bytes());
    key
}

fn txn_key(txn: TxnId) -> Vec<u8> {
    ts_key(txn.0)
}

/// The `accord_meta` key of a per-transaction record: `prefix` + the hex of
/// [`txn_key`].
fn meta_txn_key(prefix: &str, txn: TxnId) -> String {
    let mut key = String::with_capacity(prefix.len() + 40);
    key.push_str(prefix);
    for byte in txn_key(txn) {
        key.push_str(&format!("{byte:02x}"));
    }
    key
}

/// A `LIKE` pattern matching every `accord_meta` key under `prefix`.
fn like_prefix(prefix: &str) -> String {
    let mut pattern = prefix.to_string();
    pattern.push('%');
    pattern
}

/// A [`Journal`] persisting consensus state to a Turso database.
pub struct TursoJournal {
    db: turso::Database,
    /// The single writer; holds the flush and truncate transactions.
    write: tokio::sync::Mutex<turso::Connection>,
    /// Commands staged since the last [`flush`](Journal::flush), as `(key, value)`.
    staged: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    /// `accord_meta` rows (routes, promises, decision records) staged since
    /// the last flush, as `(txn, k, v)`.
    staged_meta: Mutex<Vec<(TxnId, String, Vec<u8>)>>,
    /// The `accord_meta` rows `truncate` keeps (the vote fence, the peer
    /// marks, …) staged since the last flush, as `(k, v)`, one per `k`.
    staged_kept: Mutex<Vec<(&'static str, Vec<u8>)>>,
    /// Serializes flushes — and truncations — so staged entries are only
    /// drained once their transaction has committed (a failed flush must
    /// leave them staged) and a truncation never commits between a flush's
    /// snapshot of the batch and its commit (the flush would re-insert what
    /// the truncation deleted).
    flush_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for TursoJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoJournal").finish_non_exhaustive()
    }
}

impl TursoJournal {
    /// Wraps a database as a journal and applies its pending
    /// [`ACCORD_MIGRATIONS`](crate::ACCORD_MIGRATIONS): one shared with a
    /// [`Turso`](crate::Turso) store (`TursoJournal::new(store.database().clone())`)
    /// or one of its own.
    pub async fn new(db: turso::Database) -> anyhow::Result<Self> {
        let mut write = db.connect()?;
        crate::ACCORD_MIGRATIONS
            .to_latest(&mut write)
            .await
            .map_err(|(applied, err)| {
                anyhow::anyhow!("journal migration failed after {applied} applied: {err}")
            })?;
        Ok(Self {
            db,
            write: tokio::sync::Mutex::new(write),
            staged: Mutex::new(Vec::new()),
            staged_meta: Mutex::new(Vec::new()),
            staged_kept: Mutex::new(Vec::new()),
            flush_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Opens (creating if absent) a journal of its own at `path`.
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("journal path is not UTF-8"))?;
        let db = turso::Builder::new_local(path).build().await?;
        Self::new(db).await
    }

    /// A fresh in-memory journal: for tests and simulations.
    pub async fn in_memory() -> anyhow::Result<Self> {
        let db = turso::Builder::new_local(":memory:").build().await?;
        Self::new(db).await
    }

    /// The underlying database.
    pub fn database(&self) -> &turso::Database {
        &self.db
    }

    fn reader(&self) -> anyhow::Result<turso::Connection> {
        Ok(self.db.connect()?)
    }

    /// Stages `value` as the `accord_meta` row `key`, one `truncate` keeps,
    /// replacing whatever was staged for it.
    fn stage_kept(&self, key: &'static str, value: Vec<u8>) {
        let mut kept = self.staged_kept.lock().expect("journal poisoned");
        kept.retain(|(k, _)| *k != key);
        kept.push((key, value));
    }

    /// The value stored under `key` in `accord_meta`, if any.
    async fn load_meta(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let q = query::prepare(
            &Query::select()
                .column(AccordMeta::V)
                .from(AccordMeta::Table)
                .and_where(Expr::col(AccordMeta::K).eq(key))
                .to_owned(),
        );
        match query::fetch_optional_prepared(&self.reader()?, q).await? {
            Some(row) => Ok(Some(query::blob(&row, 0)?)),
            None => Ok(None),
        }
    }

    /// Upserts `rows` (`k`, `v`) into `accord_meta` in one statement. Each key
    /// at most once per call.
    fn upsert_meta(rows: &[(&str, &[u8])]) -> Prepared {
        let mut statement = Query::insert()
            .into_table(AccordMeta::Table)
            .columns([AccordMeta::K, AccordMeta::V])
            .on_conflict(
                OnConflict::column(AccordMeta::K)
                    .update_column(AccordMeta::V)
                    .to_owned(),
            )
            .to_owned();
        for (key, value) in rows {
            statement.values_panic([(*key).into(), (*value).into()]);
        }
        query::prepare(&statement)
    }

    /// Upserts one `accord_meta` row durably, on its own.
    async fn save_meta(&self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        let q = Self::upsert_meta(&[(key, value)]);
        query::execute_prepared(&*self.write.lock().await, q).await?;
        Ok(())
    }

    /// The values of every `accord_meta` row whose key starts with `prefix`.
    async fn load_meta_prefixed(&self, prefix: &str) -> anyhow::Result<Vec<Vec<u8>>> {
        let q = query::prepare(
            &Query::select()
                .column(AccordMeta::V)
                .from(AccordMeta::Table)
                .and_where(Expr::col(AccordMeta::K).like(like_prefix(prefix)))
                .to_owned(),
        );
        query::fetch_rows_only(&self.reader()?, q)
            .await?
            .iter()
            .map(|row| query::blob(row, 0))
            .collect()
    }

    /// The epoch-keyed rows of `table`, ascending, decoded as `kind`.
    async fn load_epochs<I: Iden + Copy + 'static>(
        &self,
        table: I,
        key: I,
        value: I,
    ) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
        let q = query::prepare(
            &Query::select()
                .columns([key, value])
                .from(table)
                .order_by(key, sea_query::Order::Asc)
                .to_owned(),
        );
        query::fetch_rows_only(&self.reader()?, q)
            .await?
            .iter()
            .map(|row| Ok((query::int(row, 0)? as u64, query::blob(row, 1)?)))
            .collect()
    }
}

#[async_trait]
impl Journal for TursoJournal {
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        self.stage(state).await?;
        self.flush().await
    }

    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        let key = txn_key(state.txn);
        let value = encode_tagged(RecordKind::Command, state)?;
        self.staged
            .lock()
            .expect("journal poisoned")
            .push((key, value));
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        // Serialize flushes; entries leave `staged` only after their
        // transaction commits, so a failed flush keeps the batch for retry and
        // a concurrent `record` cannot observe "flushed" state that was never
        // made durable.
        let _guard = self.flush_lock.lock().await;

        let batch = self.staged.lock().expect("journal poisoned").clone();
        let meta_batch = self.staged_meta.lock().expect("journal poisoned").clone();
        let kept = self.staged_kept.lock().expect("journal poisoned").clone();
        if batch.is_empty() && meta_batch.is_empty() && kept.is_empty() {
            return Ok(());
        }

        // A command's state advances and is re-staged under the same txn, and
        // a multi-row upsert must carry each key at most once — dedupe keeping
        // the LAST staged value, which is what the per-row upserts converged to.
        let statements: Vec<Prepared> = {
            let mut seen = std::collections::HashSet::new();
            let mut rows: Vec<&(Vec<u8>, Vec<u8>)> = batch
                .iter()
                .rev()
                .filter(|(key, _)| seen.insert(key.as_slice()))
                .collect();
            rows.reverse();

            // One multi-row upsert per chunk, all chunks in one transaction: a
            // batch of N staged commands costs one commit and ⌈N/8000⌉
            // statements instead of N round trips.
            let mut statements = Vec::new();
            for chunk in rows.chunks(ROWS_PER_STATEMENT) {
                let mut statement = Query::insert()
                    .into_table(AccordCommands::Table)
                    .columns([AccordCommands::Txn, AccordCommands::Data])
                    .on_conflict(
                        OnConflict::column(AccordCommands::Txn)
                            .update_column(AccordCommands::Data)
                            .to_owned(),
                    )
                    .to_owned();
                for (key, value) in chunk {
                    statement.values_panic([key.clone().into(), value.clone().into()]);
                }
                statements.push(query::prepare(&statement));
            }
            // The staged `accord_meta` rows, deduped the same way (last wins).
            let mut seen = std::collections::HashSet::new();
            let mut meta_rows: Vec<&(TxnId, String, Vec<u8>)> = meta_batch
                .iter()
                .rev()
                .filter(|(_, key, _)| seen.insert(key.as_str()))
                .collect();
            meta_rows.reverse();
            // The kept rows (one per `k`, keys disjoint from the per-transaction
            // prefixes) ride in the same multi-row statements.
            let rows: Vec<(&str, &[u8])> = meta_rows
                .iter()
                .map(|(_, key, value)| (key.as_str(), value.as_slice()))
                .chain(kept.iter().map(|(key, value)| (*key, value.as_slice())))
                .collect();
            for chunk in rows.chunks(ROWS_PER_STATEMENT) {
                statements.push(Self::upsert_meta(chunk));
            }
            statements
        };

        {
            let mut write = self.write.lock().await;
            let tx = write.transaction().await?;
            for q in statements {
                query::execute_prepared(&tx, q).await?;
            }
            tx.commit().await?;
        }

        // Drop exactly what was flushed; entries staged mid-transaction stay.
        // By entry, not by position: `drop_decisions` may have purged part of
        // `staged_meta` while the transaction ran, so a batch is not always a
        // prefix of its buffer.
        let flushed: std::collections::HashSet<&(Vec<u8>, Vec<u8>)> = batch.iter().collect();
        self.staged
            .lock()
            .expect("journal poisoned")
            .retain(|entry| !flushed.contains(entry));
        let flushed: std::collections::HashSet<&(TxnId, String, Vec<u8>)> =
            meta_batch.iter().collect();
        self.staged_meta
            .lock()
            .expect("journal poisoned")
            .retain(|entry| !flushed.contains(entry));
        // A row staged again while the transaction ran is a newer one: it
        // stays.
        self.staged_kept
            .lock()
            .expect("journal poisoned")
            .retain(|entry| !kept.contains(entry));
        Ok(())
    }

    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        // Under `flush_lock`: a flush in flight cloned its batch before this
        // truncation and would re-insert the rows it deletes once the delete
        // committed. Waiting for that flush to commit (and holding the lock
        // against any later one) keeps the two serial.
        let _guard = self.flush_lock.lock().await;

        let bound = ts_key(before);
        let watermark = encode_tagged(RecordKind::Watermark, &before)?;

        let statements: Vec<Prepared> = {
            let mut statements = Vec::new();
            // Single range delete — the order-preserving key makes `txn < before`
            // a byte-wise comparison the database can do directly.
            statements.push(query::prepare(
                &Query::delete()
                    .from_table(AccordCommands::Table)
                    .and_where(Expr::col(AccordCommands::Txn).lt(bound.clone()))
                    .to_owned(),
            ));
            // Routes and blind promises go with the commands. Their keys are
            // the prefix + the hex of the order-preserving transaction key, so
            // `txn < before` is `k < prefix + hex(bound)`: one range delete per
            // prefix, nothing read back or decoded. The LIKE keeps the range
            // inside the prefix; within it the keys are lowercase hex of one
            // width, which orders numerically.
            for prefix in [ROUTE_PREFIX, PROMISE_PREFIX] {
                statements.push(query::prepare(
                    &Query::delete()
                        .from_table(AccordMeta::Table)
                        .and_where(Expr::col(AccordMeta::K).like(like_prefix(prefix)))
                        .and_where(Expr::col(AccordMeta::K).lt(meta_txn_key(prefix, TxnId(before))))
                        .to_owned(),
                ));
            }
            statements.push(Self::upsert_meta(&[(WATERMARK_KEY, watermark.as_slice())]));
            statements
        };

        {
            let mut write = self.write.lock().await;
            let tx = write.transaction().await?;
            for q in statements {
                query::execute_prepared(&tx, q).await?;
            }
            tx.commit().await?;
        }

        // Only now purge the staged (unflushed) commands below the bound, or a
        // later flush would resurrect rows this truncation just deleted. After
        // the commit, not before: a truncation that fails leaves them for the
        // next flush, as nothing below the bound was deleted.
        self.staged
            .lock()
            .expect("journal poisoned")
            .retain(|(key, _)| key.as_slice() >= bound.as_slice());
        // Decision records outlive their commands.
        self.staged_meta
            .lock()
            .expect("journal poisoned")
            .retain(|(txn, key, _)| txn.0 >= before || key.starts_with(DECISION_PREFIX));
        Ok(())
    }

    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        match self.load_meta(WATERMARK_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::Watermark, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) -> anyhow::Result<()> {
        if bounds.is_empty() {
            let q = query::prepare(
                &Query::delete()
                    .from_table(AccordMeta::Table)
                    .and_where(Expr::col(AccordMeta::K).eq(REJOIN_BOUNDS_KEY))
                    .to_owned(),
            );
            query::execute_prepared(&*self.write.lock().await, q).await?;
            return Ok(());
        }
        let value = encode_tagged(RecordKind::RejoinBounds, &bounds.to_vec())?;
        self.save_meta(REJOIN_BOUNDS_KEY, &value).await
    }

    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, Timestamp)>> {
        match self.load_meta(REJOIN_BOUNDS_KEY).await? {
            Some(bytes) => Ok(decode_tagged(RecordKind::RejoinBounds, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::ClockBound, &micros)?;
        self.save_meta(CLOCK_BOUND_KEY, &value).await
    }

    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        match self.load_meta(CLOCK_BOUND_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::ClockBound, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_born(&self, micros: u64) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Born, &micros)?;
        self.save_meta(BORN_KEY, &value).await
    }

    async fn load_born(&self) -> anyhow::Result<Option<u64>> {
        match self.load_meta(BORN_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::Born, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_vote_fence(&self, fence: Timestamp) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::VoteFence, &fence)?;
        self.stage_kept(VOTE_FENCE_KEY, value);
        Ok(())
    }

    async fn load_vote_fence(&self) -> anyhow::Result<Option<Timestamp>> {
        match self.load_meta(VOTE_FENCE_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::VoteFence, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_peer_marks(&self, marks: &[(NodeId, DurableMark)]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::PeerMarks, &marks.to_vec())?;
        self.stage_kept(PEER_MARKS_KEY, value);
        Ok(())
    }

    async fn load_peer_marks(&self) -> anyhow::Result<Vec<(NodeId, DurableMark)>> {
        match self.load_meta(PEER_MARKS_KEY).await? {
            Some(bytes) => Ok(decode_tagged(RecordKind::PeerMarks, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_copy_state(&self, state: &CopyState) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::CopyState, state)?;
        self.stage_kept(COPY_STATE_KEY, value);
        Ok(())
    }

    async fn load_copy_state(&self) -> anyhow::Result<Option<CopyState>> {
        match self.load_meta(COPY_STATE_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::CopyState, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_slot_gc(&self, epoch: u64) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::SlotGc, &epoch)?;
        self.stage_kept(SLOT_GC_KEY, value);
        Ok(())
    }

    async fn load_slot_gc(&self) -> anyhow::Result<Option<u64>> {
        match self.load_meta(SLOT_GC_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::SlotGc, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_amnesia_fence(&self, fence: &AmnesiaFence) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::AmnesiaFence, fence)?;
        self.save_meta(AMNESIA_FENCE_KEY, &value).await
    }

    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<AmnesiaFence>> {
        match self.load_meta(AMNESIA_FENCE_KEY).await? {
            Some(bytes) => Ok(Some(decode_tagged(RecordKind::AmnesiaFence, &bytes)?)),
            None => Ok(None),
        }
    }

    async fn save_fences(&self, fences: &[FenceRecord]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Fences, &fences.to_vec())?;
        self.save_meta(FENCES_KEY, &value).await
    }

    async fn load_fences(&self) -> anyhow::Result<Vec<FenceRecord>> {
        match self.load_meta(FENCES_KEY).await? {
            Some(bytes) => Ok(decode_tagged(RecordKind::Fences, &bytes)?),
            None => Ok(Vec::new()),
        }
    }

    async fn save_route(&self, txn: TxnId, route: &[Key]) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Route, &(txn, route.to_vec()))?;
        self.staged_meta.lock().expect("journal poisoned").push((
            txn,
            meta_txn_key(ROUTE_PREFIX, txn),
            value,
        ));
        Ok(())
    }

    async fn load_routes(&self) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        self.load_meta_prefixed(ROUTE_PREFIX)
            .await?
            .iter()
            .map(|bytes| decode_tagged(RecordKind::Route, bytes))
            .collect()
    }

    async fn save_promise(&self, txn: TxnId, ballot: Ballot) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Promise, &(txn, ballot))?;
        self.staged_meta.lock().expect("journal poisoned").push((
            txn,
            meta_txn_key(PROMISE_PREFIX, txn),
            value,
        ));
        Ok(())
    }

    async fn load_promises(&self) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        self.load_meta_prefixed(PROMISE_PREFIX)
            .await?
            .iter()
            .map(|bytes| decode_tagged(RecordKind::Promise, bytes))
            .collect()
    }

    async fn save_decision(&self, record: &DecisionRecord) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::Decision, record)?;
        self.staged_meta.lock().expect("journal poisoned").push((
            record.txn,
            meta_txn_key(DECISION_PREFIX, record.txn),
            value,
        ));
        Ok(())
    }

    async fn load_decisions(&self) -> anyhow::Result<Vec<DecisionRecord>> {
        self.load_meta_prefixed(DECISION_PREFIX)
            .await?
            .iter()
            .map(|bytes| decode_tagged(RecordKind::Decision, bytes))
            .collect()
    }

    async fn drop_decisions(&self, txns: &[TxnId]) -> anyhow::Result<()> {
        let keys: Vec<String> = txns
            .iter()
            .map(|txn| meta_txn_key(DECISION_PREFIX, *txn))
            .collect();
        {
            let dropped: std::collections::HashSet<&str> =
                keys.iter().map(String::as_str).collect();
            self.staged_meta
                .lock()
                .expect("journal poisoned")
                .retain(|(_, key, _)| !dropped.contains(key.as_str()));
        }
        // A flush already under way may still write one back: it is then
        // only kept longer, and dropped again after a restart.
        let statements: Vec<Prepared> = keys
            .chunks(ROWS_PER_STATEMENT)
            .map(|chunk| {
                query::prepare(
                    &Query::delete()
                        .from_table(AccordMeta::Table)
                        .and_where(Expr::col(AccordMeta::K).is_in(chunk.iter().cloned()))
                        .to_owned(),
                )
            })
            .collect();
        let write = self.write.lock().await;
        for q in statements {
            query::execute_prepared(&write, q).await?;
        }
        Ok(())
    }

    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        let q = query::prepare(
            &Query::select()
                .column(AccordCommands::Data)
                .from(AccordCommands::Table)
                .and_where(Expr::col(AccordCommands::Txn).eq(txn_key(txn)))
                .to_owned(),
        );
        match query::fetch_optional_prepared(&self.reader()?, q).await? {
            Some(row) => Ok(Some(decode_tagged(
                RecordKind::Command,
                &query::blob(&row, 0)?,
            )?)),
            None => Ok(None),
        }
    }

    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        let q = query::prepare(
            &Query::select()
                .column(AccordCommands::Data)
                .from(AccordCommands::Table)
                .to_owned(),
        );
        query::fetch_rows_only(&self.reader()?, q)
            .await?
            .iter()
            .map(|row| decode_tagged(RecordKind::Command, &query::blob(row, 0)?))
            .collect()
    }

    async fn append_metadata(&self, epoch: u64, entry: &ConfigEntry) -> anyhow::Result<()> {
        self.append_metadata_batch(&[(epoch, entry.clone())]).await
    }

    async fn append_metadata_batch(&self, entries: &[(u64, ConfigEntry)]) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // Idempotent: the first decided entry for an epoch wins. Entries
        // arrive as distinct ascending epochs; dedupe defensively anyway
        // (first wins) so a malformed batch cannot break the statement.
        let q = {
            let mut seen = std::collections::HashSet::new();
            let mut statement = Query::insert()
                .into_table(AccordMetadataLog::Table)
                .columns([AccordMetadataLog::Epoch, AccordMetadataLog::Layout])
                .on_conflict(
                    OnConflict::column(AccordMetadataLog::Epoch)
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned();
            for (epoch, entry) in entries {
                if !seen.insert(*epoch) {
                    continue;
                }
                let value = encode_tagged(RecordKind::MetadataEntry, entry)?;
                statement.values_panic([(*epoch as i64).into(), value.into()]);
            }
            query::prepare(&statement)
        };
        query::execute_prepared(&*self.write.lock().await, q).await?;
        Ok(())
    }

    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, ConfigEntry)>> {
        self.load_epochs(
            AccordMetadataLog::Table,
            AccordMetadataLog::Epoch,
            AccordMetadataLog::Layout,
        )
        .await?
        .iter()
        .map(|(epoch, bytes)| Ok((*epoch, decode_tagged(RecordKind::MetadataEntry, bytes)?)))
        .collect()
    }

    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        let value = encode_tagged(RecordKind::AcceptorState, state)?;
        let q = query::prepare(
            &Query::insert()
                .into_table(AccordAcceptors::Table)
                .columns([AccordAcceptors::Epoch, AccordAcceptors::State])
                .values_panic([(epoch as i64).into(), value.into()])
                .on_conflict(
                    OnConflict::column(AccordAcceptors::Epoch)
                        .update_column(AccordAcceptors::State)
                        .to_owned(),
                )
                .to_owned(),
        );
        query::execute_prepared(&*self.write.lock().await, q).await?;
        Ok(())
    }

    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
        self.load_epochs(
            AccordAcceptors::Table,
            AccordAcceptors::Epoch,
            AccordAcceptors::State,
        )
        .await?
        .iter()
        .map(|(epoch, bytes)| Ok((*epoch, decode_tagged(RecordKind::AcceptorState, bytes)?)))
        .collect()
    }
}
