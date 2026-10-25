//! A journal that models what a crash does to unflushed writes, and storage
//! that fails.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use evento_accord::{
    AcceptorRecord, AmnesiaFence, Ballot, CommandState, ConfigEntry, DecisionRecord, FenceRecord,
    InMemoryJournal, Journal, Key, NodeId, Timestamp, TxnId,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// One write staged in a [`FaultJournal`] and not yet durable.
#[derive(Clone)]
enum Staged {
    Command(CommandState),
    Route(TxnId, Vec<Key>),
    Promise(TxnId, Ballot),
    Decision(DecisionRecord),
    DropDecisions(Vec<TxnId>),
}

impl Staged {
    /// Whether [`truncate`](Journal::truncate) below `before` drops it, as a
    /// durable journal drops its commands, routes and promises (decision
    /// records outlive truncation).
    fn below(&self, before: Timestamp) -> bool {
        match self {
            Staged::Command(state) => state.txn.0 < before,
            Staged::Route(txn, _) | Staged::Promise(txn, _) => txn.0 < before,
            Staged::Decision(_) | Staged::DropDecisions(_) => false,
        }
    }
}

/// Probabilities that a journal write fails, drawn per call from a seeded
/// generator (see [`FaultJournal::set_storage_faults`]).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JournalFaults {
    /// A `flush` (fsync) fails: nothing staged becomes durable.
    pub flush: f64,
    /// A `truncate` fails, leaving the journal untouched.
    pub truncate: f64,
    /// A direct durable write fails: metadata log, acceptor state, clock
    /// bound, rejoin bounds.
    pub metadata: f64,
}

/// A journal over an [`InMemoryJournal`] that keeps staged writes **pending**
/// until a `flush` succeeds — exactly what a crash would lose
/// ([`crash`](Self::crash) drops them) — and whose writes can fail, by knob
/// or with a seeded probability.
///
/// A `flush` returns only once everything staged before it is durable: like
/// the SQL journal's `flush_lock`, a second flusher waits for one in
/// progress (with an optional virtual fsync delay, so they do overlap).
/// Everything `InMemoryJournal` persists is staged the same way: commands,
/// routes, promises and decision records.
pub struct FaultJournal {
    durable: InMemoryJournal,
    pending: Mutex<Vec<Staged>>,
    flush_lock: tokio::sync::Mutex<()>,
    /// How long a flush's fsync takes, in virtual time.
    fsync_delay: Mutex<Duration>,
    /// Seeded write failures, if set.
    faults: Mutex<Option<(StdRng, JournalFaults)>>,
    failing: AtomicBool,
    /// `truncate` calls, successful or not.
    truncates: AtomicUsize,
    /// While set, `truncate` fails without touching the durable journal.
    truncate_failing: AtomicBool,
    /// While set, every config-acceptor record fails.
    acceptor_failing: AtomicBool,
    /// While set, a config-acceptor record holding an accepted layout fails.
    accept_failing: AtomicBool,
    /// While set, appending to the metadata log fails.
    metadata_failing: AtomicBool,
    /// While set, persisting the clock bound fails.
    clock_bound_failing: AtomicBool,
}

impl Default for FaultJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl FaultJournal {
    pub fn new() -> Self {
        Self {
            durable: InMemoryJournal::new(),
            pending: Mutex::new(Vec::new()),
            flush_lock: tokio::sync::Mutex::new(()),
            fsync_delay: Mutex::new(Duration::ZERO),
            faults: Mutex::new(None),
            failing: AtomicBool::new(false),
            truncates: AtomicUsize::new(0),
            truncate_failing: AtomicBool::new(false),
            acceptor_failing: AtomicBool::new(false),
            accept_failing: AtomicBool::new(false),
            metadata_failing: AtomicBool::new(false),
            clock_bound_failing: AtomicBool::new(false),
        }
    }

    pub fn set_failing(&self, v: bool) {
        self.failing.store(v, Ordering::SeqCst);
    }

    pub fn set_truncate_failing(&self, v: bool) {
        self.truncate_failing.store(v, Ordering::SeqCst);
    }

    pub fn set_acceptor_failing(&self, v: bool) {
        self.acceptor_failing.store(v, Ordering::SeqCst);
    }

    pub fn set_accept_failing(&self, v: bool) {
        self.accept_failing.store(v, Ordering::SeqCst);
    }

    pub fn set_metadata_failing(&self, v: bool) {
        self.metadata_failing.store(v, Ordering::SeqCst);
    }

    pub fn set_clock_bound_failing(&self, v: bool) {
        self.clock_bound_failing.store(v, Ordering::SeqCst);
    }

    /// Makes every flush take `delay` of (virtual) time, as an fsync does,
    /// so concurrent flushers overlap.
    pub fn set_fsync_delay(&self, delay: Duration) {
        *self.fsync_delay.lock().unwrap() = delay;
    }

    /// Fails writes with the probabilities in `faults`, drawn from `seed`.
    pub fn set_storage_faults(&self, seed: u64, faults: JournalFaults) {
        *self.faults.lock().unwrap() = Some((StdRng::seed_from_u64(seed), faults));
    }

    /// Stops the seeded write failures.
    pub fn clear_storage_faults(&self) {
        *self.faults.lock().unwrap() = None;
    }

    /// A crash: whatever was staged and not flushed is gone.
    pub fn crash(&self) {
        self.pending.lock().unwrap().clear();
    }

    pub fn truncates(&self) -> usize {
        self.truncates.load(Ordering::SeqCst)
    }

    /// Whether `txn` is **durably** recorded (survived a flush) — the real
    /// post-crash state, ignoring any non-durable pending writes.
    pub async fn durable_has(&self, txn: TxnId) -> bool {
        self.durable.load(txn).await.unwrap().is_some()
    }

    /// Whether nothing at all is durable here — what a restart over this
    /// journal finds after a crash: the node never made anything durable
    /// (it never voted, reported or minted), or this is a lost journal's
    /// replacement.
    pub async fn durable_is_pristine(&self) -> bool {
        let d = &self.durable;
        d.load_all().await.unwrap().is_empty()
            && d.load_watermark().await.unwrap().is_none()
            && d.load_clock_bound().await.unwrap().is_none()
            && d.load_amnesia_fence().await.unwrap().is_none()
            && d.load_acceptors().await.unwrap().is_empty()
            && d.load_metadata().await.unwrap().is_empty()
            && d.load_fences().await.unwrap().is_empty()
            && d.load_routes().await.unwrap().is_empty()
            && d.load_promises().await.unwrap().is_empty()
            && d.load_decisions().await.unwrap().is_empty()
            && d.load_rejoin_bounds().await.unwrap().is_empty()
    }

    /// Whether a seeded fault fails this write, `pick` choosing its rate.
    fn draw(&self, pick: impl Fn(&JournalFaults) -> f64) -> bool {
        let mut faults = self.faults.lock().unwrap();
        match faults.as_mut() {
            Some((rng, faults)) => rng.random_bool(pick(faults).clamp(0.0, 1.0)),
            None => false,
        }
    }

    fn stage_write(&self, staged: Staged) {
        self.pending.lock().unwrap().push(staged);
    }

    async fn persist(&self, staged: Staged) -> anyhow::Result<()> {
        match staged {
            Staged::Command(state) => self.durable.record(&state).await,
            Staged::Route(txn, keys) => self.durable.save_route(txn, &keys).await,
            Staged::Promise(txn, ballot) => self.durable.save_promise(txn, ballot).await,
            Staged::Decision(record) => self.durable.save_decision(&record).await,
            Staged::DropDecisions(txns) => self.durable.drop_decisions(&txns).await,
        }
    }
}

#[async_trait]
impl Journal for FaultJournal {
    async fn record(&self, state: &CommandState) -> anyhow::Result<()> {
        self.stage(state).await?;
        self.flush().await
    }
    async fn stage(&self, state: &CommandState) -> anyhow::Result<()> {
        self.stage_write(Staged::Command(state.clone()));
        Ok(())
    }
    async fn flush(&self) -> anyhow::Result<()> {
        // A flusher arriving during another's fsync waits for it: it returns
        // only once everything staged before it is durable.
        let _guard = self.flush_lock.lock().await;
        // Nothing staged, nothing to fsync: as a real journal, an empty flush
        // returns at once, and never fails.
        if self.pending.lock().unwrap().is_empty() {
            return Ok(());
        }
        if self.failing.load(Ordering::SeqCst) || self.draw(|f| f.flush) {
            anyhow::bail!("simulated fsync failure");
        }
        let batch = std::mem::take(&mut *self.pending.lock().unwrap());
        let delay = *self.fsync_delay.lock().unwrap();
        if !delay.is_zero() {
            // A crash meanwhile (the node's tasks aborted) drops this future,
            // and the batch with it: it never became durable.
            tokio::time::sleep(delay).await;
        }
        for staged in batch {
            self.persist(staged).await?;
        }
        Ok(())
    }
    async fn load(&self, txn: TxnId) -> anyhow::Result<Option<CommandState>> {
        self.durable.load(txn).await
    }
    async fn load_all(&self) -> anyhow::Result<Vec<CommandState>> {
        self.durable.load_all().await
    }
    async fn truncate(&self, before: Timestamp) -> anyhow::Result<()> {
        self.truncates.fetch_add(1, Ordering::SeqCst);
        if self.truncate_failing.load(Ordering::SeqCst) || self.draw(|f| f.truncate) {
            anyhow::bail!("simulated truncate failure");
        }
        self.pending
            .lock()
            .unwrap()
            .retain(|staged| !staged.below(before));
        self.durable.truncate(before).await
    }
    async fn load_watermark(&self) -> anyhow::Result<Option<Timestamp>> {
        self.durable.load_watermark().await
    }
    async fn save_rejoin_bounds(&self, bounds: &[(NodeId, Timestamp)]) -> anyhow::Result<()> {
        if self.draw(|f| f.metadata) {
            anyhow::bail!("simulated rejoin-bounds write failure");
        }
        self.durable.save_rejoin_bounds(bounds).await
    }
    async fn load_rejoin_bounds(&self) -> anyhow::Result<Vec<(NodeId, Timestamp)>> {
        self.durable.load_rejoin_bounds().await
    }
    async fn save_route(&self, txn: TxnId, keys: &[Key]) -> anyhow::Result<()> {
        self.stage_write(Staged::Route(txn, keys.to_vec()));
        Ok(())
    }
    async fn load_routes(&self) -> anyhow::Result<Vec<(TxnId, Vec<Key>)>> {
        self.durable.load_routes().await
    }
    async fn save_promise(&self, txn: TxnId, ballot: Ballot) -> anyhow::Result<()> {
        self.stage_write(Staged::Promise(txn, ballot));
        Ok(())
    }
    async fn load_promises(&self) -> anyhow::Result<Vec<(TxnId, Ballot)>> {
        self.durable.load_promises().await
    }
    async fn save_decision(&self, record: &DecisionRecord) -> anyhow::Result<()> {
        self.stage_write(Staged::Decision(record.clone()));
        Ok(())
    }
    async fn load_decisions(&self) -> anyhow::Result<Vec<DecisionRecord>> {
        self.durable.load_decisions().await
    }
    async fn drop_decisions(&self, txns: &[TxnId]) -> anyhow::Result<()> {
        self.stage_write(Staged::DropDecisions(txns.to_vec()));
        Ok(())
    }
    async fn append_metadata(&self, epoch: u64, entry: &ConfigEntry) -> anyhow::Result<()> {
        if self.metadata_failing.load(Ordering::SeqCst) || self.draw(|f| f.metadata) {
            anyhow::bail!("simulated metadata-log write failure");
        }
        self.durable.append_metadata(epoch, entry).await
    }
    async fn load_metadata(&self) -> anyhow::Result<Vec<(u64, ConfigEntry)>> {
        self.durable.load_metadata().await
    }
    async fn save_fences(&self, fences: &[FenceRecord]) -> anyhow::Result<()> {
        if self.draw(|f| f.metadata) {
            anyhow::bail!("simulated fence write failure");
        }
        self.durable.save_fences(fences).await
    }
    async fn load_fences(&self) -> anyhow::Result<Vec<FenceRecord>> {
        self.durable.load_fences().await
    }
    async fn record_acceptor(&self, epoch: u64, state: &AcceptorRecord) -> anyhow::Result<()> {
        if self.acceptor_failing.load(Ordering::SeqCst)
            || (state.accepted.is_some() && self.accept_failing.load(Ordering::SeqCst))
            || self.draw(|f| f.metadata)
        {
            anyhow::bail!("simulated acceptor-state write failure");
        }
        self.durable.record_acceptor(epoch, state).await
    }
    async fn load_acceptors(&self) -> anyhow::Result<Vec<(u64, AcceptorRecord)>> {
        self.durable.load_acceptors().await
    }
    async fn save_clock_bound(&self, micros: u64) -> anyhow::Result<()> {
        if self.clock_bound_failing.load(Ordering::SeqCst) || self.draw(|f| f.metadata) {
            anyhow::bail!("simulated clock-bound write failure");
        }
        self.durable.save_clock_bound(micros).await
    }
    async fn load_clock_bound(&self) -> anyhow::Result<Option<u64>> {
        self.durable.load_clock_bound().await
    }
    async fn save_amnesia_fence(&self, fence: &AmnesiaFence) -> anyhow::Result<()> {
        if self.draw(|f| f.metadata) {
            anyhow::bail!("simulated amnesia-fence write failure");
        }
        self.durable.save_amnesia_fence(fence).await
    }
    async fn load_amnesia_fence(&self) -> anyhow::Result<Option<AmnesiaFence>> {
        self.durable.load_amnesia_fence().await
    }
}
