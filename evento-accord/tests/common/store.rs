//! A data store whose operations fail, by knob or with a seeded probability.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use evento_accord::{DataStore, InMemoryDataStore, SnapshotPage, Timestamp, TxnId};
use evento_core::cursor::{Args, ReadResult, Value};
use evento_core::{Event, EventFilter, RoutingKey};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// A store over another whose applies, version reads and queries can be made
/// to fail. A failed apply fails before reaching the backend, so nothing is
/// ever half applied.
pub struct FaultStore {
    inner: Arc<dyn DataStore>,
    /// The in-memory backend, when that is what `inner` is (see [`applied`](Self::applied)).
    memory: Option<Arc<InMemoryDataStore>>,
    pub apply_failing: AtomicBool,
    /// Fails this many applies, then works again.
    pub applies_to_fail: AtomicUsize,
    pub version_failing: AtomicBool,
    /// Fails every query (`DataStore::read`), as a forwarded read runs one.
    pub read_failing: AtomicBool,
    /// Seeded apply failures: the generator and the probability.
    faults: Mutex<Option<(StdRng, f64)>>,
}

impl Default for FaultStore {
    /// Over a fresh [`InMemoryDataStore`].
    fn default() -> Self {
        let memory = Arc::new(InMemoryDataStore::new());
        Self {
            memory: Some(Arc::clone(&memory)),
            ..Self::over(memory)
        }
    }
}

impl FaultStore {
    /// Over `inner`.
    pub fn over(inner: Arc<dyn DataStore>) -> Self {
        Self {
            inner,
            memory: None,
            apply_failing: AtomicBool::new(false),
            applies_to_fail: AtomicUsize::new(0),
            version_failing: AtomicBool::new(false),
            read_failing: AtomicBool::new(false),
            faults: Mutex::new(None),
        }
    }

    /// The transactions applied to the in-memory backend, in order.
    pub fn applied(&self) -> Vec<TxnId> {
        self.memory
            .as_ref()
            .expect("an in-memory backend")
            .applied_log()
            .iter()
            .map(|e| e.txn)
            .collect()
    }

    /// Fails each apply with probability `p`, drawn from `seed`.
    pub fn set_apply_faults(&self, seed: u64, p: f64) {
        *self.faults.lock().unwrap() = Some((StdRng::seed_from_u64(seed), p));
    }

    /// Stops the seeded apply failures.
    pub fn clear_faults(&self) {
        *self.faults.lock().unwrap() = None;
    }

    fn seeded_failure(&self) -> bool {
        let mut faults = self.faults.lock().unwrap();
        match faults.as_mut() {
            Some((rng, p)) => rng.random_bool(p.clamp(0.0, 1.0)),
            None => false,
        }
    }
}

#[async_trait]
impl DataStore for FaultStore {
    async fn version(&self, aggregate_type: &str, aggregate_id: &str) -> anyhow::Result<u16> {
        if self.version_failing.load(Ordering::SeqCst) {
            anyhow::bail!("simulated read failure");
        }
        self.inner.version(aggregate_type, aggregate_id).await
    }

    async fn apply(
        &self,
        txn: TxnId,
        execute_at: Timestamp,
        events: Vec<Event>,
        commit: bool,
    ) -> anyhow::Result<()> {
        if self.apply_failing.load(Ordering::SeqCst)
            || self
                .applies_to_fail
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            || self.seeded_failure()
        {
            anyhow::bail!("simulated write failure");
        }
        self.inner.apply(txn, execute_at, events, commit).await
    }

    async fn read(
        &self,
        aggregators: Option<Arc<[EventFilter]>>,
        routing_key: Option<RoutingKey>,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        if self.read_failing.load(Ordering::SeqCst) {
            anyhow::bail!("simulated query failure");
        }
        self.inner
            .read(aggregators, routing_key, args, to_micros)
            .await
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<Event>> {
        self.inner.snapshot().await
    }

    async fn snapshot_page(
        &self,
        after: Option<Value>,
        max_events: u16,
        max_bytes: usize,
    ) -> anyhow::Result<SnapshotPage> {
        self.inner.snapshot_page(after, max_events, max_bytes).await
    }
}
