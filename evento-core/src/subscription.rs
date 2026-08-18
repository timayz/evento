//! Continuous event subscriptions.
//!
//! This module provides infrastructure for processing events continuously
//! in the background with retry logic, routing key filtering, and graceful
//! shutdown support.
//!
//! # Key Types
//!
//! - [`SubscriptionBuilder`] - Builds and configures event subscriptions
//! - [`Subscription`] - Handle to a running subscription
//! - [`Handler`] - Trait for event handlers
//! - [`Context`] - Handler context with executor access
//! - [`RoutingKey`] - Filter for event routing
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::subscription::SubscriptionBuilder;
//!
//! // Build a subscription with handlers
//! let subscription = SubscriptionBuilder::new("my-subscription")
//!     .handler(account_opened_handler)
//!     .handler(money_deposited_handler)
//!     .routing_key("accounts")
//!     .chunk_size(100)
//!     .retry(5)
//!     .start(&executor)
//!     .await?;
//!
//! // Later, gracefully shutdown
//! subscription.shutdown().await?;
//! ```

use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{interval_at, Instant};
use tracing::field::Empty;
use ulid::Ulid;

use crate::{
    context,
    cursor::{Args, Value},
    Aggregate, AggregateEvent, EventFilter, Executor,
};

/// Bounds on the adaptive retry delay after a `Gated` pass. The lower bound
/// keeps a nearly-stable event from spinning the loop; the upper bound keeps
/// the subscription responsive while the watermark advances.
const GATED_RETRY_MIN: Duration = Duration::from_millis(5);
const GATED_RETRY_MAX: Duration = Duration::from_millis(250);

/// How long a cached `latest_timestamp` sample may feed the lag metric before
/// being refreshed. The metric has whole-second resolution, so a ≤1s-old
/// sample is as good as a fresh MAX() scan per acknowledge.
const LATEST_TS_TTL: Duration = Duration::from_secs(1);

/// Filter for events by routing key.
///
/// Routing keys allow partitioning events for parallel processing
/// or filtering subscriptions to specific event streams.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RoutingKey {
    /// Match all events regardless of routing key
    All,
    /// Match events with a specific routing key (or no key if `None`)
    Value(Option<String>),
}

/// Handler context providing access to executor and shared data.
///
/// `Context` wraps an [`RwContext`](crate::context::RwContext) for type-safe
/// data storage and provides access to the executor for database operations.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::subscription]
/// async fn my_handler<E: Executor>(
///     context: &Context<'_, E>,
///     event: Event<MyEventData>,
/// ) -> anyhow::Result<()> {
///     // Access shared data
///     let config: Data<AppConfig> = context.extract();
///
///     // Use executor for queries
///     let events = context.executor.read(...).await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Context<'a, E: Executor> {
    context: context::RwContext,
    /// Reference to the executor for database operations
    pub executor: &'a E,
}

impl<'a, E: Executor> Deref for Context<'a, E> {
    type Target = context::RwContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

/// Trait for event handlers.
///
/// Handlers process events in two modes:
/// - `handle`: For subscriptions that perform side effects (send emails, update read models)
/// - `apply`: For loading aggregate state by replaying events
///
/// This trait is typically implemented via the `#[evento::handler]` macro.
pub trait Handler<E: Executor>: Sync + Send {
    /// Handles an event during subscription processing.
    ///
    /// This is called when processing events in a subscription context,
    /// where side effects like database updates or API calls are appropriate.
    fn handle<'a>(
        &'a self,
        context: &'a Context<'a, E>,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    /// Returns the aggregate type this handler processes.
    fn aggregate_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
}

/// Builder for creating event subscriptions.
///
/// Created via [`Projection::subscription`](crate::projection::Projection::subscription), this builder configures
/// a continuous event processing subscription with retry logic,
/// routing key filtering, and graceful shutdown support.
///
/// # Example
///
/// ```rust,ignore
/// let subscription = projection
///     .subscription()
///     .routing_key("accounts")
///     .chunk_size(100)
///     .retry(5)
///     .delay(Duration::from_secs(10))
///     .start(&executor)
///     .await?;
///
/// // Later, gracefully shutdown
/// subscription.shutdown().await?;
/// ```
pub struct SubscriptionBuilder<E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<E>>>,
    context: context::RwContext,
    routing_key: Option<RoutingKey>,
    prefix_key: Option<String>,
    /// The storage key with its routing/tenant prefix applied — computed once
    /// in `resolve_routing_key` so the hot paths never re-format it.
    resolved_key: String,
    delay: Option<Duration>,
    poll_interval: Duration,
    chunk_size: u16,
    continue_on_error: bool,
    retry: Option<u8>,
    aggregators: HashMap<String, String>,
    safety_disabled: bool,
    shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
    ack_every: Option<u16>,
    /// Cached `(sampled at, latest event timestamp in seconds)` feeding the
    /// lag metric, so acknowledges don't pay a MAX() scan each.
    latest_ts_cache: Mutex<Option<(Instant, u64)>>,
}

/// What a single `process` pass concluded, beyond a hard error.
enum ProcessOutcome {
    /// Everything currently available was processed (or nothing was pending).
    Drained,
    /// Remaining events sit at/above the stability watermark; retry after
    /// roughly `wait`, by which time the watermark will have reached the next
    /// pending event.
    Gated { wait: Duration },
    /// Another worker took over this subscription key; this worker must stop.
    LostOwnership,
    /// A shutdown signal was observed mid-chunk.
    ShutdownRequested,
}

impl<E: Executor + 'static> SubscriptionBuilder<E> {
    /// Creates a new subscription builder with the given key.
    ///
    /// The key is used as the subscription identifier for cursor tracking.
    pub fn new(key: impl Into<String>) -> Self {
        let key = key.into();
        Self {
            resolved_key: key.clone(),
            key,
            handlers: HashMap::new(),
            safety_disabled: true,
            context: Default::default(),
            delay: None,
            poll_interval: Duration::from_millis(250),
            retry: Some(30),
            chunk_size: 300,
            continue_on_error: false,
            routing_key: None,
            prefix_key: None,
            aggregators: Default::default(),
            shutdown_rx: None,
            ack_every: None,
            latest_ts_cache: Mutex::new(None),
        }
    }

    /// Enables safety checks for unhandled events.
    ///
    /// When enabled, processing fails if an event is encountered without a handler.
    pub fn strict(mut self) -> Self {
        self.safety_disabled = false;

        self
    }

    /// Registers an event handler with this subscription.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<E> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregate_type(), h.event_name());
        match self.handlers.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                panic!(
                    "Cannot register event handler: key {} already exists",
                    entry.key()
                );
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(h));
            }
        }
        self
    }

    /// Registers a skip handler for an event type.
    ///
    /// Events of this type will be acknowledged but not processed.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn skip<EV: AggregateEvent + Send + Sync + 'static>(self) -> Self {
        self.handler(SkipHandler::<EV>(PhantomData))
    }

    /// Adds shared data to the subscription context.
    ///
    /// Data added here is accessible in handlers via the context.
    pub fn data<D: Send + Sync + 'static>(self, v: D) -> Self {
        self.context.insert(v);

        self
    }

    /// Allows the subscription to continue after handler failures.
    ///
    /// By default, subscriptions stop on the first error. With this flag,
    /// errors are logged but processing continues.
    pub fn continue_on_error(mut self) -> Self {
        self.continue_on_error = true;

        self
    }

    /// Sets the number of events to process per batch.
    ///
    /// Default is 300. Values below 1 are clamped to 1 — a chunk size of 0
    /// would make the subscription silently read nothing forever.
    pub fn chunk_size(mut self, v: u16) -> Self {
        self.chunk_size = v.max(1);

        self
    }

    /// Sets how many events may be processed between cursor acknowledges.
    ///
    /// The cursor is flushed to the store every `v` events and at the end of
    /// every chunk, instead of once per event. Delivery stays at-least-once
    /// (handlers must already be idempotent); the trade-off is the fencing
    /// bound — a worker that has lost ownership stops within `v` events plus
    /// the per-pass ownership check, and after a crash up to `v` events are
    /// redelivered. Defaults to the chunk size. Values below 1 are clamped
    /// to 1 (ack per event, the pre-batching behavior).
    pub fn ack_every(mut self, v: u16) -> Self {
        self.ack_every = Some(v.max(1));

        self
    }

    /// Sets a delay before starting the subscription.
    ///
    /// Useful for staggering subscription starts in multi-node deployments.
    pub fn delay(mut self, v: Duration) -> Self {
        self.delay = Some(v);

        self
    }

    /// Sets the polling interval used as a fallback when no write signal is
    /// available.
    ///
    /// When the executor supports [`write_watch`](crate::Executor::write_watch)
    /// (the default backends do), an in-process write wakes the subscription
    /// immediately and this interval only bounds latency for writes the signal
    /// cannot observe — cross-process writers or custom executors. Backlogs are
    /// drained at full speed regardless of this value. Default is 250ms.
    pub fn poll_interval(mut self, v: Duration) -> Self {
        self.poll_interval = v;

        self
    }

    /// Filters events by routing key.
    ///
    /// Only events with the matching routing key will be processed.
    /// Overrides any executor-level default.
    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = Some(RoutingKey::Value(Some(v.into())));

        self
    }

    /// Sets the maximum number of retries on failure.
    ///
    /// Uses exponential backoff. Default is 30.
    pub fn retry(mut self, v: u8) -> Self {
        self.retry = Some(v);

        self
    }

    /// Processes all events regardless of routing key.
    ///
    /// Overrides any executor-level default.
    pub fn all(mut self) -> Self {
        self.routing_key = Some(RoutingKey::All);

        self
    }

    /// Adds a related aggregate to process events from.
    pub fn aggregate<A: Aggregate>(mut self, id: impl Into<String>) -> Self {
        self.aggregators
            .insert(A::aggregate_type().to_owned(), id.into());

        self
    }

    /// Builds the read filters once per worker: handlers frequently collapse
    /// to identical filters (e.g. `by_type` once per handler), so they are
    /// deduplicated here, and the result is shared as an `Arc` — every poll
    /// clones the handle, not the filters.
    fn read_aggregators(&self) -> Arc<[EventFilter]> {
        let mut seen = std::collections::HashSet::new();
        self.handlers
            .values()
            .map(|h| {
                // `#[subscription_all]` handlers report the sentinel name "all" and
                // match every event of the type, so they must read by type rather
                // than by a literal event name (which would match nothing).
                let by_name = self.safety_disabled && h.event_name() != "all";
                match self.aggregators.get(h.aggregate_type()) {
                    Some(id) => EventFilter {
                        aggregate_type: h.aggregate_type().to_owned(),
                        aggregate_id: Some(id.to_owned()),
                        name: by_name.then(|| h.event_name().to_owned()),
                    },
                    _ => {
                        if by_name {
                            EventFilter::by_event(h.aggregate_type(), h.event_name())
                        } else {
                            EventFilter::by_type(h.aggregate_type())
                        }
                    }
                }
            })
            .filter(|filter| seen.insert(filter.clone()))
            .collect()
    }

    fn resolved_key(&self) -> &str {
        &self.resolved_key
    }

    /// Resolves an unset routing key from the executor's default and captures
    /// the default as a storage-key prefix.
    ///
    /// Called once at the top of `start()` / `run_once()` before any other
    /// method reads `self.routing_key`. After this, `routing_key` is always
    /// `Some(_)`.
    ///
    /// `prefix_key` is captured from `executor.default_routing_key()` even
    /// when the user has already called `.all()`, so the storage key remains
    /// scoped to the executor's tenant — otherwise two executors with
    /// different defaults would share one row in the subscriber table.
    fn resolve_routing_key(&mut self, executor: &E) {
        if self.prefix_key.is_none() {
            self.prefix_key = executor.default_routing_key().map(|s| s.to_owned());
        }
        if self.routing_key.is_none() {
            self.routing_key = Some(match executor.default_routing_key() {
                Some(k) => RoutingKey::Value(Some(k.to_owned())),
                None => RoutingKey::Value(None),
            });
        }
        let prefix = match &self.routing_key {
            Some(RoutingKey::Value(Some(k))) => Some(k.as_str()),
            Some(RoutingKey::All) => self.prefix_key.as_deref(),
            _ => None,
        };
        self.resolved_key = match prefix {
            Some(p) => format!("{p}.{}", self.key),
            None => self.key.clone(),
        };
    }

    fn effective_routing_key(&self) -> RoutingKey {
        self.routing_key.clone().unwrap_or(RoutingKey::Value(None))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            subscription = Empty,
            aggregate_type = Empty,
            aggregate_id = Empty,
            event = Empty,
        )
    )]
    async fn process(
        &self,
        executor: &E,
        id: &Ulid,
        aggregators: &Arc<[EventFilter]>,
    ) -> anyhow::Result<ProcessOutcome> {
        // Drains all currently-available events back-to-back and returns as soon
        // as it catches up. The caller (`start`'s loop) owns all waiting — poll
        // interval, write signal, and shutdown — so there is no pacing here: this
        // keeps both fresh-event latency and backlog drain at full speed.
        tracing::Span::current().record("subscription", self.resolved_key());

        let ack_every = usize::from(self.ack_every.unwrap_or(self.chunk_size).max(1));

        loop {
            let status = executor
                .subscriber_status(self.resolved_key().to_owned(), *id)
                .await?;
            if !status.running {
                return Ok(ProcessOutcome::LostOwnership);
            }
            let cursor = status.cursor;

            // Stability watermark (microseconds since epoch): on a backend where
            // independent writers can commit out of cursor order, the
            // subscription must not advance past it, or a late lower-cursor event
            // would be skipped. `None` (single-writer backends) means no gating.
            // Fetched *before* the read so the read is bounded by it and a gated
            // pass fetches nothing instead of a chunk it would discard.
            let stable = executor.stable_timestamp().await?;

            let res = executor
                .read(
                    Some(aggregators.clone()),
                    Some(self.effective_routing_key()),
                    Args::forward(self.chunk_size, cursor.clone()),
                    stable,
                )
                .await?;

            // A partial chunk means everything below the watermark has been
            // read; a full chunk likely has more pending, so loop and read the
            // next one immediately.
            let full_chunk = res.edges.len() >= self.chunk_size as usize;

            if res.edges.is_empty() {
                return self
                    .drained_or_gated(executor, aggregators, cursor, stable)
                    .await;
            }

            let context = Context {
                context: self.context.clone(),
                executor,
            };

            // Cursor + timestamp of the last processed-but-unacknowledged
            // event; flushed every `ack_every` events and on every exit path,
            // so cursor persistence costs one round trip per batch instead of
            // one per event.
            let mut pending_ack: Option<(Value, u64)> = None;
            let mut since_ack = 0usize;
            let mut last_seen = cursor;

            for event in res.edges {
                // Defensive backstop: the read was already bounded by the
                // watermark, so this only fires for a backend that ignores
                // `read`'s `to_micros` bound. Edges arrive in ascending cursor
                // order, so everything from here on is gated too.
                if let Some(w) = stable {
                    let event_micros = (event.node.timestamp)
                        .saturating_mul(1_000_000)
                        .saturating_add(event.node.timestamp_subsec as u64 * 1_000);
                    if event_micros >= w {
                        if !self
                            .flush_ack(executor, id, aggregators, &mut pending_ack, &mut since_ack)
                            .await?
                        {
                            return Ok(ProcessOutcome::LostOwnership);
                        }
                        let wait = Duration::from_micros(event_micros - w)
                            .clamp(GATED_RETRY_MIN, GATED_RETRY_MAX);
                        return Ok(ProcessOutcome::Gated { wait });
                    }
                }

                if self
                    .shutdown_rx
                    .as_ref()
                    .is_some_and(|rx| *rx.borrow() || rx.has_changed().is_err())
                {
                    tracing::info!(
                        key = self.resolved_key(),
                        "Subscription received shutdown signal, stopping gracefully"
                    );

                    self.flush_ack(executor, id, aggregators, &mut pending_ack, &mut since_ack)
                        .await?;
                    return Ok(ProcessOutcome::ShutdownRequested);
                }

                tracing::Span::current().record("aggregate_type", &event.node.aggregate_type);
                tracing::Span::current().record("aggregate_id", &event.node.aggregate_id);
                tracing::Span::current().record("event", &event.node.name);

                // A specific handler takes precedence over a `subscription_all`
                // catch-all for the same aggregate type; the catch-all key is
                // only built on a miss.
                let key = format!("{}_{}", event.node.aggregate_type, event.node.name);
                let handler = match self.handlers.get(&key).or_else(|| {
                    self.handlers
                        .get(&format!("{}_all", event.node.aggregate_type))
                }) {
                    Some(handler) => Some(handler),
                    None if !self.safety_disabled && !self.continue_on_error => {
                        self.flush_ack(executor, id, aggregators, &mut pending_ack, &mut since_ack)
                            .await?;
                        anyhow::bail!("no handler s={} k={key}", self.resolved_key())
                    }
                    None if !self.safety_disabled => {
                        // Strict mode with continue_on_error: skip the poison
                        // event (acknowledged below) instead of re-reading and
                        // re-failing the same batch forever.
                        tracing::error!(key = key, "no handler, skipping event");
                        None
                    }
                    None => None,
                };

                if let Some(handler) = handler {
                    if let Err(err) = handler.handle(&context, &event.node).await {
                        if !self.continue_on_error {
                            tracing::error!("failed");
                            // Persist the successfully processed prefix before
                            // surfacing the error, so a retry resumes at the
                            // failing event instead of re-running the chunk.
                            self.flush_ack(
                                executor,
                                id,
                                aggregators,
                                &mut pending_ack,
                                &mut since_ack,
                            )
                            .await?;
                            return Err(err);
                        }
                        // continue_on_error: log and acknowledge so the
                        // subscription makes progress past the failing event
                        // rather than retrying it forever.
                        tracing::error!(error = %err, "failed, skipping event");
                    } else {
                        tracing::debug!("completed");
                    }
                }

                last_seen = Some(event.cursor.clone());
                pending_ack = Some((event.cursor, event.node.timestamp));
                since_ack += 1;
                if since_ack >= ack_every
                    && !self
                        .flush_ack(executor, id, aggregators, &mut pending_ack, &mut since_ack)
                        .await?
                {
                    // Another worker took over mid-chunk; stop so we do not
                    // process events the new owner will also handle.
                    return Ok(ProcessOutcome::LostOwnership);
                }
            }

            if !self
                .flush_ack(executor, id, aggregators, &mut pending_ack, &mut since_ack)
                .await?
            {
                return Ok(ProcessOutcome::LostOwnership);
            }

            if !full_chunk {
                return self
                    .drained_or_gated(executor, aggregators, last_seen, stable)
                    .await;
            }
        }
    }

    /// Flushes the pending cursor via a fenced acknowledge. Returns `false`
    /// when ownership was lost (the fenced update did not apply); `true` when
    /// nothing was pending or the ack succeeded.
    async fn flush_ack(
        &self,
        executor: &E,
        id: &Ulid,
        aggregators: &Arc<[EventFilter]>,
        pending: &mut Option<(Value, u64)>,
        since_ack: &mut usize,
    ) -> anyhow::Result<bool> {
        let Some((cursor, event_ts)) = pending.take() else {
            return Ok(true);
        };
        *since_ack = 0;

        let latest = self.cached_latest_timestamp(executor, aggregators).await?;
        executor
            .acknowledge(
                self.resolved_key().to_owned(),
                *id,
                cursor,
                latest.saturating_sub(event_ts),
            )
            .await
    }

    /// The latest matching event timestamp (whole seconds), refreshed at most
    /// once per [`LATEST_TS_TTL`]. Feeds the lag metric only, so a slightly
    /// stale sample is fine and saves a MAX() scan per acknowledge.
    async fn cached_latest_timestamp(
        &self,
        executor: &E,
        aggregators: &Arc<[EventFilter]>,
    ) -> anyhow::Result<u64> {
        {
            let guard = self.latest_ts_cache.lock().expect("latest_ts poisoned");
            if let Some((at, v)) = *guard {
                if at.elapsed() < LATEST_TS_TTL {
                    return Ok(v);
                }
            }
        }

        let v = executor
            .latest_timestamp(
                Some(aggregators.clone()),
                Some(self.effective_routing_key()),
            )
            .await?;
        *self.latest_ts_cache.lock().expect("latest_ts poisoned") = Some((Instant::now(), v));
        Ok(v)
    }

    /// A bounded read returned less than a full chunk: distinguish "nothing
    /// further exists" from "the next event sits at/above the watermark" with
    /// a single-row unbounded probe, and derive how long until that event
    /// becomes stable (the watermark advances in real time).
    async fn drained_or_gated(
        &self,
        executor: &E,
        aggregators: &Arc<[EventFilter]>,
        after: Option<Value>,
        stable: Option<u64>,
    ) -> anyhow::Result<ProcessOutcome> {
        let Some(stable) = stable else {
            // No watermark means the read was unbounded: a non-full chunk is a
            // genuine drain.
            return Ok(ProcessOutcome::Drained);
        };

        let probe = executor
            .read(
                Some(aggregators.clone()),
                Some(self.effective_routing_key()),
                Args::forward(1, after),
                None,
            )
            .await?;
        let Some(edge) = probe.edges.first() else {
            return Ok(ProcessOutcome::Drained);
        };

        let event_micros = (edge.node.timestamp)
            .saturating_mul(1_000_000)
            .saturating_add(edge.node.timestamp_subsec as u64 * 1_000);
        let wait = Duration::from_micros(event_micros.saturating_sub(stable))
            .clamp(GATED_RETRY_MIN, GATED_RETRY_MAX);
        Ok(ProcessOutcome::Gated { wait })
    }

    /// Disables retry-on-failure for this subscription.
    ///
    /// By default failed batches are retried with exponential backoff (see
    /// [`retry`](Self::retry)). Combine this with [`start`](Self::start) or
    /// [`run_once`](Self::run_once) to process without retries.
    pub fn no_retry(mut self) -> Self {
        self.retry = None;

        self
    }

    /// Starts a continuous background subscription.
    ///
    /// Returns a [`Subscription`] handle that can be used for graceful shutdown.
    /// The subscription runs in a spawned tokio task and polls for new events.
    #[tracing::instrument(skip_all, fields(
        subscription = tracing::field::Empty,
        aggregate_type = tracing::field::Empty,
        aggregate_id = tracing::field::Empty,
        event = tracing::field::Empty,
    ))]
    pub async fn start(mut self, executor: &E) -> anyhow::Result<Subscription>
    where
        E: Clone,
    {
        self.resolve_routing_key(executor);
        tracing::Span::current().record("subscription", self.resolved_key());
        let executor = executor.clone();
        let id = Ulid::generate();
        let subscription_id = id;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_rx = Some(shutdown_rx.clone());
        let mut shutdown_rx = shutdown_rx;

        executor
            .upsert_subscriber(self.resolved_key().to_owned(), id.to_owned())
            .await?;

        let mut write_watch = executor.write_watch();

        let task_handle = tokio::spawn(async move {
            let read_aggregators = self.read_aggregators();
            let start = self
                .delay
                .map(|d| Instant::now() + d)
                .unwrap_or_else(Instant::now);

            // First tick fires at `start` (immediately when no delay is set), so
            // an existing backlog is processed right away. Thereafter this paces
            // the fallback re-poll; the write signal (when available) wakes the
            // loop sooner.
            let mut interval = interval_at(start, self.poll_interval);
            let mut gated: Option<Duration> = None;

            loop {
                // Wake on whichever comes first: an in-process write (when the
                // executor exposes a signal), the fallback poll tick, or a
                // shutdown signal. Selecting on shutdown here keeps shutdown
                // immediate even with a long poll interval. `changed()` only
                // resolves for write generations newer than the last seen, so it
                // never busy-loops. A closed shutdown channel (handle dropped
                // without calling `shutdown`) also stops the worker.
                //
                // Watermark-gated events become processable as the watermark
                // advances on its own — no write signal will fire for them — so
                // after a `Gated` pass, re-check on a short cadence instead of
                // waiting out the full poll interval.
                let mut shutdown = false;
                if let Some(wait) = gated {
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = shutdown_rx.changed() => { shutdown = true; }
                    }
                } else {
                    match write_watch.as_mut() {
                        Some(rx) => tokio::select! {
                            _ = interval.tick() => {}
                            res = rx.changed() => {
                                if res.is_ok() {
                                    rx.borrow_and_update();
                                }
                            }
                            _ = shutdown_rx.changed() => { shutdown = true; }
                        },
                        None => tokio::select! {
                            _ = interval.tick() => {}
                            _ = shutdown_rx.changed() => { shutdown = true; }
                        },
                    }
                }

                if shutdown {
                    tracing::info!(
                        key = self.resolved_key(),
                        "Subscription received shutdown signal, stopping gracefully"
                    );

                    break;
                }

                // The retry backoff can span minutes; selecting the shutdown
                // signal against it keeps `Subscription::shutdown` responsive
                // even while a failing pass is backing off.
                let process_fut = async {
                    match self.retry {
                        Some(retry) => {
                            (|| async { self.process(&executor, &id, &read_aggregators).await })
                                .retry(
                                    ExponentialBuilder::default()
                                        .with_jitter()
                                        .with_max_times(retry.into()),
                                )
                                .sleep(tokio::time::sleep)
                                .notify(|err, dur| {
                                    tracing::error!(
                                        error = %err,
                                        duration = ?dur,
                                        "Failed to process event"
                                    );
                                })
                                .await
                        }
                        _ => self.process(&executor, &id, &read_aggregators).await,
                    }
                };
                tokio::pin!(process_fut);
                let result = tokio::select! {
                    res = &mut process_fut => Some(res),
                    _ = shutdown_rx.changed() => None,
                };
                let Some(result) = result else {
                    tracing::info!(
                        key = self.resolved_key(),
                        "Subscription received shutdown signal, stopping gracefully"
                    );
                    break;
                };

                match result {
                    Ok(ProcessOutcome::Drained) => gated = None,
                    Ok(ProcessOutcome::Gated { wait }) => gated = Some(wait),
                    Ok(ProcessOutcome::ShutdownRequested) => break,
                    Ok(ProcessOutcome::LostOwnership) => {
                        tracing::info!(
                            key = self.resolved_key(),
                            "Subscription taken over by another worker, stopping"
                        );
                        break;
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to process event");

                        if !self.continue_on_error {
                            break;
                        }
                    }
                };
            }
        });

        Ok(Subscription {
            id: subscription_id,
            task_handle,
            shutdown_tx,
        })
    }

    /// Processes all currently pending events once, then returns.
    ///
    /// Unlike [`start`](Self::start), this does not run continuously or spawn a
    /// background task — it drains the events available now and returns. On a
    /// backend with a stability watermark it waits (briefly, in 25ms steps) for
    /// the watermark to pass the events that were pending at entry, so a
    /// completed `run_once` really has processed everything that was committed
    /// before it was called. Pair with [`no_retry`](Self::no_retry) to run a
    /// single pass without retries.
    #[tracing::instrument(skip_all, fields(
        subscription = tracing::field::Empty,
        aggregate_type = tracing::field::Empty,
        aggregate_id = tracing::field::Empty,
        event = tracing::field::Empty,
    ))]
    pub async fn run_once(&mut self, executor: &E) -> anyhow::Result<()> {
        self.resolve_routing_key(executor);
        tracing::Span::current().record("subscription", self.resolved_key());
        let id = Ulid::generate();

        executor
            .upsert_subscriber(self.resolved_key().to_owned(), id.to_owned())
            .await?;

        let read_aggregators = self.read_aggregators();

        // Exclusive upper bound (µs) covering every event committed before
        // entry: `latest_timestamp` has whole-second resolution, so cover the
        // entire latest second.
        let target_micros = executor
            .latest_timestamp(
                Some(read_aggregators.clone()),
                Some(self.effective_routing_key()),
            )
            .await?
            .saturating_add(1)
            .saturating_mul(1_000_000);

        // Set once the watermark has passed `target_micros`; one further pass
        // is still required (a `Gated` outcome may rest on a watermark fetched
        // before it advanced), after which anything still gated is post-entry.
        let mut watermark_passed = false;

        loop {
            let outcome = match self.retry {
                Some(retry) => {
                    (|| async { self.process(executor, &id, &read_aggregators).await })
                        .retry(
                            ExponentialBuilder::default()
                                .with_jitter()
                                .with_max_times(retry.into()),
                        )
                        .sleep(tokio::time::sleep)
                        .notify(|err, dur| {
                            tracing::error!(
                                error = %err,
                                duration = ?dur,
                                "Failed to process event"
                            );
                        })
                        .await
                }
                _ => self.process(executor, &id, &read_aggregators).await,
            }?;

            match outcome {
                ProcessOutcome::Drained | ProcessOutcome::ShutdownRequested => return Ok(()),
                ProcessOutcome::LostOwnership => {
                    anyhow::bail!(
                        "subscription {} was taken over by another worker during run_once",
                        self.resolved_key()
                    )
                }
                ProcessOutcome::Gated { wait } => {
                    // Wait for the watermark to pass everything that was
                    // pending at entry, then run one final pass: this `Gated`
                    // may rest on a watermark `process` fetched before it
                    // advanced, so returning immediately could strand
                    // pre-entry events between the stale watermark and the
                    // target. After that pass, anything still gated has a
                    // timestamp at/above the target — it arrived later and is
                    // out of scope for this pass.
                    match executor.stable_timestamp().await? {
                        Some(w) if w < target_micros => {
                            tokio::time::sleep(wait).await;
                        }
                        _ if watermark_passed => return Ok(()),
                        _ => watermark_passed = true,
                    }
                }
            }
        }
    }
}

/// Handle to a running event subscription.
///
/// Returned by [`SubscriptionBuilder::start`], this handle provides
/// the subscription ID and a method for graceful shutdown.
///
/// # Example
///
/// ```rust,ignore
/// let subscription = projection
///     .subscription()
///     .start(&executor)
///     .await?;
///
/// println!("Started subscription: {}", subscription.id);
///
/// // On application shutdown
/// subscription.shutdown().await?;
/// ```
#[derive(Debug)]
pub struct Subscription {
    /// Unique ID for this subscription instance
    pub id: Ulid,
    task_handle: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl Subscription {
    /// Gracefully shuts down the subscription.
    ///
    /// Signals the subscription to stop and waits for it to finish
    /// processing the current event before returning. The signal also
    /// interrupts a retry backoff in progress, so shutdown stays prompt even
    /// while the subscription is failing.
    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.shutdown_tx.send(true);

        self.task_handle.await
    }
}

struct SkipHandler<E: AggregateEvent>(PhantomData<E>);

impl<E: Executor, EV: AggregateEvent + Send + Sync> Handler<E> for SkipHandler<EV> {
    fn handle<'a>(
        &'a self,
        _context: &'a Context<'a, E>,
        _event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn aggregate_type(&self) -> &'static str {
        EV::aggregate_type()
    }

    fn event_name(&self) -> &'static str {
        EV::event_name()
    }
}
