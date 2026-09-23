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
//! - [`StopReason`] - Why a subscription's worker stopped
//! - [`Handler`] - Trait for event handlers
//! - [`Context`] - Handler context with executor access
//! - [`RoutingKey`] - Filter for event routing
//!
//! # Failure handling
//!
//! By default a subscription stops on the first handler error
//! ([`continue_on_error`](SubscriptionBuilder::continue_on_error) is opt-in),
//! and a stopped worker never processes another event. It is easy to miss:
//!
//! - the worker reports itself through `tracing`, which does nothing unless the
//!   application installs a subscriber (`tracing_subscriber::fmt::init()`);
//! - the store-side `running` flag is ownership state, not liveness — it stays
//!   set after the local worker dies.
//!
//! So supervise the handle: [`Subscription::stopped`] resolves with the
//! [`StopReason`], and [`Subscription::is_finished`] is the non-blocking check.
//!
//! # Starting position
//!
//! A subscription resumes from its stored cursor, and a key that has never
//! acknowledged an event starts at the beginning of history. For a live bridge
//! (SSE, a WebSocket, a broadcast fanout) where history is meaningless, opt out
//! with [`start_from_latest`](SubscriptionBuilder::start_from_latest): a
//! brand-new key is seeded at the stream head instead. Once a cursor exists it
//! has no effect, so a restart always resumes rather than jumping forward.
//!
//! # Example
//!
//! ```rust,no_run
//! use evento::metadata::Event;
//! use evento::subscription::{Context, SubscriptionBuilder};
//! use evento::Executor;
//!
//! # #[evento::aggregate]
//! # pub enum Account {
//! #     AccountOpened { owner: String },
//! #     MoneyDeposited { amount: i64 },
//! # }
//! # #[evento::subscription]
//! # async fn account_opened<E: Executor>(
//! #     _ctx: &Context<'_, E>,
//! #     _event: Event<AccountOpened>,
//! # ) -> anyhow::Result<()> {
//! #     Ok(())
//! # }
//! # #[evento::subscription]
//! # async fn money_deposited<E: Executor>(
//! #     _ctx: &Context<'_, E>,
//! #     _event: Event<MoneyDeposited>,
//! # ) -> anyhow::Result<()> {
//! #     Ok(())
//! # }
//! # async fn run<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
//! // Build a subscription with handlers
//! let subscription = SubscriptionBuilder::new("my-subscription")
//!     .handler(account_opened())
//!     .handler(money_deposited())
//!     .routing_key("accounts")
//!     .chunk_size(100)
//!     .retry(5)
//!     .start(executor)
//!     .await?;
//!
//! // Later, gracefully shutdown
//! subscription.shutdown().await?;
//! # Ok(())
//! # }
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
    upcast::Aliases,
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
/// Shared data is registered with [`SubscriptionBuilder::data`] and read back in
/// a handler by the same type it was registered under. `extract` clones the
/// value, so that type must be `Clone` and cheap to clone; wrap anything else in
/// [`Data`](crate::context::Data) and extract it as `Data<T>`.
///
/// # Example
///
/// ```rust,no_run
/// use evento::cursor::Args;
/// use evento::metadata::Event;
/// use evento::subscription::{Context, SubscriptionBuilder};
/// use evento::Executor;
///
/// # #[evento::aggregate]
/// # pub enum Account {
/// #     MoneyDeposited { amount: i64 },
/// # }
/// #[derive(Clone)]
/// struct AppConfig {
///     webhook_url: String,
/// }
///
/// #[evento::subscription]
/// async fn my_handler<E: Executor>(
///     context: &Context<'_, E>,
///     event: Event<MoneyDeposited>,
/// ) -> anyhow::Result<()> {
///     // Access shared data (or `context.try_extract::<AppConfig>()?` to get an
///     // error instead of a panic when it was never registered)
///     let config: AppConfig = context.extract();
///
///     // Use executor for queries
///     let events = context
///         .executor
///         .read(None, None, Args::forward(10, None), None)
///         .await?;
///     Ok(())
/// }
///
/// # async fn run<E: Executor + Clone>(executor: &E, config: AppConfig) -> anyhow::Result<()> {
/// // The other half: whatever a handler extracts must be registered here.
/// let subscription = SubscriptionBuilder::new("deposits")
///     .data(config)
///     .handler(my_handler())
///     .start(executor)
///     .await?;
/// # subscription.shutdown().await?;
/// # Ok(()) }
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

    /// Older stored events this handler also accepts, converted to its event
    /// first (see [`AggregateEvent::upcasters`]). The `#[evento::subscription]`
    /// macro returns its event's list; hand-written handlers default to none.
    fn upcasters(&self) -> &'static [crate::Upcaster] {
        &[]
    }

    /// Returns the aggregate type this handler processes.
    fn aggregate_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
}

/// Builder for creating event subscriptions.
///
/// Created with [`SubscriptionBuilder::new`], this builder configures
/// a continuous event processing subscription with retry logic,
/// routing key filtering, and graceful shutdown support.
///
/// # Example
///
/// ```rust,no_run
/// # use std::time::Duration;
/// # use evento::subscription::SubscriptionBuilder;
/// # async fn run<E: evento::Executor + Clone>(executor: &E) -> anyhow::Result<()> {
/// let subscription = SubscriptionBuilder::new("my-subscription")
///     .routing_key("accounts")
///     .chunk_size(100)
///     .retry(5)
///     .delay(Duration::from_secs(10))
///     .start(executor)
///     .await?;
///
/// // Later, gracefully shutdown
/// subscription.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct SubscriptionBuilder<E: Executor> {
    key: String,
    handlers: HashMap<String, Box<dyn Handler<E>>>,
    /// Older event names routed to the handler of the event they upcast to.
    aliases: Aliases,
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
    /// When set and this key has no stored cursor yet, the cursor is seeded at
    /// the stream's head before the first read, so history is never delivered.
    start_from_latest: bool,
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
            aliases: Aliases::default(),
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
            start_from_latest: false,
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
    /// Older events declared with `#[evento(upcast_to = ...)]` that lead to
    /// the handler's event are routed to it too, converted first — unless a
    /// handler is registered for the older event itself, which then wins.
    /// `#[evento::subscription_all]` handlers receive stored events as they
    /// are, never upcast.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<E> + 'static>(self, h: H) -> Self {
        self.register(h, true)
    }

    /// `convert` is `false` for skips: their payload is never read.
    fn register<H: Handler<E> + 'static>(mut self, h: H, convert: bool) -> Self {
        self.aliases
            .register(h.aggregate_type(), h.event_name(), h.upcasters(), convert);

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
    /// Events of this type — and older events that upcast to it — will be
    /// acknowledged but not processed.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn skip<EV: AggregateEvent + Send + Sync + 'static>(self) -> Self {
        self.register(SkipHandler::<EV>(PhantomData), false)
    }

    /// Adds shared data to the subscription context.
    ///
    /// The value is stored under its own type, and a handler reads it back with
    /// `context.extract::<D>()` (or [`try_extract`](crate::context::RwContext::try_extract)
    /// for a `Result`). Extraction clones the value out of a read lock, so `D`
    /// should be `Clone` and cheap to clone — a pool handle, an `Arc`, a small
    /// config.
    ///
    /// For a type that is not `Clone`, or is expensive to clone, register
    /// `Data::new(value)` and extract it as [`Data<D>`](crate::context::Data).
    /// The registered and extracted types must match exactly.
    ///
    /// ```rust,no_run
    /// # use evento::{subscription::{Context, SubscriptionBuilder}, metadata::Event, Executor};
    /// # #[evento::aggregate]
    /// # pub enum Account { MoneyDeposited { amount: i64 } }
    /// # #[derive(Clone)]
    /// # struct AppConfig { webhook_url: String }
    /// #[evento::subscription]
    /// async fn on_deposit<E: Executor>(
    ///     context: &Context<'_, E>,
    ///     event: Event<MoneyDeposited>,
    /// ) -> anyhow::Result<()> {
    ///     let config: AppConfig = context.extract();
    ///     println!("{} {}", config.webhook_url, event.data.amount);
    ///     Ok(())
    /// }
    ///
    /// # async fn run<E: Executor + Clone>(executor: &E, config: AppConfig) -> anyhow::Result<()> {
    /// let subscription = SubscriptionBuilder::new("deposits")
    ///     .data(config)
    ///     .handler(on_deposit())
    ///     .start(executor)
    ///     .await?;
    /// # subscription.shutdown().await?;
    /// # Ok(()) }
    /// ```
    pub fn data<D: Send + Sync + 'static>(self, v: D) -> Self {
        self.context.insert(v);

        self
    }

    /// Allows the subscription to continue after handler failures.
    ///
    /// By default, subscriptions stop on the first error. With this flag,
    /// errors are logged but processing continues.
    ///
    /// Without it, a failing handler stops the worker for good and the only
    /// signals are a `tracing::error!` (a no-op unless the application installs
    /// a subscriber) and [`Subscription::stopped`], which reports
    /// [`StopReason::Failed`].
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

    /// Starts a brand-new subscription at the stream's head instead of at the
    /// beginning of history.
    ///
    /// This applies **only when the key has no stored cursor yet**: the head is
    /// recorded as the cursor before the first event is read, so history is
    /// never delivered. Once a cursor exists — the first acknowledged event, or
    /// a restart of an existing subscription — this has no effect and the
    /// subscription resumes exactly where it left off, so a restart still picks
    /// up everything committed while the process was down.
    ///
    /// The shape this exists for is a live bridge — SSE, a WebSocket, push
    /// notifications, an in-process broadcast channel — where replaying history
    /// pushes stale updates at clients that only care about what happens from
    /// now on:
    ///
    /// ```rust,no_run
    /// # use evento::{Executor, metadata::Event, subscription::{Context, SubscriptionBuilder}};
    /// # #[evento::aggregate]
    /// # pub enum Account { MoneyDeposited { amount: i64 } }
    /// #[evento::subscription]
    /// async fn fanout<E: Executor>(
    ///     context: &Context<'_, E>,
    ///     event: Event<MoneyDeposited>,
    /// ) -> anyhow::Result<()> {
    ///     let tx: tokio::sync::broadcast::Sender<i64> = context.extract();
    ///     let _ = tx.send(event.data.amount);
    ///     Ok(())
    /// }
    ///
    /// # async fn run<E: Executor + Clone>(
    /// #     executor: &E,
    /// #     tx: tokio::sync::broadcast::Sender<i64>,
    /// # ) -> anyhow::Result<()> {
    /// let subscription = SubscriptionBuilder::new("sse-fanout")
    ///     .handler(fanout())
    ///     .data(tx)
    ///     .start_from_latest()
    ///     .start(executor)
    ///     .await?;
    /// # subscription.shutdown().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Keep the key stable across restarts: that is what makes the resume half
    /// work. A fresh key per process re-seeds at the head every time, which is
    /// harmless for a pure fanout but leaves a subscriber row behind per
    /// process.
    ///
    /// The head is sampled when [`start`](Self::start) or
    /// [`run_once`](Self::run_once) is called, not when the worker first polls,
    /// so events committed while a [`delay`](Self::delay) elapses are still
    /// delivered.
    ///
    /// # This skips history, not "everything before now"
    ///
    /// On a backend with a stability watermark (a shared SQL store, multi-node
    /// Accord) the head is the newest event *below* the watermark, so up to the
    /// backend's stability margin — default 1s for `Sql` — of very recent
    /// events can still be delivered on a first start. That is deliberate:
    /// seeding past the watermark could permanently skip an event committed
    /// moments after the subscription started. On a single-writer backend the
    /// watermark does not exist and the head is exact.
    ///
    /// With the read/write split executor (`Rw`) the cursor is read from the
    /// replica, so a stale replica can report "no cursor" and seed again. The
    /// subscription already trusts the replica for cursor state on every pass,
    /// so this adds no new hazard.
    ///
    /// Deliberately not offered on
    /// [`ProjectionSubscription`](crate::projection::ProjectionSubscription):
    /// a read model started at the head would be missing the state its
    /// handlers exist to fold.
    pub fn start_from_latest(mut self) -> Self {
        self.start_from_latest = true;

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

    /// Processes events with any routing key, instead of filtering by one.
    ///
    /// The counterpart to [`routing_key`](Self::routing_key). Overrides any
    /// executor-level default.
    pub fn any_routing_key(mut self) -> Self {
        self.routing_key = Some(RoutingKey::All);

        self
    }

    /// Sets the maximum number of retries on failure.
    ///
    /// Uses exponential backoff. Default is 30.
    pub fn retry(mut self, v: u8) -> Self {
        self.retry = Some(v);

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
        // Older events that upcast to a handled one are read as well; in
        // strict mode every event of the type already is.
        let upcast_names = self
            .aliases
            .iter()
            .filter(|(key, _)| self.safety_disabled && !self.handlers.contains_key(*key))
            .map(|(_, alias)| (alias.aggregate_type, alias.from));

        self.handlers
            .values()
            .map(|h| (h.aggregate_type(), h.event_name()))
            .chain(upcast_names)
            .map(|(aggregate_type, event_name)| {
                // `#[subscription_all]` handlers report the sentinel name "all" and
                // match every event of the type, so they must read by type rather
                // than by a literal event name (which would match nothing).
                let by_name = self.safety_disabled && event_name != "all";
                match self.aggregators.get(aggregate_type) {
                    Some(id) => EventFilter {
                        aggregate_type: aggregate_type.to_owned(),
                        aggregate_id: Some(id.to_owned()),
                        name: by_name.then(|| event_name.to_owned()),
                    },
                    _ => {
                        if by_name {
                            EventFilter::by_event_raw(aggregate_type, event_name)
                        } else {
                            EventFilter::by_type_raw(aggregate_type)
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
    /// when the user has already called `.any_routing_key()`, so the storage key
    /// remains scoped to the executor's tenant — otherwise two executors with
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

                // A specific handler takes precedence over an upcast to a newer
                // event's handler (so consumers migrate one at a time), which
                // takes precedence over a `subscription_all` catch-all for the
                // same aggregate type; the catch-all key is only built on a miss.
                let key = format!("{}_{}", event.node.aggregate_type, event.node.name);
                let alias = match self.handlers.contains_key(&key) {
                    true => None,
                    false => self.aliases.get(&key),
                };
                let handler = match self
                    .handlers
                    .get(alias.map_or(&key, |alias| &alias.target_key))
                    .or_else(|| {
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
                    // A failed upcast is handled like a failed handler.
                    let result = match alias.map(|alias| alias.apply(&event.node)) {
                        Some(Ok(upcast)) => handler.handle(&context, &upcast).await,
                        Some(Err(err)) => Err(err),
                        None => handler.handle(&context, &event.node).await,
                    };
                    if let Err(err) = result {
                        if !self.continue_on_error {
                            tracing::error!(error = %err, key = self.resolved_key(), "failed");
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

    /// Seeds the cursor at the stream's head when
    /// [`start_from_latest`](Self::start_from_latest) is set and this key has no
    /// stored cursor yet. A no-op otherwise.
    ///
    /// Must be called *after* `upsert_subscriber`: the fence claimed there is
    /// what makes check-then-seed safe. A worker that loses the fence in between
    /// fails the fenced acknowledge, seeds nothing, and stops on its first
    /// `process` pass with [`StopReason::LostOwnership`] — so it can never
    /// replay the history it was told to skip.
    async fn seed_latest_cursor(
        &self,
        executor: &E,
        id: &Ulid,
        aggregators: &Arc<[EventFilter]>,
    ) -> anyhow::Result<()> {
        if !self.start_from_latest {
            return Ok(());
        }

        // One round trip for both facts: a stored cursor means "resume", which
        // `start_from_latest` never overrides, and a lost fence means there is
        // nothing to seed for.
        let status = executor
            .subscriber_status(self.resolved_key().to_owned(), *id)
            .await?;
        if !status.running || status.cursor.is_some() {
            return Ok(());
        }

        // Bounded by the stability watermark, exactly like the reads in
        // `process`: the seeded cursor is then a position the normal read path
        // could itself have reached right now, so it inherits the watermark's
        // no-skip guarantee instead of introducing a new one. Seeding to the
        // *unbounded* head would put the cursor beyond where `process` is
        // allowed to go, and any event committed just after start but stamped
        // below the head would be skipped for good.
        let stable = executor.stable_timestamp().await?;
        // Same filters and routing key the worker will read with, so `.strict()`,
        // `.aggregate::<A>(id)` and `.routing_key(..)` all scope the head to
        // *this* subscription's stream. A global head could sit above a matching
        // event and skip it.
        let head = executor
            .read(
                Some(aggregators.clone()),
                Some(self.effective_routing_key()),
                Args::backward(1, None),
                stable,
            )
            .await?;

        // Nothing matches yet: cursor zero already skips nothing, so leave it
        // unset and let the first real acknowledge write it.
        let Some(edge) = head.edges.first() else {
            return Ok(());
        };

        // Through `flush_ack` so the lag metric is computed the same way as for
        // every other acknowledge.
        let mut pending = Some((edge.cursor.clone(), edge.node.timestamp));
        let mut since_ack = 0usize;
        if self
            .flush_ack(executor, id, aggregators, &mut pending, &mut since_ack)
            .await?
        {
            tracing::info!(
                key = self.resolved_key(),
                "Subscription seeded at the stream head, history skipped"
            );
        } else {
            // Another worker claimed the key between the status read and the
            // acknowledge. It seeds (or resumes) on its own; this worker stops
            // on its first pass.
            tracing::debug!(
                key = self.resolved_key(),
                "Lost ownership before the head cursor could be stored"
            );
        }

        Ok(())
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
    ///
    /// The worker can also stop on its own — a handler error without
    /// [`continue_on_error`](Self::continue_on_error), or another worker taking
    /// over the key — and the returned handle is the only way to find out:
    /// see [`Subscription::stopped`] and [`StopReason`].
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
        // Records why the worker stopped. The sender is owned by the spawned
        // task below and dropped with it, so a closed channel that never
        // received a value means the task died without recording one —
        // a panic, an abort, or a runtime shutdown.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(None::<StopReason>);

        executor
            .upsert_subscriber(self.resolved_key().to_owned(), id.to_owned())
            .await?;

        // Hoisted above the spawn so `start_from_latest` can seed the cursor
        // while the fence claimed just above is still held, and before the
        // worker can read anything. Seeding here rather than inside the task
        // also makes a failing probe a `start()` error instead of a worker that
        // dies later, and makes "after `start()` returns, history will not be
        // replayed" an observable guarantee.
        let read_aggregators = self.read_aggregators();
        self.seed_latest_cursor(&executor, &id, &read_aggregators)
            .await?;

        let mut write_watch = executor.write_watch();

        let task_handle = tokio::spawn(async move {
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

            // Every exit from this loop names a `StopReason`, so the worker can
            // never die without saying why: adding a new terminal path is a
            // compile error until it does.
            let reason = loop {
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
                    break StopReason::Shutdown;
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
                    break StopReason::Shutdown;
                };

                match result {
                    Ok(ProcessOutcome::Drained) => gated = None,
                    Ok(ProcessOutcome::Gated { wait }) => gated = Some(wait),
                    Ok(ProcessOutcome::ShutdownRequested) => break StopReason::Shutdown,
                    Ok(ProcessOutcome::LostOwnership) => break StopReason::LostOwnership,
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to process event");

                        if !self.continue_on_error {
                            break StopReason::Failed(Arc::new(err));
                        }
                    }
                };
            };

            // The one record that says the worker is gone. Without it a stopped
            // subscription is invisible: the per-pass error above does not say
            // whether processing continues, and nothing else ever fires again.
            match &reason {
                StopReason::Failed(err) => tracing::error!(
                    key = self.resolved_key(),
                    error = %err,
                    "Subscription stopped: a pass failed and continue_on_error is not set"
                ),
                reason => tracing::info!(
                    key = self.resolved_key(),
                    reason = %reason,
                    "Subscription stopped"
                ),
            }

            let _ = stop_tx.send(Some(reason));
        });

        Ok(Subscription {
            id: subscription_id,
            task_handle,
            shutdown_tx,
            stop_rx,
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

        // Under the fence claimed just above, and before the first read.
        self.seed_latest_cursor(executor, &id, &read_aggregators)
            .await?;

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

/// Why a [`Subscription`]'s worker stopped.
///
/// The worker records this as it exits. Read it without blocking through
/// [`Subscription::stop_reason`], or wait for it with
/// [`Subscription::stopped`].
///
/// [`Failed`](Self::Failed) and [`Panicked`](Self::Panicked) are the abnormal
/// ones — the others are the worker doing what it was told.
#[derive(Clone, Debug)]
pub enum StopReason {
    /// [`Subscription::shutdown`] or [`Subscription::stop`] was called, or the
    /// handle was dropped (which also signals the worker).
    Shutdown,
    /// Another worker claimed this subscription key, so this one stepped aside.
    LostOwnership,
    /// A pass failed after exhausting [`retry`](SubscriptionBuilder::retry),
    /// and the subscription was not built with
    /// [`continue_on_error`](SubscriptionBuilder::continue_on_error).
    Failed(Arc<anyhow::Error>),
    /// The worker ended without recording a reason: it panicked, was aborted,
    /// or the runtime shut down under it.
    Panicked,
}

impl StopReason {
    /// The error that stopped the subscription, if it stopped because of one.
    pub fn error(&self) -> Option<&anyhow::Error> {
        match self {
            StopReason::Failed(err) => Some(err),
            _ => None,
        }
    }

    /// Whether the subscription stopped abnormally instead of on request.
    pub fn is_failure(&self) -> bool {
        matches!(self, StopReason::Failed(_) | StopReason::Panicked)
    }
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StopReason::Shutdown => f.write_str("shutdown requested"),
            StopReason::LostOwnership => f.write_str("taken over by another worker"),
            // `{:#}` keeps the whole `anyhow` context chain on one line —
            // without it a `%reason` log drops everything a handler added.
            StopReason::Failed(err) => write!(f, "failed: {err:#}"),
            StopReason::Panicked => f.write_str("worker task panicked or was aborted"),
        }
    }
}

/// Handle to a running event subscription.
///
/// Returned by [`SubscriptionBuilder::start`], this handle carries the
/// subscription ID, graceful shutdown, and — because a worker can also stop on
/// its own — the reason it stopped.
///
/// # Example
///
/// ```rust,no_run
/// # use evento::subscription::SubscriptionBuilder;
/// # async fn run<E: evento::Executor + Clone>(executor: &E) -> anyhow::Result<()> {
/// let subscription = SubscriptionBuilder::new("my-subscription")
///     .start(executor)
///     .await?;
///
/// println!("Started subscription: {}", subscription.id);
///
/// // Supervise it: a failing handler stops the worker for good, and this
/// // handle is the only way to find out.
/// tokio::select! {
///     _ = tokio::signal::ctrl_c() => {}
///     reason = subscription.stopped() => {
///         tracing::error!(%reason, "subscription stopped on its own");
///     }
/// }
///
/// subscription.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Subscription {
    /// Unique ID for this subscription instance
    pub id: Ulid,
    task_handle: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    stop_rx: tokio::sync::watch::Receiver<Option<StopReason>>,
}

impl Subscription {
    /// Gracefully shuts down the subscription.
    ///
    /// Signals the subscription to stop and waits for it to finish
    /// processing the current event before returning. The signal also
    /// interrupts a retry backoff in progress, so shutdown stays prompt even
    /// while the subscription is failing.
    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.stop();

        self.task_handle.await
    }

    /// Signals the worker to stop, without waiting for it.
    ///
    /// Unlike [`shutdown`](Self::shutdown) this takes `&self`, so a supervisor
    /// holding an `Arc<Subscription>` can stop it. Pair it with
    /// [`stopped`](Self::stopped) to wait for the worker to actually exit.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Whether the worker has stopped — `true` means no further events will be
    /// processed, whatever the reason.
    ///
    /// Equivalent to `self.stop_reason().is_some()`. The task itself may still
    /// be unwinding; [`shutdown`](Self::shutdown) joins it.
    pub fn is_finished(&self) -> bool {
        self.stop_reason().is_some()
    }

    /// Why the worker stopped, or `None` while it is still running.
    ///
    /// A non-blocking snapshot; [`stopped`](Self::stopped) waits for it.
    pub fn stop_reason(&self) -> Option<StopReason> {
        peek_stop_reason(&self.stop_rx)
    }

    /// Waits until the worker stops, and reports why.
    ///
    /// Resolves immediately if it has already stopped. Cancel-safe: the reason
    /// lives in a watch channel, so this can be dropped inside a
    /// [`tokio::select!`] and awaited again later without losing it.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use evento::subscription::{StopReason, SubscriptionBuilder};
    /// # async fn run<E: evento::Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    /// # let subscription = SubscriptionBuilder::new("my-subscription").start(executor).await?;
    /// match subscription.stopped().await {
    ///     StopReason::Failed(err) => tracing::error!(%err, "restarting"),
    ///     reason => tracing::info!(%reason, "worker stopped"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stopped(&self) -> StopReason {
        // `wait_for` needs `&mut`; cloning the receiver is an `Arc` bump and
        // keeps this `&self`, so an `Arc<Subscription>` supervisor can await it.
        await_stop_reason(self.stop_rx.clone()).await
    }
}

/// Reads a recorded stop reason without blocking.
///
/// Shared with the unit tests, which exercise it without a live worker.
fn peek_stop_reason(rx: &tokio::sync::watch::Receiver<Option<StopReason>>) -> Option<StopReason> {
    // Sample "closed" *before* the value: if the channel is already closed then
    // any send happened before the sender dropped, so the borrow below sees it.
    // The other order can read a stale `None`, then observe the close, and
    // report `Panicked` for a worker that did record a reason.
    let closed = rx.has_changed().is_err();
    let recorded = rx.borrow().as_ref().cloned();

    match recorded {
        Some(reason) => Some(reason),
        // Closed with nothing recorded: the task ended without setting one.
        None if closed => Some(StopReason::Panicked),
        None => None,
    }
}

/// Waits for a stop reason to be recorded.
///
/// Shared with the unit tests, which exercise it without a live worker.
async fn await_stop_reason(mut rx: tokio::sync::watch::Receiver<Option<StopReason>>) -> StopReason {
    // `wait_for` re-checks the current value after observing a close, so a
    // reason sent just before the sender dropped still comes back as `Ok`.
    match rx.wait_for(|reason| reason.is_some()).await {
        Ok(reason) => reason.as_ref().cloned().unwrap_or(StopReason::Panicked),
        // All senders dropped without a reason: the task panicked or was aborted.
        Err(_) => StopReason::Panicked,
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

    fn upcasters(&self) -> &'static [crate::Upcaster] {
        EV::upcasters()
    }
}

#[cfg(test)]
mod tests {
    use super::{await_stop_reason, peek_stop_reason, StopReason};
    use std::sync::Arc;

    fn channel() -> (
        tokio::sync::watch::Sender<Option<StopReason>>,
        tokio::sync::watch::Receiver<Option<StopReason>>,
    ) {
        tokio::sync::watch::channel(None)
    }

    #[test]
    fn peek_is_none_while_the_worker_runs() {
        let (_tx, rx) = channel();

        assert!(peek_stop_reason(&rx).is_none());
    }

    /// A reason recorded before the worker task ended must survive the sender
    /// being dropped — reading the value before the closed flag would report a
    /// spurious `Panicked` here.
    #[test]
    fn peek_prefers_a_recorded_reason_over_the_closed_channel() {
        let (tx, rx) = channel();
        tx.send(Some(StopReason::Failed(Arc::new(anyhow::anyhow!("boom")))))
            .unwrap();
        drop(tx);

        let reason = peek_stop_reason(&rx).expect("a reason was recorded");
        assert!(matches!(reason, StopReason::Failed(_)), "{reason}");
    }

    #[test]
    fn peek_reports_panicked_when_nothing_was_recorded() {
        let (tx, rx) = channel();
        drop(tx);

        assert!(matches!(peek_stop_reason(&rx), Some(StopReason::Panicked)));
    }

    #[tokio::test]
    async fn await_resolves_with_a_reason_recorded_earlier() {
        let (tx, rx) = channel();
        tx.send(Some(StopReason::LostOwnership)).unwrap();

        assert!(matches!(
            await_stop_reason(rx).await,
            StopReason::LostOwnership
        ));
    }

    #[tokio::test]
    async fn await_resolves_when_the_worker_records_later() {
        let (tx, rx) = channel();
        let waiting = tokio::spawn(await_stop_reason(rx));

        tokio::task::yield_now().await;
        tx.send(Some(StopReason::Shutdown)).unwrap();

        assert!(matches!(waiting.await.unwrap(), StopReason::Shutdown));
    }

    #[tokio::test]
    async fn await_reports_panicked_when_the_sender_drops_empty() {
        let (tx, rx) = channel();
        let waiting = tokio::spawn(await_stop_reason(rx));

        tokio::task::yield_now().await;
        drop(tx);

        assert!(matches!(waiting.await.unwrap(), StopReason::Panicked));
    }

    /// `Display` must flatten the whole `anyhow` context chain: a `%reason` log
    /// that only carried the outermost message would lose the diagnosis.
    #[test]
    fn display_keeps_the_error_context_chain() {
        let err = anyhow::anyhow!("boom").context("while handling MoneyDeposited");
        let reason = StopReason::Failed(Arc::new(err));

        let rendered = reason.to_string();
        assert!(
            rendered.contains("while handling MoneyDeposited"),
            "{rendered}"
        );
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn only_abnormal_reasons_are_failures() {
        assert!(!StopReason::Shutdown.is_failure());
        assert!(!StopReason::LostOwnership.is_failure());
        assert!(StopReason::Panicked.is_failure());

        let failed = StopReason::Failed(Arc::new(anyhow::anyhow!("boom")));
        assert!(failed.is_failure());
        assert!(failed.error().is_some());
        assert!(StopReason::Shutdown.error().is_none());
    }
}
