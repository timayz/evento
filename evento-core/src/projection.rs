//! Projections and event subscriptions.
//!
//! This module provides the core building blocks for event sourcing:
//! - Projections that build read models from events
//! - Subscriptions that continuously process events
//! - Loading aggregate state from event streams
//!
//! # Key Types
//!
//! - [`Projection`] - Defines handlers for building projections
//! - [`LoadBuilder`] - Loads aggregate state from events
//! - [`SubscriptionBuilder`] - Builds continuous event subscriptions
//! - [`Subscription`] - Handle to a running subscription
//! - [`EventData`] - Typed event with deserialized data and metadata
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::projection::Projection;
//!
//! // Define a projection with event handlers
//! let projection = Projection::<AccountView, _>::new("accounts")
//!     .handler(account_opened)
//!     .handler(money_deposited);
//!
//! // Load aggregate state
//! let result = projection
//!     .load::<Account>("account-123")
//!     .execute(&executor)
//!     .await?;
//!
//! // Or start a subscription
//! let subscription = projection
//!     .subscription()
//!     .routing_key("accounts")
//!     .start(&executor)
//!     .await?;
//! ```

use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin};

use crate::{
    context,
    cursor::{self, Args, Cursor},
    Aggregator, AggregatorBuilder, AggregatorEvent, Executor, ReadAggregator,
};

/// Trait for event handlers.
///
/// Handlers process events in two modes:
/// - `handle`: For subscriptions that perform side effects (send emails, update read models)
/// - `apply`: For loading aggregate state by replaying events
///
/// This trait is typically implemented via the `#[evento::handler]` macro.
pub trait Handler<P: 'static>: Sync + Send {
    /// Applies an event to build projection state.
    ///
    /// This is called when loading aggregate state by replaying events.
    /// It should be a pure function that modifies the projection without side effects.
    fn handle<'a>(
        &'a self,
        projection: &'a mut P,
        event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

    /// Returns the aggregator type this handler processes.
    fn aggregator_type(&self) -> &'static str;
    /// Returns the event name this handler processes.
    fn event_name(&self) -> &'static str;
}

pub trait ProjectionCursor {
    fn get_cursor(&self) -> cursor::Value;
    fn set_cursor(&mut self, v: &cursor::Value);
}

pub trait ProjectionAggregator: ProjectionCursor {
    fn aggregator_id(&self) -> String {
        todo!("ProjectionCursor.aggregator_id must be implemented for ProjectionCursor.aggregator")
    }

    fn aggregator_version(&self) -> anyhow::Result<u16> {
        let value = self.get_cursor();
        if value == Default::default() {
            return Ok(0);
        }

        let cursor = crate::Event::deserialize_cursor(&value)?;

        Ok(cursor.v)
    }

    fn aggregator(&self) -> anyhow::Result<AggregatorBuilder> {
        Ok(AggregatorBuilder::new(self.aggregator_id())
            .original_version(self.aggregator_version()?)
            .to_owned())
    }
}

/// Trait for types that can be restored from snapshots.
///
/// Snapshots provide a performance optimization by storing pre-computed
/// state, avoiding the need to replay all events from the beginning.
///
/// This trait is typically implemented via the `#[evento::snapshot]` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::snapshot]
/// #[derive(Default)]
/// pub struct AccountView {
///     pub balance: i64,
///     pub owner: String,
/// }
///
/// // The macro generates the restore implementation that loads
/// // from a snapshot table if available
/// ```
pub trait Snapshot: ProjectionCursor + Sized {
    /// Restores state from a snapshot if available.
    ///
    /// Returns `None` if no snapshot exists for the given ID.
    fn restore(
        _context: &context::RwContext,
        _id: String,
        _aggregators: &HashMap<String, String>,
    ) -> impl Future<Output = anyhow::Result<Option<Self>>> + Send {
        Box::pin(async { Ok(None) })
    }

    fn take_snapshot(
        &self,
        _context: &context::RwContext,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        Box::pin(async { Ok(()) })
    }
}

/// Builder for loading aggregate state from events.
///
/// Created via [`Projection::load`], this builder configures how to
/// load an aggregate's state by replaying events.
///
/// # Example
///
/// ```rust,ignore
/// let result = projection
///     .load::<Account>("account-123")
///     .data(app_config)  // Add shared data
///     .aggregator::<User>("user-456")  // Add related aggregate
///     .execute(&executor)
///     .await?;
/// ```
pub struct Projection<P: Snapshot + Default + 'static> {
    id: String,
    aggregators: HashMap<String, String>,
    handlers: HashMap<String, Box<dyn Handler<P>>>,
    context: context::RwContext,
    safety_disabled: bool,
}

impl<P: Snapshot + Default + 'static> Projection<P> {
    /// Creates a builder for loading aggregate state.
    ///
    /// This consumes the projection and returns a [`LoadBuilder`] configured
    /// to load the state for the specified aggregate.
    ///
    /// # Type Parameters
    ///
    /// - `A`: The aggregate type to load
    pub fn new<A: Aggregator>(id: impl Into<String>) -> Projection<P>
    where
        P: Snapshot + Default,
    {
        let id = id.into();
        let mut aggregators = HashMap::new();
        aggregators.insert(A::aggregator_type().to_owned(), id.to_owned());

        Projection {
            id,
            aggregators,
            context: Default::default(),
            handlers: HashMap::new(),
            safety_disabled: true,
        }
    }

    pub fn safety_check(mut self) -> Self {
        self.safety_disabled = false;

        self
    }

    /// Registers an event handler with this projection.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn handler<H: Handler<P> + 'static>(mut self, h: H) -> Self {
        let key = format!("{}_{}", h.aggregator_type(), h.event_name());
        if self.handlers.insert(key.to_owned(), Box::new(h)).is_some() {
            panic!("Cannot register event handler: key {} already exists", key);
        }
        self
    }

    /// Registers a skip handler with this projection.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn skip<EV: AggregatorEvent + Send + Sync + 'static>(self) -> Self {
        self.handler(SkipHandler::<EV>(PhantomData))
    }

    /// Adds shared data to the load context.
    ///
    /// Data added here is accessible in handlers via the context.
    pub fn data<D: Send + Sync + 'static>(self, v: D) -> Self {
        self.context.insert(v);

        self
    }

    /// Adds a related aggregate to load events from.
    ///
    /// Use this when the projection needs events from multiple aggregates.
    pub fn aggregator<A: Aggregator>(self, id: impl Into<String>) -> Self {
        self.aggregator_raw(A::aggregator_type().to_owned(), id)
    }

    /// Adds a related aggregate to load events from.
    ///
    /// Use this when the projection needs events from multiple aggregates.
    pub fn aggregator_raw(
        mut self,
        aggregator_type: impl Into<String>,
        id: impl Into<String>,
    ) -> Self {
        self.aggregators.insert(aggregator_type.into(), id.into());

        self
    }

    /// Executes the load operation, returning the rebuilt state.
    ///
    /// Returns `None` if no events exist for the aggregate.
    /// Returns `Err` if there are too many events to process in one batch.
    pub async fn execute<E: Executor>(&self, executor: &E) -> anyhow::Result<Option<P>> {
        let context = self.context.clone();
        let snapshot = P::restore(&context, self.id.to_owned(), &self.aggregators).await?;
        let cursor = snapshot.as_ref().map(|s| s.get_cursor());

        let read_aggregators = self
            .handlers
            .values()
            .map(|h| match self.aggregators.get(h.aggregator_type()) {
                Some(id) => ReadAggregator {
                    aggregator_type: h.aggregator_type().to_owned(),
                    aggregator_id: Some(id.to_owned()),
                    name: if self.safety_disabled {
                        Some(h.event_name().to_owned())
                    } else {
                        None
                    },
                },
                _ => {
                    if self.safety_disabled {
                        ReadAggregator::event(h.aggregator_type(), h.event_name())
                    } else {
                        ReadAggregator::aggregator(h.aggregator_type())
                    }
                }
            })
            .collect::<Vec<_>>();

        let events = executor
            .read(
                Some(read_aggregators.to_vec()),
                None,
                Args::forward(100, cursor.clone()),
            )
            .await?;

        if events.edges.is_empty() && snapshot.is_none() {
            return Ok(None);
        }

        let mut snapshot = snapshot.unwrap_or_default();

        for event in events.edges.iter() {
            let key = format!("{}_{}", event.node.aggregator_type, event.node.name);

            let Some(handler) = self.handlers.get(&key) else {
                if !self.safety_disabled {
                    anyhow::bail!("no handler k={key}");
                }

                continue;
            };

            handler.handle(&mut snapshot, &event.node).await?;
        }

        if let Some(event) = events.edges.last() {
            snapshot.set_cursor(&event.cursor);
            snapshot.take_snapshot(&context).await?;
        }

        if events.page_info.has_next_page {
            anyhow::bail!("Too busy");
        }

        Ok(Some(snapshot))
    }
}

pub(crate) struct SkipHandler<E: AggregatorEvent>(PhantomData<E>);

impl<P: 'static, EV: AggregatorEvent + Send + Sync> Handler<P> for SkipHandler<EV> {
    fn handle<'a>(
        &'a self,
        _projection: &'a mut P,
        _event: &'a crate::Event,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn aggregator_type(&self) -> &'static str {
        EV::aggregator_type()
    }

    fn event_name(&self) -> &'static str {
        EV::event_name()
    }
}
