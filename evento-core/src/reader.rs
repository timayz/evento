//! A first-class reader for an aggregate's event stream.
//!
//! [`Projection`](crate::projection::Projection) covers "build a read model".
//! This module covers the other half — "just give me this aggregate's events" —
//! without dropping to the raw [`Executor::read`] interface and its four
//! positional arguments.
//!
//! ```rust,no_run
//! # #[evento::aggregate]
//! # pub enum Account {
//! #     AccountOpened { owner: String },
//! # }
//! # async fn run<E: evento::Executor>(executor: &E, id: &str) -> anyhow::Result<()> {
//! // The whole stream, paginated internally.
//! let events: Vec<evento::Event> = evento::read::<Account>(id).execute(executor).await?;
//!
//! // Typed, exhaustively matchable.
//! let typed: Vec<AccountEvent> = evento::read::<Account>(id).decode(executor).await?;
//! # Ok(())
//! # }
//! ```

use std::{marker::PhantomData, sync::Arc};

use crate::{
    cursor::{Args, ReadResult, Value},
    Aggregate, AggregateEvent, AggregateEvents, Event, EventFilter, Executor, RoutingKey,
};

/// Events fetched per backend round trip while draining a stream.
///
/// Matches the page size `Projection` replays with.
const PAGE_SIZE: usize = 100;

/// Page size used by [`ReadBuilder::page`] when no [`limit`](ReadBuilder::limit)
/// is set — the same default [`Args::get_info`] applies.
const DEFAULT_PAGE: u16 = 40;

/// Reads the events of one aggregate instance.
///
/// The aggregate type comes from `A`, so it cannot drift from the aggregate
/// definition. Use [`read_raw`] when the type is only known as a string.
///
/// # Example
///
/// ```rust,no_run
/// # #[evento::aggregate]
/// # pub enum Account {
/// #     AccountOpened { owner: String },
/// #     MoneyDeposited { amount: i64 },
/// # }
/// # async fn run<E: evento::Executor>(executor: &E, id: &str) -> anyhow::Result<()> {
/// // Every event of the stream.
/// let all = evento::read::<Account>(id).execute(executor).await?;
///
/// // The 10 most recent.
/// let recent = evento::read::<Account>(id)
///     .backward()
///     .limit(10)
///     .execute(executor)
///     .await?;
///
/// // Just the deposits.
/// let deposits = evento::read::<Account>(id)
///     .event::<MoneyDeposited>()
///     .execute(executor)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn read<A: Aggregate>(id: impl Into<String>) -> ReadBuilder<A> {
    ReadBuilder::from_filter(EventFilter::by_id::<A>(id))
}

/// Reads the events of one aggregate instance, from a raw aggregate type string.
///
/// The untyped counterpart of [`read`], for callers that only know the
/// aggregate type at runtime. The returned builder has no
/// [`decode`](ReadBuilder::decode), since there is no type to decode into.
pub fn read_raw(aggregate_type: impl Into<String>, id: impl Into<String>) -> ReadBuilder {
    ReadBuilder::from_filter(EventFilter::by_id_raw(aggregate_type, id))
}

/// Builder for reading an aggregate's events.
///
/// Created by [`read`] or [`read_raw`]. Terminal methods take the executor, so
/// one builder can be reused across backends.
///
/// # Routing keys
///
/// Unlike a [`SubscriptionBuilder`](crate::subscription::SubscriptionBuilder) —
/// which without `.routing_key()` sees only events whose routing key is `NULL` —
/// a reader with no routing filter reads events under **every** routing key.
/// Narrow it with [`routing_key`](Self::routing_key) or
/// [`no_routing_key`](Self::no_routing_key).
pub struct ReadBuilder<A = ()> {
    filter: EventFilter,
    routing_key: Option<RoutingKey>,
    limit: Option<u16>,
    after: Option<Value>,
    before: Option<Value>,
    backward: bool,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A> ReadBuilder<A> {
    fn from_filter(filter: EventFilter) -> Self {
        Self {
            filter,
            routing_key: None,
            limit: None,
            after: None,
            before: None,
            backward: false,
            _aggregate: PhantomData,
        }
    }

    /// Narrows the read to a single event type of this aggregate.
    ///
    /// Only the event *name* is taken from `EV`; the aggregate type stays the
    /// one the builder was created with.
    pub fn event<EV: AggregateEvent>(mut self) -> Self {
        self.filter.name = Some(EV::event_name().to_owned());
        self
    }

    /// Narrows the read to a single event name, as a string.
    ///
    /// The untyped counterpart of [`event`](Self::event).
    pub fn event_raw(mut self, name: impl Into<String>) -> Self {
        self.filter.name = Some(name.into());
        self
    }

    /// Reads only events committed with this routing key.
    pub fn routing_key(mut self, v: impl Into<String>) -> Self {
        self.routing_key = Some(RoutingKey::Value(Some(v.into())));
        self
    }

    /// Reads only events committed with *no* routing key.
    ///
    /// The default — no routing filter at all — reads every key instead.
    pub fn no_routing_key(mut self) -> Self {
        self.routing_key = Some(RoutingKey::Value(None));
        self
    }

    /// Caps the total number of events returned.
    ///
    /// On [`execute`](Self::execute) and [`decode`](Self::decode) this is the
    /// total across every page, not a page size; on [`page`](Self::page) it is
    /// the size of the single page fetched. Unset, `execute`/`decode` return the
    /// whole stream.
    pub fn limit(mut self, v: u16) -> Self {
        self.limit = Some(v);
        self
    }

    /// Starts reading after this cursor (forward pagination).
    pub fn after(mut self, cursor: Value) -> Self {
        self.after = Some(cursor);
        self
    }

    /// Reads the events immediately *preceding* this cursor.
    ///
    /// Implies [`backward`](Self::backward), so with a
    /// [`limit`](Self::limit) this is "the N events before the cursor".
    pub fn before(mut self, cursor: Value) -> Self {
        self.before = Some(cursor);
        self.backward = true;
        self
    }

    /// Reads from the **end** of the stream instead of the start.
    ///
    /// With [`limit`](Self::limit) this is "the last N events" — the events are
    /// still returned oldest-first, matching the `last:` argument of a GraphQL
    /// connection. Without a limit it reads the same events as a forward read.
    pub fn backward(mut self) -> Self {
        self.backward = true;
        self
    }

    /// Replaces the pagination settings with a prebuilt [`Args`].
    ///
    /// An escape hatch for callers that already hold one (e.g. from a GraphQL
    /// connection argument). An `Args` with neither `first` nor `last` set
    /// leaves the read uncapped.
    pub fn args(mut self, args: Args) -> Self {
        self.backward = args.is_backward();
        self.limit = args.first.or(args.last);
        self.after = args.after;
        self.before = args.before;
        self
    }

    fn filters(&self) -> Arc<[EventFilter]> {
        Arc::from([self.filter.clone()])
    }

    fn args_for(&self, page_size: u16, cursor: Option<Value>) -> Args {
        if self.backward {
            Args::backward(page_size, cursor)
        } else {
            Args::forward(page_size, cursor)
        }
    }

    /// Reads a single page, with the cursors and flags needed to fetch the next.
    ///
    /// [`limit`](Self::limit) is the page size here (default 40). Use this when
    /// you are driving pagination yourself; [`execute`](Self::execute) drains
    /// the stream for you.
    pub async fn page<E: Executor>(&self, executor: &E) -> anyhow::Result<ReadResult<Event>> {
        let cursor = if self.backward {
            self.before.clone()
        } else {
            self.after.clone()
        };
        let args = self.args_for(self.limit.unwrap_or(DEFAULT_PAGE), cursor);

        executor
            .read(Some(self.filters()), self.routing_key.clone(), args, None)
            .await
    }

    /// Reads the aggregate's events, draining every page.
    ///
    /// Returns an empty `Vec` — not an error — when the stream does not exist.
    /// [`limit`](Self::limit) caps the total; without it the whole stream is
    /// returned.
    pub async fn execute<E: Executor>(&self, executor: &E) -> anyhow::Result<Vec<Event>> {
        let filters = self.filters();
        let mut cursor = if self.backward {
            self.before.clone()
        } else {
            self.after.clone()
        };
        let mut remaining = self.limit.map(usize::from);
        // One entry per page. A backward read walks the stream from the end, so
        // its pages arrive newest-block-first while each page is itself
        // oldest-first; the page order is reversed once at the end so the
        // result is chronological either way.
        let mut pages: Vec<Vec<Event>> = Vec::new();

        loop {
            let page_size = match remaining {
                Some(0) => break,
                Some(r) => r.min(PAGE_SIZE),
                None => PAGE_SIZE,
            };

            let result = executor
                .read(
                    Some(filters.clone()),
                    self.routing_key.clone(),
                    // `to_micros` is deliberately unbounded: the stability
                    // watermark exists to hold back a *persisted* subscription
                    // cursor, and a one-shot read wants read-your-writes.
                    self.args_for(page_size as u16, cursor.take()),
                    None,
                )
                .await?;

            let (more, next) = if self.backward {
                (
                    result.page_info.has_previous_page,
                    result.page_info.start_cursor.clone(),
                )
            } else {
                (
                    result.page_info.has_next_page,
                    result.page_info.end_cursor.clone(),
                )
            };

            let fetched = result.edges.len();
            pages.push(result.edges.into_iter().map(|edge| edge.node).collect());

            if let Some(r) = remaining.as_mut() {
                *r = r.saturating_sub(fetched);
                if *r == 0 {
                    break;
                }
            }

            if fetched == 0 || !more {
                break;
            }

            // A "more pages" claim with no cursor cannot advance the read —
            // stop rather than loop forever.
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        if self.backward {
            pages.reverse();
        }

        Ok(pages.into_iter().flatten().collect())
    }
}

impl<A: AggregateEvents> ReadBuilder<A> {
    /// Reads the aggregate's events, decoded into its generated events enum.
    ///
    /// Drains the stream like [`execute`](Self::execute), then converts each
    /// stored event with the `TryFrom<&Event>` impl that `#[evento::aggregate]`
    /// generates. Decoding is verbatim: `#[evento(upcast_to = ...)]` is **not**
    /// applied, so an old stored event decodes to its own variant — the same
    /// semantics [`RawEvent::decode`](crate::metadata::RawEvent::decode) has.
    ///
    /// Fails with [`FromEventError`](crate::FromEventError) if a stored event
    /// does not belong to this aggregate or its payload cannot be decoded.
    pub async fn decode<E: Executor>(&self, executor: &E) -> anyhow::Result<Vec<A::Events>> {
        let events = self.execute(executor).await?;
        events
            .iter()
            .map(|event| A::Events::try_from(event).map_err(Into::into))
            .collect()
    }
}
