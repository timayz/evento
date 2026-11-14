//! The write signal a backend wakes its subscriptions with.
//!
//! - [`WriteInterest`] - What a watcher follows: the filter of its reads
//! - [`WriteSignal`] - The channels of an executor: one for every write, one
//!   per interest
//! - [`WriteWake`] - The watchers a batch wakes, fired once it is committed

use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::{Event, EventFilter, RoutingKey, TenantFilter};

/// What a watcher of [`Executor::write_watch_for`](crate::Executor::write_watch_for)
/// follows: the filter it reads with, the three arguments of
/// [`Executor::read`](crate::Executor::read).
///
/// A write none of whose events pass the filter holds nothing the watcher
/// would read, so it is not woken for it.
#[derive(Clone, Debug)]
pub struct WriteInterest {
    /// The aggregate filters of the read; `None` for every aggregate.
    pub filters: Option<Arc<[EventFilter]>>,
    /// The routing key of the read; `None` for any.
    pub routing_key: Option<RoutingKey>,
    /// The tenants of the read.
    pub tenant: TenantFilter,
}

impl WriteInterest {
    /// Every write of `tenant`, whatever its aggregate and routing key.
    pub fn tenant(tenant: impl Into<String>) -> Self {
        Self {
            filters: None,
            routing_key: None,
            tenant: TenantFilter::Only(tenant.into()),
        }
    }

    /// Whether a read with this filter returns `event`.
    pub fn matches(&self, event: &Event) -> bool {
        let routing = match &self.routing_key {
            Some(RoutingKey::Value(Some(key))) => event.routing_key.as_ref() == Some(key),
            Some(RoutingKey::Value(None)) => event.routing_key.is_none(),
            Some(RoutingKey::All) | None => true,
        };
        routing
            && self.tenant.matches(&event.tenant)
            && self.filters.as_deref().is_none_or(|filters| {
                filters.iter().any(|f| {
                    f.aggregate_type == event.aggregate_type
                        && f.aggregate_id
                            .as_ref()
                            .is_none_or(|id| *id == event.aggregate_id)
                        && f.name.as_ref().is_none_or(|name| *name == event.name)
                })
            })
    }
}

/// An interest and the channel of the receivers that watch with it.
type Watcher = (WriteInterest, watch::Sender<u64>);

/// The write signal of an executor: what its
/// [`write_watch`](crate::Executor::write_watch) and
/// [`write_watch_for`](crate::Executor::write_watch_for) hand out, and what
/// its `write` and `replicate` fire.
///
/// A backend keeps one, shared by its clones, and brackets each commit:
///
/// ```rust,ignore
/// let wake = self.signal.prepare(&events);
/// self.insert(events).await?;
/// wake.fire();
/// ```
///
/// [`prepare`](Self::prepare) picks the watchers the batch concerns while the
/// events are still at hand, and [`WriteWake::fire`] wakes them once the
/// events can be read. A batch that fails drops the [`WriteWake`] and wakes
/// nobody.
#[derive(Clone, Debug)]
pub struct WriteSignal {
    /// Bumped by every write.
    all: watch::Sender<u64>,
    /// One channel per interest, bumped by the writes that match it. An
    /// entry leaves when its last receiver is gone.
    interests: Arc<Mutex<Vec<Watcher>>>,
}

impl Default for WriteSignal {
    fn default() -> Self {
        Self {
            all: watch::channel(0).0,
            interests: Arc::default(),
        }
    }
}

impl WriteSignal {
    /// A signal nobody watches yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// A receiver notified by every write.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.all.subscribe()
    }

    /// A receiver notified by the writes that hold an event `interest`
    /// matches.
    pub fn subscribe_to(&self, interest: WriteInterest) -> watch::Receiver<u64> {
        let (tx, rx) = watch::channel(0);
        self.interests
            .lock()
            .expect("write interests poisoned")
            .push((interest, tx));
        rx
    }

    /// The watchers a write of `events` wakes; fire it once the events are
    /// committed.
    pub fn prepare(&self, events: &[Event]) -> WriteWake {
        let mut interests = self.interests.lock().expect("write interests poisoned");
        // The one place the registry is walked on every write, so it is also
        // where the watchers that went away are dropped.
        interests.retain(|(_, tx)| !tx.is_closed());
        let matched = interests
            .iter()
            .filter(|(interest, _)| events.iter().any(|event| interest.matches(event)))
            .map(|(_, tx)| tx.clone())
            .collect();
        WriteWake {
            all: self.all.clone(),
            matched,
        }
    }

    /// Wakes every watcher, whatever it follows: for a write whose events are
    /// not at hand.
    pub fn fire_all(&self) {
        self.all.send_modify(|v| *v += 1);
        for (_, tx) in self
            .interests
            .lock()
            .expect("write interests poisoned")
            .iter()
        {
            tx.send_modify(|v| *v += 1);
        }
    }
}

/// The watchers one write wakes, picked by [`WriteSignal::prepare`].
#[derive(Debug)]
#[must_use = "the watchers are woken by `fire`, once the write is committed"]
pub struct WriteWake {
    all: watch::Sender<u64>,
    matched: Vec<watch::Sender<u64>>,
}

impl WriteWake {
    /// Wakes the watchers: the events are committed and can be read.
    pub fn fire(self) {
        self.all.send_modify(|v| *v += 1);
        for tx in self.matched {
            tx.send_modify(|v| *v += 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(aggregate_type: &str, name: &str, routing_key: Option<&str>, tenant: &str) -> Event {
        Event {
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: "1".to_owned(),
            name: name.to_owned(),
            routing_key: routing_key.map(str::to_owned),
            tenant: tenant.to_owned(),
            ..Default::default()
        }
    }

    fn interest(filters: &[EventFilter]) -> WriteInterest {
        WriteInterest {
            filters: Some(filters.into()),
            routing_key: Some(RoutingKey::Value(None)),
            tenant: TenantFilter::All,
        }
    }

    #[test]
    fn an_interest_matches_what_its_read_returns() {
        let by_type = interest(&[EventFilter::by_type_raw("bank/Account")]);
        assert!(by_type.matches(&event("bank/Account", "Opened", None, "")));
        assert!(!by_type.matches(&event("bank/Card", "Opened", None, "")));
        // `Value(None)` reads the events written without a routing key.
        assert!(!by_type.matches(&event("bank/Account", "Opened", Some("eu"), "")));

        let by_event = interest(&[EventFilter::by_event_raw("bank/Account", "Closed")]);
        assert!(by_event.matches(&event("bank/Account", "Closed", None, "")));
        assert!(!by_event.matches(&event("bank/Account", "Opened", None, "")));

        let by_id = interest(&[EventFilter::by_id_raw("bank/Account", "2")]);
        assert!(!by_id.matches(&event("bank/Account", "Opened", None, "")));

        let keyed = WriteInterest {
            routing_key: Some(RoutingKey::Value(Some("eu".to_owned()))),
            ..by_type.clone()
        };
        assert!(keyed.matches(&event("bank/Account", "Opened", Some("eu"), "")));
        assert!(!keyed.matches(&event("bank/Account", "Opened", Some("us"), "")));
        assert!(!keyed.matches(&event("bank/Account", "Opened", None, "")));

        let any_key = WriteInterest {
            routing_key: Some(RoutingKey::All),
            ..by_type
        };
        assert!(any_key.matches(&event("bank/Account", "Opened", Some("us"), "")));

        let tenant = WriteInterest::tenant("shop");
        assert!(tenant.matches(&event("bank/Card", "Opened", Some("eu"), "shop")));
        assert!(!tenant.matches(&event("bank/Card", "Opened", Some("eu"), "")));
    }

    #[test]
    fn a_write_wakes_the_interests_it_matches_once_fired() {
        let signal = WriteSignal::new();
        let mut all = signal.subscribe();
        let mut accounts =
            signal.subscribe_to(interest(&[EventFilter::by_type_raw("bank/Account")]));
        let mut cards = signal.subscribe_to(interest(&[EventFilter::by_type_raw("bank/Card")]));

        let wake = signal.prepare(&[
            event("bank/Order", "Placed", None, ""),
            event("bank/Account", "Opened", None, ""),
        ]);
        // Nothing can be read before the commit.
        assert!(!all.has_changed().unwrap());
        assert!(!accounts.has_changed().unwrap());

        wake.fire();
        assert!(all.has_changed().unwrap());
        assert!(accounts.has_changed().unwrap());
        assert!(!cards.has_changed().unwrap());

        all.borrow_and_update();
        accounts.borrow_and_update();
        signal.fire_all();
        assert!(all.has_changed().unwrap());
        assert!(accounts.has_changed().unwrap());
        assert!(cards.has_changed().unwrap());
        cards.borrow_and_update();
    }

    #[test]
    fn a_failed_write_wakes_nobody() {
        let signal = WriteSignal::new();
        let all = signal.subscribe();
        let accounts = signal.subscribe_to(interest(&[EventFilter::by_type_raw("bank/Account")]));

        drop(signal.prepare(&[event("bank/Account", "Opened", None, "")]));
        assert!(!all.has_changed().unwrap());
        assert!(!accounts.has_changed().unwrap());
    }

    #[test]
    fn an_interest_leaves_with_its_last_receiver() {
        let signal = WriteSignal::new();
        let accounts = signal.subscribe_to(interest(&[EventFilter::by_type_raw("bank/Account")]));
        let clone = accounts.clone();
        drop(accounts);
        signal.prepare(&[]).fire();
        assert_eq!(signal.interests.lock().unwrap().len(), 1);

        drop(clone);
        signal.prepare(&[]).fire();
        assert!(signal.interests.lock().unwrap().is_empty());
    }
}
