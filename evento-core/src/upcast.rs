//! Routing of older stored events to the handler of the event they upcast to.
//!
//! Shared by [`Projection`](crate::projection::Projection) and
//! [`SubscriptionBuilder`](crate::subscription::SubscriptionBuilder): both
//! dispatch on the stored `"{aggregate_type}_{event_name}"` key, and both
//! consult an [`Aliases`] table when no handler is registered under it.

use std::{borrow::Cow, collections::HashMap};

use crate::{Event, Upcaster};

/// An older event name routed to the handler registered for a newer event.
pub(crate) struct Alias {
    /// Handler-map key of the target: `"{aggregate_type}_{to}"`.
    pub target_key: String,
    pub aggregate_type: &'static str,
    /// Stored name of the older event.
    pub from: &'static str,
    /// Name of the event the target handler expects.
    to: &'static str,
    hops: u8,
    upcast: fn(&[u8]) -> Result<Vec<u8>, bitcode::Error>,
    /// `false` when the target is a skip: the payload is never read, so it is
    /// not converted either.
    convert: bool,
}

impl Alias {
    /// The event as the target handler must see it: `name` and `data` are
    /// those of the newer event (a consistent pair for handlers that forward
    /// the event), everything else is the stored event's.
    pub fn apply<'a>(&self, event: &'a Event) -> anyhow::Result<Cow<'a, Event>> {
        if !self.convert {
            return Ok(Cow::Borrowed(event));
        }

        let data = (self.upcast)(&event.data).map_err(|e| {
            anyhow::anyhow!(
                "failed to upcast `{}` to `{}` (event {}): {e}",
                self.from,
                self.to,
                event.id
            )
        })?;
        tracing::debug!(from = self.from, to = self.to, "upcast event");

        let mut event = event.clone();
        event.name = self.to.to_owned();
        event.data = data;

        Ok(Cow::Owned(event))
    }
}

/// Older event names accepted on behalf of the registered handlers, keyed by
/// `"{aggregate_type}_{from}"`.
#[derive(Default)]
pub(crate) struct Aliases(HashMap<String, Alias>);

impl Aliases {
    /// Records the events that upcast into a handler being registered for
    /// `aggregate_type`/`to`.
    ///
    /// An older event can reach several registered events (`V1` reaches both
    /// `V2` and `V3` when both are handled): the nearest one wins, whatever
    /// the registration order.
    pub fn register(
        &mut self,
        aggregate_type: &'static str,
        to: &'static str,
        upcasters: &'static [Upcaster],
        convert: bool,
    ) {
        for upcaster in upcasters {
            let key = format!("{aggregate_type}_{}", upcaster.from);
            if self
                .0
                .get(&key)
                .is_some_and(|alias| alias.hops <= upcaster.hops)
            {
                continue;
            }

            self.0.insert(
                key,
                Alias {
                    target_key: format!("{aggregate_type}_{to}"),
                    aggregate_type,
                    from: upcaster.from,
                    to,
                    hops: upcaster.hops,
                    upcast: upcaster.upcast,
                    convert,
                },
            );
        }
    }

    pub fn get(&self, key: &str) -> Option<&Alias> {
        self.0.get(key)
    }

    /// Every alias with its `"{aggregate_type}_{from}"` key.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Alias)> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(suffix: u8) -> fn(&[u8]) -> Result<Vec<u8>, bitcode::Error> {
        match suffix {
            2 => |d| Ok([d, &[2]].concat()),
            _ => |d| Ok([d, &[3]].concat()),
        }
    }

    fn v2() -> &'static [Upcaster] {
        Box::leak(Box::new([Upcaster::new("V1", 1, append(2))]))
    }

    fn v3() -> &'static [Upcaster] {
        Box::leak(Box::new([
            Upcaster::new("V1", 2, append(3)),
            Upcaster::new("V2", 1, append(3)),
        ]))
    }

    #[test]
    fn nearest_target_wins_whatever_the_order() {
        for order in [[2u8, 3], [3, 2]] {
            let mut aliases = Aliases::default();
            for v in order {
                match v {
                    2 => aliases.register("t/A", "V2", v2(), true),
                    _ => aliases.register("t/A", "V3", v3(), true),
                }
            }

            assert_eq!(aliases.get("t/A_V1").unwrap().target_key, "t/A_V2");
            assert_eq!(aliases.get("t/A_V2").unwrap().target_key, "t/A_V3");
        }
    }

    #[test]
    fn apply_rewrites_name_and_data_only() {
        let mut aliases = Aliases::default();
        aliases.register("t/A", "V2", v2(), true);

        let stored = Event {
            id: ulid::Ulid::generate(),
            aggregate_id: "a-1".to_owned(),
            aggregate_type: "t/A".to_owned(),
            version: 7,
            name: "V1".to_owned(),
            routing_key: Some("rk".to_owned()),
            data: vec![1],
            metadata: Default::default(),
            timestamp: 42,
            timestamp_subsec: 5,
        };

        let upcast = aliases.get("t/A_V1").unwrap().apply(&stored).unwrap();
        assert_eq!(upcast.name, "V2");
        assert_eq!(upcast.data, vec![1, 2]);
        assert_eq!(
            (
                upcast.id,
                upcast.version,
                upcast.timestamp,
                &upcast.routing_key
            ),
            (stored.id, 7, 42, &stored.routing_key)
        );
    }

    #[test]
    fn skip_alias_leaves_the_event_untouched() {
        let mut aliases = Aliases::default();
        aliases.register("t/A", "V2", v2(), false);

        let stored = Event {
            id: ulid::Ulid::generate(),
            aggregate_id: "a-1".to_owned(),
            aggregate_type: "t/A".to_owned(),
            version: 1,
            name: "V1".to_owned(),
            routing_key: None,
            data: vec![1],
            metadata: Default::default(),
            timestamp: 0,
            timestamp_subsec: 0,
        };

        let upcast = aliases.get("t/A_V1").unwrap().apply(&stored).unwrap();
        assert!(matches!(upcast, Cow::Borrowed(_)));
    }
}
