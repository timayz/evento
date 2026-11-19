//! Stream claims: a stream is created once, whatever routing key each of its
//! creators names it under (#570).
//!
//! A key's shard is chosen by its routing key ([`Key::shard_prefix`]), so two
//! creations of one stream under routing keys of **two shards** are voted on
//! by two replica sets: they share no replica, both commit, and each shard
//! then holds a stream of that id with another first event. Inside one shard
//! the two conflict by the stream they name ([`Key::stream`], #563); across
//! shards nothing makes them meet.
//!
//! With [`NodeConfig::stream_claims`](crate::NodeConfig::stream_claims) on, a
//! write that creates a stream — it carries the stream's version 1 — also
//! carries a **claim**: one more event ([`claim_of`]), the first of a stream
//! of its own that is derived from the created stream alone (its tenant, its
//! type and its id, no routing key). The claim's key is the stream's id in
//! its tenant, so it lives in the shard that id hashes to — the same for
//! every creator. Two creations of one stream then both write version 1 of
//! one claim stream, in one shard: the second reads a version conflict there,
//! and the transaction aborts in every shard it touches, as any multi-shard
//! write whose condition fails in one.
//!
//! A claim is an event like any other to everything below the coordinator:
//! journaled, applied, copied to a joining node, moved with its slot by a
//! re-sharding, repaired by anti-entropy. Only its aggregate type tells it
//! apart ([`is_claim`]): no subscription or load names that type, so none
//! reads it.
//!
//! What it costs: one more event per stream created, and a creation under a
//! routing key whose shard is not the id's becomes a two-shard transaction.
//! A stream written without a routing key has its claim in its own shard.
//!
//! A stream created while the setting was off has no claim: turn it on before
//! the cluster has a second shard, on every node.

use evento_core::Event;

use crate::api::stable_hash;

/// What the aggregate type of every claim starts with ([`claim_type`]).
pub const CLAIM_TYPE: &str = "evento-cluster/Claim";

/// The aggregate type of the claims on streams of `aggregate_type`:
/// [`CLAIM_TYPE`] and a stable hash of the type. One claim stream per stream:
/// two aggregates of different types may share an id, and each is created on
/// its own. A hash, not the type itself: the SQL stores keep an aggregate
/// type in 50 characters, which the type may already fill.
pub fn claim_type(aggregate_type: &str) -> String {
    format!(
        "{CLAIM_TYPE}:{:016x}",
        stable_hash(aggregate_type.as_bytes())
    )
}

/// What tells a claim's id from the id of the event it goes with: bits of the
/// id's random part, its time left as it is.
const CLAIM_ID: u128 = 0xc1a1 << 64;

/// Whether `event` is a claim ([`claim_of`]) and not an application's event.
pub fn is_claim(event: &Event) -> bool {
    event
        .aggregate_type
        .strip_prefix(CLAIM_TYPE)
        .is_some_and(|rest| rest.starts_with(':'))
}

/// The claim a creation of `event`'s stream carries: version 1 of the claim
/// stream of that stream — same tenant, same id, the type's
/// [`claim_type`] — with no routing key, so its key is the id alone in its
/// tenant. Its id is the event's, a few bits apart. It is named after the stream's aggregate type and holds the
/// routing key the stream was created under (nothing for none), for whoever
/// inspects the store.
pub fn claim_of(event: &Event) -> Event {
    Event {
        // Derived from the event's own id, one for one: the write written
        // again carries the same claim, and another creation's another.
        id: ulid::Ulid::from(u128::from(event.id) ^ CLAIM_ID),
        aggregate_id: event.aggregate_id.clone(),
        aggregate_type: claim_type(&event.aggregate_type),
        version: 1,
        name: event.aggregate_type.clone(),
        routing_key: None,
        tenant: event.tenant.clone(),
        data: event
            .routing_key
            .as_deref()
            .map(|key| key.as_bytes().to_vec())
            .unwrap_or_default(),
        metadata: Default::default(),
        timestamp: event.timestamp,
        timestamp_subsec: event.timestamp_subsec,
    }
}

/// What the aggregate types of the cluster's own streams start with: the
/// claims, and the routing keys the nodes serve ([`keys`](crate::keys)).
const OWN_TYPES: &str = "evento-cluster/";

/// `events` with the claim of every stream they create: one per event at
/// version 1. A stream of the cluster's own has none: every node names it
/// the same way, without a routing key.
pub(crate) fn with_claims(events: &[Event]) -> Vec<Event> {
    let claims: Vec<Event> = events
        .iter()
        .filter(|event| event.version == 1 && !event.aggregate_type.starts_with(OWN_TYPES))
        .map(claim_of)
        .collect();
    let mut all = Vec::with_capacity(events.len() + claims.len());
    all.extend_from_slice(events);
    all.extend(claims);
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Key;

    fn event(aggregate_type: &str, id: &str, version: u16, routing_key: Option<&str>) -> Event {
        Event {
            id: ulid::Ulid::generate(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: id.into(),
            version,
            routing_key: routing_key.map(str::to_owned),
            tenant: "acme".into(),
            ..Default::default()
        }
    }

    /// Whatever routing key a creation names the stream under, its claim is
    /// the same stream, under the same key: the stream's id in its tenant.
    #[test]
    fn every_creation_of_a_stream_claims_the_same_stream() {
        let eu = claim_of(&event("bank/Account", "a", 1, Some("eu")));
        let us = claim_of(&event("bank/Account", "a", 1, Some("us")));
        let plain = claim_of(&event("bank/Account", "a", 1, None));
        for claim in [&eu, &us, &plain] {
            assert!(is_claim(claim));
            assert_eq!(claim.version, 1);
            assert_eq!(claim.tenant, "acme");
            assert_eq!(Key::of(claim), Key::in_tenant("acme", "a"));
            assert_eq!(claim.aggregate_type, eu.aggregate_type);
            assert_eq!(claim.name, "bank/Account");
        }
        assert_ne!(eu.id, us.id);
        let creation = event("bank/Account", "a", 1, Some("eu"));
        assert_eq!(claim_of(&creation).id, claim_of(&creation).id);
        assert_ne!(claim_of(&creation).id, creation.id);
        assert_eq!(eu.data, b"eu");
        assert!(plain.data.is_empty());
    }

    /// Two aggregate types sharing an id are two streams, each with a claim.
    #[test]
    fn another_type_under_the_same_id_is_another_claim() {
        let account = claim_of(&event("bank/Account", "a", 1, None));
        let owner = claim_of(&event("bank/Owner", "a", 1, None));
        assert_ne!(account.aggregate_type, owner.aggregate_type);
        assert_eq!(Key::of(&account), Key::of(&owner));
    }

    /// A claim's type fits the 50 characters the SQL stores give it, however
    /// long the stream's own.
    #[test]
    fn a_claim_type_fits_the_sql_column() {
        assert!(claim_type(&"t".repeat(50)).len() <= 50);
    }

    #[test]
    fn only_a_creation_carries_a_claim() {
        let events = vec![
            event("bank/Account", "a", 1, Some("eu")),
            event("bank/Account", "a", 2, Some("eu")),
            event("bank/Account", "b", 7, Some("eu")),
        ];
        let all = with_claims(&events);
        assert_eq!(all.len(), 4);
        assert_eq!(&all[..3], &events[..]);
        assert!(is_claim(&all[3]));
        assert_eq!(all[3].aggregate_id, "a");
        // The cluster's own streams are not claimed: a claim, a served
        // routing key.
        assert_eq!(with_claims(&all[3..]).len(), 1);
        let served = event(crate::keys::AGGREGATE_TYPE, "eu", 1, None);
        assert_eq!(with_claims(&[served]).len(), 1);
        assert!(!is_claim(&events[0]));
        assert!(!is_claim(&event("evento-cluster/Claimed", "a", 1, None)));
    }
}
