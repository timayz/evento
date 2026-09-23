use evento::{metadata::RawEvent, AggregateEvent, Event, FromEventError};
use std::marker::PhantomData;

#[evento::aggregate(name = "myapp/Ledger")]
pub enum Ledger {
    #[evento(upcast_to = EntryAddedV2)]
    EntryAdded { amount: i64 },
    EntryAddedV2 { amount: i64, memo: String },

    #[evento(name = "legacy.tagged")]
    Tagged(String),

    Closed,
}

impl From<EntryAdded> for EntryAddedV2 {
    fn from(old: EntryAdded) -> Self {
        Self {
            amount: old.amount,
            memo: "unknown".into(),
        }
    }
}

/// Builds a stored event the way the executor would.
fn stored(name: &str, data: Vec<u8>) -> Event {
    Event {
        aggregate_type: "myapp/Ledger".into(),
        name: name.into(),
        data,
        ..Default::default()
    }
}

fn main() {
    // Named variant round-trips.
    let event = stored("EntryAddedV2", bitcode::encode(&EntryAddedV2 {
        amount: 42,
        memo: "rent".into(),
    }));
    assert_eq!(
        LedgerEvent::try_from(&event).unwrap(),
        LedgerEvent::EntryAddedV2(EntryAddedV2 {
            amount: 42,
            memo: "rent".into()
        })
    );

    // Tuple variant, matched on its pinned stored name rather than the ident.
    let event = stored("legacy.tagged", bitcode::encode(&Tagged("vat".to_owned())));
    let decoded = LedgerEvent::try_from(&event).unwrap();
    assert_eq!(decoded, LedgerEvent::Tagged(Tagged("vat".into())));
    assert_eq!(decoded.event_name(), "legacy.tagged");
    assert_eq!(decoded.event_name(), Tagged::event_name());

    // Unit variant.
    let event = stored("Closed", bitcode::encode(&Closed));
    assert_eq!(LedgerEvent::try_from(&event).unwrap(), LedgerEvent::Closed(Closed));

    // An `upcast_to` predecessor decodes into its OWN variant: the enum is a
    // verbatim view of what is stored, so no upcasting is applied.
    let event = stored("EntryAdded", bitcode::encode(&EntryAdded { amount: 7 }));
    let decoded = LedgerEvent::try_from(&event).unwrap();
    assert_eq!(decoded, LedgerEvent::EntryAdded(EntryAdded { amount: 7 }));
    assert_eq!(decoded.event_name(), "EntryAdded");

    // `RawEvent<A>` decodes itself through the marker struct.
    let raw: RawEvent<'_, Ledger> = RawEvent(&event, PhantomData);
    assert_eq!(raw.decode().unwrap(), LedgerEvent::EntryAdded(EntryAdded { amount: 7 }));

    // `From<Variant>` builds the enum without naming the variant twice.
    assert_eq!(
        LedgerEvent::from(Closed),
        LedgerEvent::Closed(Closed)
    );

    // An event of another aggregate is rejected before any decoding.
    let mut event = stored("Closed", bitcode::encode(&Closed));
    event.aggregate_type = "myapp/Other".into();
    assert!(matches!(
        LedgerEvent::try_from(&event),
        Err(FromEventError::AggregateMismatch { expected: "myapp/Ledger", got }) if got == "myapp/Other"
    ));

    // An unknown name is reported rather than silently skipped.
    let event = stored("NeverDeclared", vec![]);
    assert!(matches!(
        LedgerEvent::try_from(&event),
        Err(FromEventError::UnknownEvent { aggregate_type: "myapp/Ledger", name }) if name == "NeverDeclared"
    ));

    // A corrupt payload names the event it failed on.
    let event = stored("EntryAddedV2", vec![0xFF]);
    assert!(matches!(
        LedgerEvent::try_from(&event),
        Err(FromEventError::Decode { name, .. }) if name == "EntryAddedV2"
    ));
}
