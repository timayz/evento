use evento::{Event, FromEventError};

/// An aggregate that declares no events yet: the generated enum is uninhabited.
#[evento::aggregate(name = "myapp/Empty")]
pub enum Empty {}

/// The degenerate non-empty case: one variant, so the match has a single arm.
#[evento::aggregate(name = "myapp/Single")]
pub enum Single {
    OnlyOne { amount: i64 },
}

/// The uninhabited enum is still a nameable type.
fn _accepts_empty(_: EmptyEvent) {}

fn main() {
    // Nothing can decode against an aggregate with no events, but it still
    // compiles and reports the event it could not place.
    let event = Event {
        aggregate_type: "myapp/Empty".into(),
        name: "Whatever".into(),
        ..Default::default()
    };
    assert!(matches!(
        EmptyEvent::try_from(&event),
        Err(FromEventError::UnknownEvent { name, .. }) if name == "Whatever"
    ));

    let event = Event {
        aggregate_type: "myapp/Single".into(),
        name: "OnlyOne".into(),
        data: bitcode::encode(&OnlyOne { amount: 5 }),
        ..Default::default()
    };
    let decoded = SingleEvent::try_from(&event).unwrap();
    assert_eq!(decoded, SingleEvent::OnlyOne(OnlyOne { amount: 5 }));
    assert_eq!(decoded.event_name(), "OnlyOne");
}
