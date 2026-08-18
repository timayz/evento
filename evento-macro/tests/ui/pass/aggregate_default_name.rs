#[evento::aggregate]
pub enum Ledger {
    EntryAdded { amount: i64 },
}

fn main() {
    use evento::{Aggregate, AggregateEvent};

    // Default identity stays `{pkg}/{Enum}`.
    assert!(Ledger::aggregate_type().ends_with("/Ledger"));
    assert_eq!(EntryAdded::aggregate_type(), Ledger::aggregate_type());
    assert_eq!(EntryAdded::event_name(), "EntryAdded");
}
