#[evento::aggregate(name = "custom/Account", serde::Serialize)]
pub enum Account {
    /// Doc comments are preserved on the generated struct.
    #[evento(name = "opened.v1")]
    Opened { id: String },
    Closed,
    Adjusted(i64),
}

fn main() {
    use evento::{Aggregate, AggregateEvent};

    assert_eq!(Account::aggregate_type(), "custom/Account");
    assert_eq!(Opened::aggregate_type(), "custom/Account");
    assert_eq!(Opened::event_name(), "opened.v1");
    assert_eq!(Closed::event_name(), "Closed");
    assert_eq!(Adjusted::event_name(), "Adjusted");
}
