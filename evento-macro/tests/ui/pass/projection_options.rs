use evento::{ProjectionCursor, cursor, projection::ProjectionAggregate};

#[evento::projection(cursor = cursor::Value, id = id)]
pub struct View {
    pub id: String,
    pub balance: i64,
}

fn main() {
    let mut view = View {
        id: "account-1".into(),
        ..Default::default()
    };

    view.set_cursor(&cursor::Value("abc".into()));
    assert_eq!(view.get_cursor().0, "abc");

    view.set_aggregate_version(3);
    assert_eq!(view.get_aggregate_version(), 3);

    assert_eq!(view.aggregate_id(), "account-1");

    let builder = view.write().unwrap();
    let _ = builder;
}
