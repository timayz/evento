#[evento::projection]
#[evento::snapshot(memory)]
pub struct MemView {
    pub id: String,
    pub balance: i64,
}

#[evento::projection(cursor = evento::cursor::Value)]
#[evento::snapshot(none)]
pub struct NoSnapView {
    pub value: i64,
}

fn _bounds<E: evento::Executor, S: evento::Snapshot<E>>() {}
fn _assert_impls<E: evento::Executor>() {
    _bounds::<E, MemView>();
    _bounds::<E, NoSnapView>();
}

fn main() {
    MemView::snapshot_rows().write().unwrap().insert(
        "account-1".into(),
        MemView {
            id: "account-1".into(),
            balance: 42,
            ..Default::default()
        },
    );

    let rows = MemView::snapshot_rows().read().unwrap();
    assert_eq!(rows.get("account-1").unwrap().balance, 42);
}
