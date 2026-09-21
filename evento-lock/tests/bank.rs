//! Dogfood: the bank example's persisted shapes, locked in
//! `examples/bank/events.lock`. Refresh with `EVENTO_LOCK=update cargo test -p evento-lock`.

#[test]
fn bank_persisted_shapes_only_grow() {
    let report = evento_lock::Config::new(env!("CARGO_MANIFEST_DIR"))
        .packages(["bank"])
        .lock_path("examples/bank/events.lock")
        .mode_from_env()
        .run()
        .unwrap();
    assert!(report.shapes > 0);
}
