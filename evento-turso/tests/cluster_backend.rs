//! [`Turso`] as a `ClusterBackend`: the shared scenario over a store in each
//! write mode. Gated by the `cluster` feature.

#![cfg(feature = "cluster")]

use evento_turso::Turso;

#[tokio::test]
async fn cluster_backend() -> anyhow::Result<()> {
    evento_test::cluster_backend(&Turso::in_memory().await?).await
}

#[tokio::test]
async fn cluster_backend_concurrent() -> anyhow::Result<()> {
    evento_test::cluster_backend(&Turso::in_memory_concurrent().await?).await
}

/// Every table, index and statement the database records, in creation order.
async fn schema(store: &Turso) -> anyhow::Result<Vec<(String, String, Option<String>)>> {
    let conn = store.connect()?;
    let mut rows = conn
        .query(
            "SELECT type, name, sql FROM sqlite_schema ORDER BY tbl_name, type DESC, name",
            (),
        )
        .await?;
    let mut schema = Vec::new();
    while let Some(row) = rows.next().await? {
        schema.push((row.get(0)?, row.get(1)?, row.get(2)?));
    }
    Ok(schema)
}

/// Emptying for a join drops the store's tables and creates them again
/// (#510): the schema comes back as it was — the migrations' indexes and one
/// the application added —, the migration records and the commit clock stay.
async fn empty_for_join_keeps_the_schema(store: Turso) -> anyhow::Result<()> {
    use evento_cluster::ClusterBackend as _;
    use evento_core::Executor as _;

    store
        .connect()?
        .execute("CREATE INDEX idx_app_event_name ON event (name)", ())
        .await?;
    let event = |id: &str| evento_core::Event {
        id: ulid::Ulid::generate(),
        aggregate_id: id.to_owned(),
        aggregate_type: "demo/Join".to_owned(),
        version: 1,
        name: "Created".to_owned(),
        ..Default::default()
    };
    store.write(vec![event("a"), event("b")]).await?;
    let before = schema(&store).await?;
    let stamp = store.last_stamp();
    assert!(before
        .iter()
        .any(|(_, name, _)| name == "idx_app_event_name"));

    store.empty_for_join().await?;
    assert_eq!(schema(&store).await?, before);
    let mut conn = store.connect()?;
    assert_eq!(
        evento_turso::MIGRATIONS
            .to_latest(&mut conn)
            .await
            .map_err(|(_, e)| e)?,
        0,
        "the migrations stay recorded"
    );
    assert_eq!(store.last_stamp(), stamp);

    // The connections the store already held write the new tables.
    store.write(vec![event("a")]).await?;
    assert!(store.last_stamp() > stamp, "the commit clock goes on");
    Ok(())
}

#[tokio::test]
async fn empty_for_join_keeps_the_schema_serial() -> anyhow::Result<()> {
    empty_for_join_keeps_the_schema(Turso::temporary().await?).await
}

#[tokio::test]
async fn empty_for_join_keeps_the_schema_concurrent() -> anyhow::Result<()> {
    empty_for_join_keeps_the_schema(Turso::temporary_concurrent().await?).await
}
