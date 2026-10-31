//! The migration runner: `user_version` bookkeeping, idempotency, partial
//! application, a failing migration, and the crate's own [`MIGRATIONS`].

use evento_turso::migrate::{get_user_version, Migration, MigrationError, Migrations};
use evento_turso::{up_fn, MIGRATIONS};

const TWO: Migrations = Migrations::new(&[
    Migration::up("001", "CREATE TABLE friend(name TEXT NOT NULL);"),
    up_fn!("002", users),
]);

async fn users(conn: &turso::Connection) -> turso::Result<()> {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .await?;
    conn.execute("INSERT INTO users (name) VALUES ('Alice')", ())
        .await?;
    Ok(())
}

async fn fresh() -> turso::Connection {
    let db = turso::Builder::new_local(":memory:").build().await.unwrap();
    db.connect().unwrap()
}

#[tokio::test]
async fn applies_every_migration_once() {
    let mut conn = fresh().await;
    assert_eq!(TWO.to_latest(&mut conn).await.unwrap(), 2);
    assert_eq!(get_user_version(&conn).await.unwrap(), 2);
    conn.execute("INSERT INTO friend (name) VALUES ('test')", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO users (name) VALUES ('test')", ())
        .await
        .unwrap();
    // A second run has nothing to do.
    assert_eq!(TWO.to_latest(&mut conn).await.unwrap(), 0);
}

#[tokio::test]
async fn resumes_from_the_recorded_version() {
    let mut conn = fresh().await;
    conn.pragma_update("user_version", 1).await.unwrap();
    assert_eq!(TWO.to_latest(&mut conn).await.unwrap(), 1);
    assert_eq!(get_user_version(&conn).await.unwrap(), 2);
}

#[tokio::test]
async fn rejects_a_version_outside_the_list() {
    for (version, expected) in [(-5, -5), (10, 10)] {
        let mut conn = fresh().await;
        conn.pragma_update("user_version", version).await.unwrap();
        match TWO.to_latest(&mut conn).await {
            Err((0, MigrationError::InvalidUserVersion(v, 2))) => assert_eq!(v, expected),
            other => panic!("expected InvalidUserVersion, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_failing_migration_leaves_the_version_behind_it() {
    const BROKEN: Migrations = Migrations::new(&[
        Migration::up("001", "CREATE TABLE ok (id int)"),
        Migration::up("002", "SELECT * FROM non_existent_table"),
    ]);
    let mut conn = fresh().await;
    match BROKEN.to_latest(&mut conn).await {
        Err((1, MigrationError::Turso(_))) => {}
        other => panic!("expected a Turso error after one migration, got {other:?}"),
    }
    assert_eq!(get_user_version(&conn).await.unwrap(), 1);
}

#[tokio::test]
async fn the_crate_schema_applies_to_a_fresh_database() {
    let conn = MIGRATIONS.run_all_in_memory().await.unwrap();
    assert_eq!(
        get_user_version(&conn).await.unwrap(),
        MIGRATIONS.len() as i32
    );
    for table in ["event", "snapshot", "subscriber", "meta"] {
        conn.execute(&format!("SELECT * FROM {table} LIMIT 0"), ())
            .await
            .unwrap_or_else(|err| panic!("table {table} missing: {err}"));
    }
    #[cfg(feature = "accord")]
    for table in [
        "accord_commands",
        "accord_meta",
        "accord_metadata_log",
        "accord_acceptors",
    ] {
        conn.execute(&format!("SELECT * FROM {table} LIMIT 0"), ())
            .await
            .unwrap_or_else(|err| panic!("table {table} missing: {err}"));
    }
}

#[tokio::test]
async fn open_reopens_a_migrated_file_without_reapplying() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let first = evento_turso::Turso::open(&path).await.unwrap();
    drop(first);
    // A second open finds user_version at the latest and applies nothing.
    let second = evento_turso::Turso::open(&path).await.unwrap();
    let conn = second.database().connect().unwrap();
    assert_eq!(
        get_user_version(&conn).await.unwrap(),
        MIGRATIONS.len() as i32
    );
}
