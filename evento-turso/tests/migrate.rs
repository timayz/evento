//! The migration runner: per-app records, idempotency, resuming, a failing
//! migration, refusing an unknown or mismatched history, several apps in one
//! database, and the crate's own lists.

use evento_turso::migrate::{Migration, MigrationError, Migrations};
use evento_turso::{up_fn, MIGRATIONS};

const TWO: Migrations = Migrations::new(
    "test",
    &[
        Migration::up("001_friend", "CREATE TABLE friend(name TEXT NOT NULL);"),
        up_fn!("002_users", users),
    ],
);

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
    assert_eq!(
        TWO.applied(&conn).await.unwrap(),
        vec!["001_friend".to_string(), "002_users".to_string()]
    );
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
async fn resumes_after_the_recorded_migrations() {
    const ONE: Migrations = Migrations::new(
        "test",
        &[Migration::up(
            "001_friend",
            "CREATE TABLE friend(name TEXT NOT NULL);",
        )],
    );
    let mut conn = fresh().await;
    assert_eq!(ONE.to_latest(&mut conn).await.unwrap(), 1);
    // The list grew by one: only the new migration runs.
    assert_eq!(TWO.to_latest(&mut conn).await.unwrap(), 1);
    assert_eq!(TWO.applied(&conn).await.unwrap().len(), 2);
}

#[tokio::test]
async fn refuses_a_database_ahead_of_the_list() {
    const ONE: Migrations = Migrations::new(
        "test",
        &[Migration::up(
            "001_friend",
            "CREATE TABLE friend(name TEXT NOT NULL);",
        )],
    );
    let mut conn = fresh().await;
    TWO.to_latest(&mut conn).await.unwrap();
    match ONE.to_latest(&mut conn).await {
        Err((
            0,
            MigrationError::Unknown {
                app,
                applied: 2,
                known: 1,
            },
        )) => assert_eq!(app, "test"),
        other => panic!("expected Unknown, got {other:?}"),
    }
}

#[tokio::test]
async fn refuses_a_renamed_or_reordered_migration() {
    const RENAMED: Migrations = Migrations::new(
        "test",
        &[
            Migration::up("001_friends", "CREATE TABLE friend(name TEXT NOT NULL);"),
            up_fn!("002_users", users),
        ],
    );
    let mut conn = fresh().await;
    TWO.to_latest(&mut conn).await.unwrap();
    match RENAMED.to_latest(&mut conn).await {
        Err((
            0,
            MigrationError::Mismatch {
                position: 1,
                recorded,
                expected,
                ..
            },
        )) => {
            assert_eq!(recorded, "001_friend");
            assert_eq!(expected, "001_friends");
        }
        other => panic!("expected Mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn a_failing_migration_records_only_what_succeeded() {
    const BROKEN: Migrations = Migrations::new(
        "test",
        &[
            Migration::up("001_ok", "CREATE TABLE ok (id int)"),
            Migration::up("002_broken", "SELECT * FROM non_existent_table"),
        ],
    );
    let mut conn = fresh().await;
    match BROKEN.to_latest(&mut conn).await {
        Err((1, MigrationError::Turso(_))) => {}
        other => panic!("expected a Turso error after one migration, got {other:?}"),
    }
    assert_eq!(
        BROKEN.applied(&conn).await.unwrap(),
        vec!["001_ok".to_string()]
    );
}

#[tokio::test]
async fn apps_are_independent_namespaces() {
    const APP: Migrations = Migrations::new(
        "my-app",
        &[Migration::up(
            "001_friend",
            "CREATE TABLE app_friend(name TEXT NOT NULL);",
        )],
    );
    // evento's schema, then the app's, in one database.
    let store = evento_turso::Turso::in_memory().await.unwrap();
    let mut conn = store.database().connect().unwrap();
    assert_eq!(APP.to_latest(&mut conn).await.unwrap(), 1);
    assert_eq!(MIGRATIONS.to_latest(&mut conn).await.unwrap(), 0);
    assert_eq!(APP.applied(&conn).await.unwrap().len(), 1);
    assert_eq!(
        MIGRATIONS.applied(&conn).await.unwrap().len(),
        MIGRATIONS.len()
    );
    // Same names under another app are a separate history.
    const OTHER: Migrations = Migrations::new(
        "other-app",
        &[Migration::up(
            "001_friend",
            "CREATE TABLE other_friend(name TEXT NOT NULL);",
        )],
    );
    assert_eq!(OTHER.to_latest(&mut conn).await.unwrap(), 1);
}

#[tokio::test]
async fn the_crate_schema_applies_to_a_fresh_database() {
    let conn = MIGRATIONS.run_all_in_memory().await.unwrap();
    assert_eq!(
        MIGRATIONS.applied(&conn).await.unwrap().len(),
        MIGRATIONS.len()
    );
    for table in ["event", "snapshot", "subscriber", "meta"] {
        conn.execute(&format!("SELECT * FROM {table} LIMIT 0"), ())
            .await
            .unwrap_or_else(|err| panic!("table {table} missing: {err}"));
    }
}

#[cfg(feature = "accord")]
#[tokio::test]
async fn the_journal_schema_applies_next_to_the_store_schema() {
    let store = evento_turso::Turso::in_memory().await.unwrap();
    let mut conn = store.database().connect().unwrap();
    assert_eq!(
        evento_turso::ACCORD_MIGRATIONS
            .to_latest(&mut conn)
            .await
            .unwrap(),
        1
    );
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
    // A second open finds every migration recorded and applies nothing.
    let second = evento_turso::Turso::open(&path).await.unwrap();
    let conn = second.database().connect().unwrap();
    assert_eq!(
        MIGRATIONS.applied(&conn).await.unwrap().len(),
        MIGRATIONS.len()
    );
}
