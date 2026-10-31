//! Turso's full-text search behind the `fts` feature: an index on a read
//! model's table, a `MATCH` filter inside a `Reader` page — the shape of an
//! admin search — and `fts_score` ranking.

#![cfg(feature = "fts")]
#![allow(missing_docs)]

use evento::cursor::Cursor as _;
use evento::turso::{
    self,
    migrate::{Migration, Migrations},
    Reader, Turso,
};
use evento::Cursor;
use sea_query::{Expr, ExprTrait, Iden, Query};

#[derive(Iden, Clone, Copy)]
pub enum UserAdmin {
    Table,
    Id,
    Cursor,
    AggregateVersion,
    Tenant,
    Email,
    FullName,
}

const MIGRATIONS: Migrations = Migrations::new(
    "fts-test",
    &[
        Migration::up(
            "001_user_admin",
            "CREATE TABLE user_admin (
                id TEXT NOT NULL PRIMARY KEY,
                cursor TEXT NOT NULL,
                aggregate_version INTEGER NOT NULL,
                tenant TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL,
                full_name TEXT NOT NULL
            );",
        ),
        Migration::up(
            "002_user_admin_fts",
            "CREATE INDEX user_admin_fts ON user_admin USING fts(email, full_name);",
        ),
    ],
);

#[evento::projection(backend = turso, turso::FromRow, Cursor, Debug)]
pub struct AdminView {
    #[cursor(by_name, UserAdmin::Id, 1)]
    pub id: String,
    #[cursor(by_name, UserAdmin::Email, 2)]
    pub email: String,
    pub full_name: String,
}

const COLUMNS: [UserAdmin; 6] = [
    UserAdmin::Id,
    UserAdmin::Cursor,
    UserAdmin::AggregateVersion,
    UserAdmin::Tenant,
    UserAdmin::Email,
    UserAdmin::FullName,
];

const ADMINS: [(&str, &str, &str); 5] = [
    ("u1", "ada@example.com", "Ada Lovelace"),
    ("u2", "grace@example.com", "Grace Hopper"),
    ("u3", "linus@example.com", "Linus Torvalds"),
    ("u4", "ada.king@example.com", "Ada King"),
    ("u5", "hedy@example.com", "Hedy Lamarr"),
];

async fn seeded() -> anyhow::Result<turso::turso::Connection> {
    let store = Turso::in_memory().await?;
    let mut conn = store.connect()?;
    MIGRATIONS
        .to_latest(&mut conn)
        .await
        .map_err(|(_, err)| err)?;
    for (id, email, name) in ADMINS {
        turso::execute(
            &conn,
            &Query::insert()
                .into_table(UserAdmin::Table)
                .columns(COLUMNS)
                .values_panic([
                    id.into(),
                    format!("cursor-{id}").into(),
                    1.into(),
                    "".into(),
                    email.into(),
                    name.into(),
                ])
                .to_owned(),
        )
        .await?;
    }
    // The store keeps the connection's database alive through the test.
    std::mem::forget(store);
    Ok(conn)
}

fn ids(page: &evento::cursor::ReadResult<AdminViewByName>) -> Vec<&str> {
    page.edges.iter().map(|e| e.node.id.as_str()).collect()
}

/// A search narrows a `Reader` page through an FTS subquery, as an admin
/// search does, and the page keeps its own order and cursors.
#[tokio::test]
async fn a_match_subquery_narrows_a_page() -> anyhow::Result<()> {
    let conn = seeded().await?;

    let search = |words: &str| {
        Query::select()
            .columns(COLUMNS)
            .from(UserAdmin::Table)
            .and_where(
                Expr::col(UserAdmin::Id).in_subquery(
                    Query::select()
                        .column(UserAdmin::Id)
                        .from(UserAdmin::Table)
                        .and_where(Expr::cust_with_values(
                            "(email, full_name) MATCH ?",
                            [words],
                        ))
                        .limit(20)
                        .take(),
                ),
            )
            .to_owned()
    };

    let adas = Reader::new(search("ada"))
        .forward(10, None)
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert_eq!(
        ids(&adas),
        ["u4", "u1"],
        "ascending by email within the matches"
    );
    assert!(!adas.page_info.has_next_page);

    let first = Reader::new(search("ada"))
        .forward(1, None)
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert_eq!(ids(&first), ["u4"]);
    assert!(first.page_info.has_next_page);
    let second = Reader::new(search("ada"))
        .forward(1, first.page_info.end_cursor.clone())
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert_eq!(ids(&second), ["u1"]);
    let cursor: AdminViewByNameCursor = second.edges[0].node.serialize();
    assert_eq!(cursor.e, "ada@example.com");

    let none = Reader::new(search("nobody"))
        .forward(10, None)
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert!(none.edges.is_empty());
    Ok(())
}

/// `fts_match` filters and `fts_score` ranks: the matches come back in
/// score order, and a term only its document has hits only that document.
#[tokio::test]
async fn fts_score_ranks_matches() -> anyhow::Result<()> {
    let conn = seeded().await?;
    let ranked = |words: &'static str| {
        let conn = conn.clone();
        async move {
            // `fts_score` / `fts_match` name the index's columns, all of them,
            // then the query: that is the shape the planner serves from the
            // index (a subset of the columns matches, but scores nothing).
            let mut rows = conn
                .query(
                    "SELECT id, fts_score(email, full_name, ?1) AS score FROM user_admin \
                     WHERE fts_match(email, full_name, ?1) ORDER BY score DESC",
                    [words],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.get::<String>(0)?, row.get::<f64>(1)?));
            }
            anyhow::Ok(out)
        }
    };

    let adas = ranked("ada king").await?;
    let ids: Vec<&str> = adas.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids.len(), 2, "{adas:?}");
    assert!(ids.contains(&"u1") && ids.contains(&"u4"), "{adas:?}");
    assert!(
        adas.windows(2).all(|pair| pair[0].1 >= pair[1].1),
        "scores descend: {adas:?}"
    );
    assert!(adas.iter().all(|(_, score)| *score > 0.0), "{adas:?}");

    let kings = ranked("king").await?;
    assert_eq!(kings.len(), 1, "{kings:?}");
    assert_eq!(kings[0].0, "u4");
    Ok(())
}
