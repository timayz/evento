//! A read model on Turso, shaped like an application's admin view: its own
//! table from an app migration next to evento's, a `#[evento::projection]`
//! struct decoded with the `FromRow` derive, and keyset pages through
//! `Reader` over the `Cursor` derive's named cursors.

#![allow(missing_docs)]

use evento::cursor::Args;
use evento::cursor::Cursor as _;
use evento::turso::{
    self,
    migrate::{Migration, Migrations},
    Reader, Text, Turso,
};
use evento::Cursor;
use sea_query::{Expr, ExprTrait, Iden, OnConflict, Query};

#[derive(Iden, Clone, Copy)]
pub enum UserAdmin {
    Table,
    Id,
    Cursor,
    AggregateVersion,
    Tenant,
    Email,
    Role,
    TotalRecipesCount,
    CreatedAt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    #[default]
    User,
    Chef,
    Admin,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::User => "user",
            Role::Chef => "chef",
            Role::Admin => "admin",
        })
    }
}

impl std::str::FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "user" => Ok(Role::User),
            "chef" => Ok(Role::Chef),
            "admin" => Ok(Role::Admin),
            other => Err(format!("unknown role {other:?}")),
        }
    }
}

/// The application's own migrations, under its own app name.
const MIGRATIONS: Migrations = Migrations::new(
    "read-model-test",
    &[Migration::up(
        "001_user_admin",
        "CREATE TABLE user_admin (
            id TEXT NOT NULL PRIMARY KEY,
            cursor TEXT NOT NULL,
            aggregate_version INTEGER NOT NULL,
            tenant TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL,
            role TEXT NOT NULL,
            total_recipes_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );",
    )],
);

#[evento::projection(backend = turso, turso::FromRow, Cursor, Debug)]
pub struct AdminView {
    #[cursor(by_recently_joined, UserAdmin::Id, 1)]
    #[cursor(by_name, UserAdmin::Id, 1)]
    #[cursor(by_most_recipes, UserAdmin::Id, 1)]
    pub id: String,
    #[cursor(by_name, UserAdmin::Email, 2)]
    pub email: String,
    pub role: Text<Role>,
    #[cursor(by_most_recipes, UserAdmin::TotalRecipesCount, 2)]
    pub total_recipes_count: i64,
    #[cursor(by_recently_joined, UserAdmin::CreatedAt, 2)]
    pub created_at: u64,
    /// Not a column: set by a handler so the next snapshot removes the row.
    #[turso(default)]
    pub deleted: bool,
}

const COLUMNS: [UserAdmin; 8] = [
    UserAdmin::Id,
    UserAdmin::Cursor,
    UserAdmin::AggregateVersion,
    UserAdmin::Tenant,
    UserAdmin::Email,
    UserAdmin::Role,
    UserAdmin::TotalRecipesCount,
    UserAdmin::CreatedAt,
];

fn select() -> sea_query::SelectStatement {
    Query::select()
        .columns(COLUMNS)
        .from(UserAdmin::Table)
        .to_owned()
}

/// `(id, email, role, recipes, created_at)` of the seeded admins.
const ADMINS: [(&str, &str, Role, i64, u64); 6] = [
    ("u1", "zoe@example.com", Role::Admin, 12, 100),
    ("u2", "adam@example.com", Role::User, 3, 600),
    ("u3", "mia@example.com", Role::Chef, 40, 300),
    ("u4", "bob@example.com", Role::User, 3, 500),
    ("u5", "eve@example.com", Role::Chef, 7, 200),
    ("u6", "liam@example.com", Role::User, 0, 400),
];

async fn seeded() -> anyhow::Result<(Turso, turso::turso::Connection)> {
    let store = Turso::in_memory().await?;
    let mut conn = store.connect()?;
    MIGRATIONS
        .to_latest(&mut conn)
        .await
        .map_err(|(_, err)| err)?;
    // evento's own list is untouched by the app's, and vice versa.
    assert_eq!(turso::MIGRATIONS.to_latest(&mut conn).await.unwrap(), 0);

    for (id, email, role, recipes, created_at) in ADMINS {
        let view = AdminView {
            id: id.into(),
            email: email.into(),
            role: Text(role),
            total_recipes_count: recipes,
            created_at,
            deleted: false,
            cursor: format!("cursor-{id}"),
            aggregate_version: 1,
            tenant: String::new(),
        };
        upsert(&conn, &view).await?;
    }
    Ok((store, conn))
}

/// What a `Snapshot::take_snapshot` does: upsert the row.
async fn upsert(conn: &turso::turso::Connection, view: &AdminView) -> anyhow::Result<()> {
    let statement = Query::insert()
        .into_table(UserAdmin::Table)
        .columns(COLUMNS)
        .values_panic([
            view.id.clone().into(),
            view.cursor.clone().into(),
            view.aggregate_version.into(),
            view.tenant.clone().into(),
            view.email.clone().into(),
            view.role.into(),
            view.total_recipes_count.into(),
            view.created_at.into(),
        ])
        .on_conflict(
            OnConflict::column(UserAdmin::Id)
                .update_columns([
                    UserAdmin::Cursor,
                    UserAdmin::AggregateVersion,
                    UserAdmin::Email,
                    UserAdmin::Role,
                    UserAdmin::TotalRecipesCount,
                ])
                .to_owned(),
        )
        .to_owned();
    turso::execute(conn, &statement).await?;
    Ok(())
}

fn ids<O: std::ops::Deref<Target = AdminView>>(page: &evento::cursor::ReadResult<O>) -> Vec<&str> {
    page.edges.iter().map(|e| e.node.id.as_str()).collect()
}

#[tokio::test]
async fn a_row_decodes_through_the_derive() -> anyhow::Result<()> {
    let (_store, conn) = seeded().await?;

    // What a `Snapshot::restore` does: one row by id.
    let statement = select()
        .and_where(Expr::col(UserAdmin::Id).eq("u3"))
        .limit(1)
        .to_owned();
    let view: AdminView = turso::fetch_one(&conn, &statement).await?;
    assert_eq!(view.email, "mia@example.com");
    assert_eq!(*view.role, Role::Chef);
    assert_eq!(view.total_recipes_count, 40);
    assert_eq!(view.created_at, 300);
    assert_eq!(view.cursor, "cursor-u3");
    assert_eq!(view.aggregate_version, 1);
    assert_eq!(view.tenant, "");
    assert!(!view.deleted, "a column the select leaves out defaults");

    let missing: Option<AdminView> = turso::fetch_optional(
        &conn,
        &select()
            .and_where(Expr::col(UserAdmin::Id).eq("nobody"))
            .to_owned(),
    )
    .await?;
    assert!(missing.is_none());

    // An upsert updates in place: the same statement an app's take_snapshot runs.
    let mut view = view;
    view.role = Text(Role::Admin);
    view.aggregate_version = 2;
    upsert(&conn, &view).await?;
    let again: AdminView = turso::fetch_one(
        &conn,
        &select()
            .and_where(Expr::col(UserAdmin::Id).eq("u3"))
            .to_owned(),
    )
    .await?;
    assert_eq!(*again.role, Role::Admin);
    assert_eq!(again.aggregate_version, 2);
    let all: Vec<AdminView> = turso::fetch_all(&conn, &select()).await?;
    assert_eq!(all.len(), 6);

    // A text column that does not parse is an error naming the column.
    turso::execute(
        &conn,
        &Query::update()
            .table(UserAdmin::Table)
            .values([(UserAdmin::Role, "wizard".into())])
            .and_where(Expr::col(UserAdmin::Id).eq("u1"))
            .to_owned(),
    )
    .await?;
    let err = turso::fetch_one::<AdminView, _>(
        &conn,
        &select()
            .and_where(Expr::col(UserAdmin::Id).eq("u1"))
            .to_owned(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("role"), "{err}");
    assert!(err.to_string().contains("wizard"), "{err}");
    Ok(())
}

#[tokio::test]
async fn pages_by_name_forward_and_backward() -> anyhow::Result<()> {
    let (_store, conn) = seeded().await?;

    // Ascending by email: adam, bob, eve, liam, mia, zoe.
    let first = Reader::new(select())
        .forward(4, None)
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert_eq!(ids(&first), ["u2", "u4", "u5", "u6"]);
    assert!(first.page_info.has_next_page);
    assert!(!first.page_info.has_previous_page);

    let second = Reader::new(select())
        .forward(4, first.page_info.end_cursor.clone())
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert_eq!(ids(&second), ["u3", "u1"]);
    assert!(!second.page_info.has_next_page);

    // Backward from the second page's start: the last two before it, in
    // ascending order, with more before them.
    let before = Reader::new(select())
        .backward(2, second.page_info.start_cursor.clone())
        .execute::<AdminViewByName>(&conn)
        .await?;
    assert_eq!(ids(&before), ["u5", "u6"]);
    assert!(before.page_info.has_previous_page);
    assert!(!before.page_info.has_next_page);

    // The reader's filters are the caller's: a WHERE narrows the page.
    let chefs = Reader::new(
        select()
            .and_where(Expr::col(UserAdmin::Role).eq(Role::Chef.to_string()))
            .to_owned(),
    )
    .forward(10, None)
    .execute::<AdminViewByName>(&conn)
    .await?;
    assert_eq!(ids(&chefs), ["u5", "u3"]);
    Ok(())
}

#[tokio::test]
async fn pages_descending_by_a_tie_breaking_cursor() -> anyhow::Result<()> {
    let (_store, conn) = seeded().await?;

    // Most recipes first; u2 and u4 tie on 3 and fall back to the id.
    let first = Reader::new(select())
        .desc()
        .forward(3, None)
        .execute::<AdminViewByMostRecipes>(&conn)
        .await?;
    assert_eq!(ids(&first), ["u3", "u1", "u5"]);
    assert!(first.page_info.has_next_page);

    let second = Reader::new(select())
        .desc()
        .forward(3, first.page_info.end_cursor.clone())
        .execute::<AdminViewByMostRecipes>(&conn)
        .await?;
    assert_eq!(ids(&second), ["u4", "u2", "u6"]);
    assert!(!second.page_info.has_next_page);

    // Most recently joined, as `Args` from a request.
    let recent = Reader::new(select())
        .desc()
        .args(Args::forward(2, None))
        .execute::<AdminViewByRecentlyJoined>(&conn)
        .await?;
    assert_eq!(ids(&recent), ["u2", "u4"]);
    let recent_cursor: AdminViewByRecentlyJoinedCursor = recent.edges[1].node.serialize();
    assert_eq!(recent_cursor.i, "u4");
    assert_eq!(recent_cursor.c, 500);
    Ok(())
}
