mod common;
use chrono::{DateTime, Utc};
use common::get_pool;
use evento_query::{Cursor, PageInfo, Query};
use serde::Deserialize;
use tokio::sync::OnceCell;
use uuid::Uuid;

static POOL_PATH: &str = "./tests/fixtures/query";
static SELECT_USERS: &str = "SELECT * FROM users";
static ONE: OnceCell<Vec<User>> = OnceCell::const_new();

async fn get_users() -> &'static Vec<User> {
    ONE.get_or_init(|| async {
        let db = get_pool(POOL_PATH).await;
        sqlx::query_as::<_, User>("SELECT * FROM users ORDER BY created_at ASC, age ASC, id ASC")
            .fetch_all(db)
            .await
            .unwrap()
    })
    .await
}

#[tokio::test]
async fn query_first() {
    let db = common::get_pool("./tests/fixtures/query").await;
    let users = get_users().await;
    let query = Query::<User>::new(SELECT_USERS)
        .build(Default::default())
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 10);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: Some(query.edges[9].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[0]);
    assert_eq!(query.edges[1].node, users[1]);
    assert_eq!(query.edges[2].node, users[2]);
    assert_eq!(query.edges[3].node, users[3]);
    assert_eq!(query.edges[4].node, users[4]);
    assert_eq!(query.edges[5].node, users[5]);
    assert_eq!(query.edges[6].node, users[6]);
    assert_eq!(query.edges[7].node, users[7]);
    assert_eq!(query.edges[8].node, users[8]);
    assert_eq!(query.edges[9].node, users[9]);
}

#[tokio::test]
async fn query_first_3() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;
    let query = Query::<User>::new(SELECT_USERS)
        .forward(3, None)
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 3);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(query.edges[2].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[0]);
    assert_eq!(query.edges[1].node, users[1]);
    assert_eq!(query.edges[2].node, users[2]);
}

#[tokio::test]
async fn query_first_2_after_3() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;

    let query = Query::<User>::new(SELECT_USERS)
        .forward(2, Some(users[2].to_cursor()))
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 2);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(query.edges[1].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[3]);
    assert_eq!(query.edges[1].node, users[4]);
}

#[tokio::test]
async fn query_first_2_after_9() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;

    let query = Query::<User>::new(SELECT_USERS)
        .forward(2, Some(users[8].to_cursor()))
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 1);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: Some(query.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[9]);
}

#[tokio::test]
async fn query_first_3_after_5() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;

    let query = Query::<User>::new(SELECT_USERS)
        .forward(3, Some(users[4].to_cursor()))
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 3);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(query.edges[2].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[5]);
    assert_eq!(query.edges[1].node, users[6]);
    assert_eq!(query.edges[2].node, users[7]);
}

#[tokio::test]
async fn query_last() {
    let db = common::get_pool("./tests/fixtures/query").await;
    let users = get_users().await;
    let query = Query::<User>::new(SELECT_USERS)
        .backward(20, None)
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 10);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: Some(query.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[0]);
    assert_eq!(query.edges[1].node, users[1]);
    assert_eq!(query.edges[2].node, users[2]);
    assert_eq!(query.edges[3].node, users[3]);
    assert_eq!(query.edges[4].node, users[4]);
    assert_eq!(query.edges[5].node, users[5]);
    assert_eq!(query.edges[6].node, users[6]);
    assert_eq!(query.edges[7].node, users[7]);
    assert_eq!(query.edges[8].node, users[8]);
    assert_eq!(query.edges[9].node, users[9]);
}

#[tokio::test]
async fn query_last_3() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;
    let query = Query::<User>::new(SELECT_USERS)
        .backward(3, None)
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 3);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(query.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[7]);
    assert_eq!(query.edges[1].node, users[8]);
    assert_eq!(query.edges[2].node, users[9]);
}

#[tokio::test]
async fn query_last_2_before_4() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;

    let query = Query::<User>::new(SELECT_USERS)
        .backward(2, Some(users[3].to_cursor()))
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 2);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(query.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[1]);
    assert_eq!(query.edges[1].node, users[2]);
}

#[tokio::test]
async fn query_last_2_before_2() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;

    let query = Query::<User>::new(SELECT_USERS)
        .backward(2, Some(users[1].to_cursor()))
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 1);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: Some(query.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[0]);
}

#[tokio::test]
async fn query_last_3_before_8() {
    let db = common::get_pool(POOL_PATH).await;
    let users = get_users().await;

    let query = Query::<User>::new(SELECT_USERS)
        .backward(3, Some(users[8].to_cursor()))
        .fetch_all(db)
        .await
        .unwrap();

    assert_eq!(query.edges.len(), 3);
    assert_eq!(
        query.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(query.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(query.edges[0].node, users[5]);
    assert_eq!(query.edges[1].node, users[6]);
    assert_eq!(query.edges[2].node, users[7]);
}

#[derive(Clone, Deserialize, Debug, sqlx::FromRow, Default, PartialEq)]
pub struct User {
    pub id: Uuid,
    pub name: String,
    pub age: i32,
    pub created_at: DateTime<Utc>,
}

impl Cursor for User {
    fn keys() -> Vec<&'static str> {
        vec!["created_at", "age", "id"]
    }

    fn bind<'q, O>(
        self,
        query: sqlx::query::QueryAs<sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryAs<sqlx::Postgres, O, sqlx::postgres::PgArguments>
    where
        O: for<'r> sqlx::FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
        O: 'q + std::marker::Send,
        O: 'q + Unpin,
        O: 'q + Cursor,
    {
        query.bind(self.created_at).bind(self.age).bind(self.id)
    }

    fn serialize(&self) -> Vec<String> {
        vec![
            Self::serialize_utc(self.created_at),
            self.age.to_string(),
            self.id.to_string(),
        ]
    }

    fn deserialize(values: Vec<&str>) -> Result<Self, evento_query::QueryError> {
        let mut values = values.iter();
        let created_at = Self::deserialize_as_utc("created_at", values.next())?;
        let age = Self::deserialize_as("age", values.next())?;
        let id = Self::deserialize_as("id", values.next())?;

        Ok(User {
            id,
            age,
            created_at,
            ..Default::default()
        })
    }
}
