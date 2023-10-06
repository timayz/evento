use chrono::{DateTime, Utc};
use evento_query::{Cursor, CursorOrder};
use serde::Deserialize;
use uuid::Uuid;

#[tokio::test]
async fn pg_filter_asc_forward() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Asc, false, None, None),
        "created_at > $1 OR (created_at = $1 AND (text > $2 OR (text = $2 AND id > $3)))"
    );
}

#[tokio::test]
async fn pg_filter_asc_backward() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Asc, true, None, None),
        "created_at < $1 OR (created_at = $1 AND (text < $2 OR (text = $2 AND id < $3)))"
    );
}

#[tokio::test]
async fn pg_filter_desc_forward() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Desc, false, None, None),
        "created_at < $1 OR (created_at = $1 AND (text < $2 OR (text = $2 AND id < $3)))"
    );
}

#[tokio::test]
async fn pg_filter_desc_backward() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Desc, true, None, None),
        "created_at > $1 OR (created_at = $1 AND (text > $2 OR (text = $2 AND id > $3)))"
    );
}

#[tokio::test]
async fn pg_filter_asc_forward_pos() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Asc, false, None, Some(2)),
        "created_at > $2 OR (created_at = $2 AND (text > $3 OR (text = $3 AND id > $4)))"
    );

    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Asc, false, None, Some(3)),
        "created_at > $3 OR (created_at = $3 AND (text > $4 OR (text = $4 AND id > $5)))"
    );
}

#[tokio::test]
async fn pg_filter_asc_backward_pos() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Asc, true, None, Some(2)),
        "created_at < $2 OR (created_at = $2 AND (text < $3 OR (text = $3 AND id < $4)))"
    );

    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Asc, true, None, Some(3)),
        "created_at < $3 OR (created_at = $3 AND (text < $4 OR (text = $4 AND id < $5)))"
    );
}

#[tokio::test]
async fn pg_filter_desc_forward_pos() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Desc, false, None, Some(2)),
        "created_at < $2 OR (created_at = $2 AND (text < $3 OR (text = $3 AND id < $4)))"
    );

    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Desc, false, None, Some(3)),
        "created_at < $3 OR (created_at = $3 AND (text < $4 OR (text = $4 AND id < $5)))"
    );
}

#[tokio::test]
async fn pg_filter_desc_backward_pos() {
    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Desc, true, None, Some(2)),
        "created_at > $2 OR (created_at = $2 AND (text > $3 OR (text = $3 AND id > $4)))"
    );

    assert_eq!(
        Todo::to_pg_filter_opts(&CursorOrder::Desc, true, None, Some(3)),
        "created_at > $3 OR (created_at = $3 AND (text > $4 OR (text = $4 AND id > $5)))"
    );
}

#[tokio::test]
async fn pg_order_asc_forward() {
    assert_eq!(
        Todo::to_pg_order(&CursorOrder::Asc, false),
        "created_at ASC, text ASC, id ASC"
    );
}

#[tokio::test]
async fn pg_order_asc_backward() {
    assert_eq!(
        Todo::to_pg_order(&CursorOrder::Asc, true),
        "created_at DESC, text DESC, id DESC"
    );
}

#[tokio::test]
async fn pg_order_desc_forward() {
    assert_eq!(
        Todo::to_pg_order(&CursorOrder::Desc, false),
        "created_at DESC, text DESC, id DESC"
    );
}

#[tokio::test]
async fn pg_order_desc_backward() {
    assert_eq!(
        Todo::to_pg_order(&CursorOrder::Desc, true),
        "created_at ASC, text ASC, id ASC"
    );
}

#[derive(Clone, Deserialize, Debug, sqlx::FromRow, Default, PartialEq)]
pub struct Todo {
    pub id: Uuid,
    pub text: String,
    pub done: bool,
    pub created_at: DateTime<Utc>,
}

impl Cursor for Todo {
    fn keys() -> Vec<&'static str> {
        vec!["created_at", "text", "id"]
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
        query.bind(self.created_at).bind(self.text).bind(self.id)
    }

    fn serialize(&self) -> Vec<String> {
        vec![
            Self::serialize_utc(self.created_at),
            self.text.to_owned(),
            self.id.to_string(),
        ]
    }

    fn deserialize(values: Vec<&str>) -> Result<Self, evento_query::QueryError> {
        let mut values = values.iter();
        let created_at = Self::deserialize_as_utc("created_at", values.next())?;
        let text = Self::deserialize_as("text", values.next())?;
        let id = Self::deserialize_as("id", values.next())?;

        Ok(Todo {
            id,
            text,
            created_at,
            ..Default::default()
        })
    }
}
