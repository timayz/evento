use chrono::{DateTime, Utc};
use evento_query::Cursor;
use futures_util::{Future, TryFutureExt};
use serde::Deserialize;
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    Any, PgPool,
};
use std::{io, path::Path, time::Duration};
use tokio::sync::OnceCell;
use uuid::Uuid;

static POOL: OnceCell<PgPool> = OnceCell::const_new();

pub async fn get_pool(path: &str) -> &'static PgPool {
    POOL.get_or_init(|| async {
        let dsn = "postgres://postgres:postgres@localhost:5432/evento_query_test";
        let exists = retry_connect_errors(dsn, Any::database_exists)
            .await
            .unwrap();

        if exists {
            Any::drop_database(dsn).await.unwrap();
        }

        Any::create_database(dsn).await.unwrap();

        let pool = PgPool::connect(dsn).await.unwrap();

        Migrator::new(Path::new(path))
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();

        pool
    })
    .await
}

/// Attempt an operation that may return errors like `ConnectionRefused`,
/// retrying up until `ops.connect_timeout`.
///
/// The closure is passed `&ops.database_url` for easy composition.
async fn retry_connect_errors<'a, F, Fut, T>(
    database_url: &'a str,
    mut connect: F,
) -> sqlx::Result<T>
where
    F: FnMut(&'a str) -> Fut,
    Fut: Future<Output = sqlx::Result<T>> + 'a,
{
    sqlx::any::install_default_drivers();

    backoff::future::retry(
        backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(10)))
            .build(),
        || {
            connect(database_url).map_err(|e| -> backoff::Error<sqlx::Error> {
                if let sqlx::Error::Io(ref ioe) = e {
                    match ioe.kind() {
                        io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted => {
                            return backoff::Error::transient(e);
                        }
                        _ => (),
                    }
                }

                backoff::Error::permanent(e)
            })
        },
    )
    .await
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
