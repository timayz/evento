use evento_store::{Aggregate, Engine, Event, EventStore, PgEngine};
use futures_util::{Future, TryFutureExt};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    Any, PgPool,
};
use std::{io, path::Path, time::Duration};


pub async fn create_pg_store(
    table_prefix: Option<&str>,
    init: bool,
) -> (EventStore<PgEngine>, PgPool) {
    let dsn = "postgres://postgres:postgres@localhost:5432/evento_test";
    let exists = retry_connect_errors(dsn, Any::database_exists)
        .await
        .unwrap();

    if !exists {
        let _ = Any::create_database(dsn).await;
    }

    let pool = PgPool::connect(dsn).await.unwrap();

    Migrator::new(Path::new("../migrations"))
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();

    let table_events = get_table_name(table_prefix, "events");

    sqlx::query::<_>(format!("TRUNCATE {table_events};").as_str())
        .execute(&pool)
        .await
        .unwrap();

    let store = table_prefix.map_or(PgEngine::new(pool.clone()), |table_prefix| {
        PgEngine::new_prefix(pool.clone(), table_prefix)
    });

    if init {
        init_store(&store).await;
    }

    (store, pool)
}

fn get_table_name(table_prefix: Option<&str>, name: impl Into<String>) -> String {
    match table_prefix.as_ref() {
        Some(prefix) => format!("evento_{prefix}_{}", name.into()),
        _ => format!("evento_{}", name.into()),
    }
}

/// Attempt to connect to the database server, retrying up to `ops.connect_timeout`.
// async fn connect(database_url: &String) -> sqlx::Result<AnyConnection> {
//     retry_connect_errors(database_url, AnyConnection::connect).await
// }

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
