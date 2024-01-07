#![allow(clippy::needless_return)]
mod consumer;

use std::{io, path::Path, time::Duration};

use evento::PgConsumer;
use evento_store::PgStore;
use futures_util::{Future, TryFutureExt};
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    Any, PgPool,
};
use tokio::sync::OnceCell;
use tracing_test::traced_test;

static POOL: OnceCell<PgPool> = OnceCell::const_new();

pub async fn get_pool() -> &'static PgPool {
    POOL.get_or_init(|| async {
        let dsn = "postgres://postgres:postgres@localhost:5432/evento_test";
        let exists = retry_connect_errors(dsn, Any::database_exists)
            .await
            .unwrap();

        if exists {
            Any::drop_database(dsn).await.unwrap();
        }

        Any::create_database(dsn).await.unwrap();

        let pool = PgPool::connect(dsn).await.unwrap();

        Migrator::new(Path::new("./tests/fixtures/pg"))
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();

        pool
    })
    .await
}

#[tokio_shared_rt::test]
#[traced_test]
async fn multiple_consumer() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("multiple_consumer");

    consumer::test_multiple_consumer(&consumer).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn no_cdc() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("no_cdc");

    consumer::test_no_cdc(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn no_cdc_with_name() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("no_cdc_with_name");

    consumer::test_no_cdc(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn filter() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("filter");

    consumer::test_filter(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn filter_with_name() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("filter_with_name");

    consumer::test_filter(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn deadletter() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("deadletter");

    consumer::test_deadletter(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn deadletter_with_name() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("deadletter_with_name");

    consumer::test_deadletter(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn external_store() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("external_store");
    let store = PgStore::with_prefix(pool, "external_store_ext");

    consumer::test_external_store(&consumer, &store, false)
        .await
        .unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn external_store_with_name() {
    let pool = get_pool().await;
    let consumer = PgConsumer::new(pool).prefix("external_store_with_name");
    let store = PgStore::with_prefix(pool, "external_store_with_name_ext");

    consumer::test_external_store(&consumer, &store, true)
        .await
        .unwrap();
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
