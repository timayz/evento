#![allow(clippy::needless_return)]
mod store;

use std::{io, path::Path, time::Duration};

use evento_store::PgStore;
use futures_util::{Future, TryFutureExt};
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    Any, PgPool,
};
use tokio::sync::OnceCell;

static POOL: OnceCell<PgPool> = OnceCell::const_new();

pub async fn get_pool() -> &'static PgPool {
    POOL.get_or_init(|| async {
        let dsn = "postgres://postgres:postgres@localhost:5432/evento_test_store";
        let exists = retry_connect_errors(dsn, Any::database_exists)
            .await
            .unwrap();

        if exists {
            Any::drop_database(dsn).await.unwrap();
        }

        Any::create_database(dsn).await.unwrap();

        let pool = PgPool::connect(dsn).await.unwrap();

        Migrator::new(Path::new("./tests/fixtures/db"))
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
async fn concurrency() {
    let pool = get_pool().await;
    let store = PgStore::with_prefix(pool, "concurrency");
    store::init(&store).await.unwrap();
    store::test_concurrency(&store).await.unwrap();
}

#[tokio_shared_rt::test]
async fn save() {
    let pool = get_pool().await;
    let store = PgStore::with_prefix(pool, "save");
    store::init(&store).await.unwrap();
    store::test_save(&store).await.unwrap();
}

#[tokio_shared_rt::test]
async fn wrong_version() {
    let pool = get_pool().await;
    let store = PgStore::with_prefix(pool, "wrong_version");
    store::init(&store).await.unwrap();
    store::test_wrong_version(&store).await.unwrap();
}

#[tokio_shared_rt::test]
async fn insert() {
    let pool = get_pool().await;
    let store = PgStore::with_prefix(pool, "insert");
    store::init(&store).await.unwrap();
    store::test_insert(&store).await.unwrap();
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
