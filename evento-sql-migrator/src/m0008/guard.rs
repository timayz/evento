//! Refuses to revert `m0008` while a tenant other than the default one holds
//! events or subscriptions: the previous schema keys them by aggregate id and
//! subscription key alone, so reverting would merge tenants — or fail halfway
//! through, on a unique index two tenants' rows collide on.
//!
//! Listed last, so its `down` runs first and nothing is reverted unless it
//! passes. `up` does nothing.

/// The revert guard; see the module docs.
pub struct RevertGuard;

/// Counts the rows outside the default tenant, in `event` then `subscriber`.
const COUNT_EVENTS: &str = "SELECT COUNT(*) FROM event WHERE tenant <> ''";
const COUNT_SUBSCRIBERS: &str = "SELECT COUNT(*) FROM subscriber WHERE tenant <> ''";

/// The error a revert gets when tenants hold rows.
fn refused(events: i64, subscribers: i64) -> sqlx_migrator::Error {
    sqlx_migrator::Error::Box(
        format!(
            "m0008 cannot be reverted: {events} event(s) and {subscribers} subscription(s) \
             belong to a tenant other than the default one, which the previous schema \
             cannot tell apart"
        )
        .into(),
    )
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Sqlite> for RevertGuard {
    async fn up(
        &self,
        _connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let events: i64 = sqlx::query_scalar(COUNT_EVENTS)
            .fetch_one(&mut *connection)
            .await?;
        let subscribers: i64 = sqlx::query_scalar(COUNT_SUBSCRIBERS)
            .fetch_one(&mut *connection)
            .await?;
        match events + subscribers {
            0 => Ok(()),
            _ => Err(refused(events, subscribers)),
        }
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::MySql> for RevertGuard {
    async fn up(
        &self,
        _connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let events: i64 = sqlx::query_scalar(COUNT_EVENTS)
            .fetch_one(&mut *connection)
            .await?;
        let subscribers: i64 = sqlx::query_scalar(COUNT_SUBSCRIBERS)
            .fetch_one(&mut *connection)
            .await?;
        match events + subscribers {
            0 => Ok(()),
            _ => Err(refused(events, subscribers)),
        }
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl sqlx_migrator::Operation<sqlx::Postgres> for RevertGuard {
    async fn up(&self, _connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let events: i64 = sqlx::query_scalar(COUNT_EVENTS)
            .fetch_one(&mut *connection)
            .await?;
        let subscribers: i64 = sqlx::query_scalar(COUNT_SUBSCRIBERS)
            .fetch_one(&mut *connection)
            .await?;
        match events + subscribers {
            0 => Ok(()),
            _ => Err(refused(events, subscribers)),
        }
    }
}
