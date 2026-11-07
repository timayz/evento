//! The building blocks of a migration operation: a [`Step`] renders its
//! statements for a [`Backend`], and [`step_operation!`] makes it a
//! `sqlx_migrator::Operation` on every enabled backend.

/// The database an operation renders its statements for.
#[derive(Clone, Copy)]
pub(crate) enum Backend {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "mysql")]
    MySql,
    #[cfg(feature = "postgres")]
    Postgres,
}

impl Backend {
    /// Renders a schema statement (table or index DDL) for this backend.
    pub(crate) fn schema(self, statement: &impl sea_query::SchemaStatementBuilder) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => statement.to_string(sea_query::SqliteQueryBuilder),
            #[cfg(feature = "mysql")]
            Backend::MySql => statement.to_string(sea_query::MysqlQueryBuilder),
            #[cfg(feature = "postgres")]
            Backend::Postgres => statement.to_string(sea_query::PostgresQueryBuilder),
        }
    }
}

/// An operation made of plain statements, run in order.
pub(crate) trait Step: Send + Sync {
    /// The statements that apply the operation.
    fn up(&self, backend: Backend) -> Vec<String>;
    /// The statements that revert it.
    fn down(&self, backend: Backend) -> Vec<String>;
}

/// Implements `sqlx_migrator::Operation` for a [`Step`] on every enabled
/// backend.
macro_rules! step_operation {
    ($step:ty) => {
        #[cfg(feature = "sqlite")]
        #[async_trait::async_trait]
        impl sqlx_migrator::Operation<sqlx::Sqlite> for $step {
            async fn up(
                &self,
                connection: &mut sqlx::SqliteConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::step::Step::up(self, $crate::step::Backend::Sqlite) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }

            async fn down(
                &self,
                connection: &mut sqlx::SqliteConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::step::Step::down(self, $crate::step::Backend::Sqlite) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }
        }

        #[cfg(feature = "mysql")]
        #[async_trait::async_trait]
        impl sqlx_migrator::Operation<sqlx::MySql> for $step {
            async fn up(
                &self,
                connection: &mut sqlx::MySqlConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::step::Step::up(self, $crate::step::Backend::MySql) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }

            async fn down(
                &self,
                connection: &mut sqlx::MySqlConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::step::Step::down(self, $crate::step::Backend::MySql) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }
        }

        #[cfg(feature = "postgres")]
        #[async_trait::async_trait]
        impl sqlx_migrator::Operation<sqlx::Postgres> for $step {
            async fn up(
                &self,
                connection: &mut sqlx::PgConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::step::Step::up(self, $crate::step::Backend::Postgres) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }

            async fn down(
                &self,
                connection: &mut sqlx::PgConnection,
            ) -> Result<(), sqlx_migrator::Error> {
                for statement in $crate::step::Step::down(self, $crate::step::Backend::Postgres) {
                    sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            }
        }
    };
}

pub(crate) use step_operation;
