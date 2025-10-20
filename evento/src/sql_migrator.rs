use sea_query::{
    ColumnDef, Expr, Index, IndexCreateStatement, IndexDropStatement, Table, TableCreateStatement,
    TableDropStatement,
};
use sqlx_migrator::{vec_box, Info, Migrator, Operation};

use crate::sql::{Event, Snapshot, Subscriber};

pub struct CreateEventTable;

fn create_event_table_statement() -> TableCreateStatement {
    Table::create()
        .table(Event::Table)
        .col(
            ColumnDef::new(Event::Id)
                .string()
                .not_null()
                .string_len(26)
                .primary_key(),
        )
        .col(
            ColumnDef::new(Event::Name)
                .string()
                .string_len(20)
                .not_null(),
        )
        .col(
            ColumnDef::new(Event::AggregatorType)
                .string()
                .string_len(50)
                .not_null(),
        )
        .col(
            ColumnDef::new(Event::AggregatorId)
                .string()
                .string_len(26)
                .not_null(),
        )
        .col(ColumnDef::new(Event::Version).integer().not_null())
        .col(ColumnDef::new(Event::Data).blob().not_null())
        .col(ColumnDef::new(Event::Metadata).blob().not_null())
        .col(ColumnDef::new(Event::RoutingKey).string().string_len(50))
        .col(ColumnDef::new(Event::Timestamp).big_integer().not_null())
        .to_owned()
}

fn drop_event_table_statement() -> TableDropStatement {
    Table::drop().table(Event::Table).to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateEventTable {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_table_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_table_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateEventTable {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_table_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_table_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateEventTable {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_table_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_table_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct CreateEventTypeIdx;

fn create_event_type_idx_statement() -> IndexCreateStatement {
    Index::create()
        .name("idx_event_type")
        .table(Event::Table)
        .col(Event::AggregatorType)
        .to_owned()
}

fn drop_event_type_idx_statement() -> IndexDropStatement {
    Index::drop()
        .name("idx_event_type")
        .table(Event::Table)
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateEventTypeIdx {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_type_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_type_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateEventTypeIdx {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_type_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_type_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateEventTypeIdx {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_type_idx_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_type_idx_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct CreateEventTypeIdIdx;

fn create_event_type_id_idx_statement() -> IndexCreateStatement {
    Index::create()
        .name("idx_event_type_id")
        .table(Event::Table)
        .col(Event::AggregatorType)
        .col(Event::AggregatorId)
        .to_owned()
}

fn drop_event_type_id_idx_statement() -> IndexDropStatement {
    Index::drop()
        .name("idx_event_type_id")
        .table(Event::Table)
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateEventTypeIdIdx {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_event_type_id_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_type_id_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateEventTypeIdIdx {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_type_id_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_event_type_id_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateEventTypeIdIdx {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_event_type_id_idx_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_type_id_idx_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct CreateEventRoutingKeyTypeIdx;

fn create_event_routing_key_typw_idx_statement() -> IndexCreateStatement {
    Index::create()
        .name("idx_event_routing_key_type")
        .table(Event::Table)
        .col(Event::RoutingKey)
        .col(Event::AggregatorType)
        .to_owned()
}

fn drop_event_routing_key_type_idx_statement() -> IndexDropStatement {
    Index::drop()
        .name("idx_event_routing_key_type")
        .table(Event::Table)
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateEventRoutingKeyTypeIdx {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_event_routing_key_typw_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_routing_key_type_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateEventRoutingKeyTypeIdx {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_event_routing_key_typw_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_routing_key_type_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateEventRoutingKeyTypeIdx {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_routing_key_typw_idx_statement()
            .to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_routing_key_type_idx_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct CreateEventTypeIdVersionIdx;

fn create_event_typen_id_version_idx_statement() -> IndexCreateStatement {
    Index::create()
        .name("idx_event_type_id_version")
        .table(Event::Table)
        .unique()
        .col(Event::AggregatorType)
        .col(Event::AggregatorId)
        .col(Event::Version)
        .to_owned()
}

fn drop_event_type_id_verison_idx_statement() -> IndexDropStatement {
    Index::drop()
        .name("idx_event_type_id_version")
        .table(Event::Table)
        .to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateEventTypeIdVersionIdx {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_event_typen_id_version_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_type_id_verison_idx_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateEventTypeIdVersionIdx {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_event_typen_id_version_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_type_id_verison_idx_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateEventTypeIdVersionIdx {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_event_typen_id_version_idx_statement()
            .to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            drop_event_type_id_verison_idx_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct CreateSnapshotTable;

fn create_snapshot_table_statement() -> TableCreateStatement {
    Table::create()
        .table(Snapshot::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Snapshot::Id)
                .string()
                .not_null()
                .string_len(26),
        )
        .col(
            ColumnDef::new(Snapshot::Type)
                .string()
                .string_len(50)
                .not_null(),
        )
        .col(ColumnDef::new(Snapshot::Cursor).string().not_null())
        .col(ColumnDef::new(Snapshot::Revision).string().not_null())
        .col(ColumnDef::new(Snapshot::Data).blob().not_null())
        .col(
            ColumnDef::new(Snapshot::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .col(
            ColumnDef::new(Snapshot::UpdatedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .primary_key(Index::create().col(Snapshot::Type).col(Snapshot::Id))
        .to_owned()
}

fn drop_snapshot_table_statement() -> TableDropStatement {
    Table::drop().table(Snapshot::Table).to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateSnapshotTable {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = create_snapshot_table_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_snapshot_table_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateSnapshotTable {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_snapshot_table_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_snapshot_table_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateSnapshotTable {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_snapshot_table_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_snapshot_table_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct CreateSubscriberTable;

fn create_subscriber_table_statement() -> TableCreateStatement {
    Table::create()
        .table(Subscriber::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Subscriber::Key)
                .string()
                .string_len(50)
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(Subscriber::WorkerId)
                .string()
                .not_null()
                .string_len(26),
        )
        .col(ColumnDef::new(Subscriber::Cursor).string())
        .col(ColumnDef::new(Subscriber::Lag).integer().not_null())
        .col(
            ColumnDef::new(Subscriber::Enabled)
                .boolean()
                .not_null()
                .default(true),
        )
        .col(
            ColumnDef::new(Subscriber::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .col(
            ColumnDef::new(Subscriber::UpdatedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .to_owned()
}

fn drop_subscriber_table_statement() -> TableDropStatement {
    Table::drop().table(Subscriber::Table).to_owned()
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl Operation<sqlx::Sqlite> for CreateSubscriberTable {
    async fn up(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = create_subscriber_table_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_subscriber_table_statement().to_string(sea_query::SqliteQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl Operation<sqlx::MySql> for CreateSubscriberTable {
    async fn up(&self, connection: &mut sqlx::MySqlConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = create_subscriber_table_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(
        &self,
        connection: &mut sqlx::MySqlConnection,
    ) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_subscriber_table_statement().to_string(sea_query::MysqlQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl Operation<sqlx::Postgres> for CreateSubscriberTable {
    async fn up(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment =
            create_subscriber_table_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }

    async fn down(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx_migrator::Error> {
        let statment = drop_subscriber_table_statement().to_string(sea_query::PostgresQueryBuilder);
        sqlx::query(&statment).execute(connection).await?;

        Ok(())
    }
}

pub struct InitMigration;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        CreateEventTable,
        CreateEventTypeIdx,
        CreateEventTypeIdIdx,
        CreateEventRoutingKeyTypeIdx,
        CreateEventTypeIdVersionIdx,
        CreateSnapshotTable,
        CreateSubscriberTable
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        CreateEventTable,
        CreateEventTypeIdx,
        CreateEventTypeIdIdx,
        CreateEventRoutingKeyTypeIdx,
        CreateEventTypeIdVersionIdx,
        CreateSnapshotTable,
        CreateSubscriberTable
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        CreateEventTable,
        CreateEventTypeIdx,
        CreateEventTypeIdIdx,
        CreateEventRoutingKeyTypeIdx,
        CreateEventTypeIdVersionIdx,
        CreateSnapshotTable,
        CreateSubscriberTable
    ]
);

pub fn new_migrator<DB: sqlx::Database>() -> Result<Migrator<DB>, sqlx_migrator::Error>
where
    InitMigration: sqlx_migrator::Migration<DB>,
{
    let mut migrator = Migrator::default();
    migrator.add_migration(Box::new(InitMigration))?;

    Ok(migrator)
}
