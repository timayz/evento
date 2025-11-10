mod event_add_column_timestamp_subsec;

use sqlx_migrator::vec_box;

pub struct M0002;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0002,
    "main",
    "m0002",
    vec_box![crate::sql_migrator::InitMigration],
    vec_box![event_add_column_timestamp_subsec::Operation]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0002,
    "main",
    "m0002",
    vec_box![crate::sql_migrator::InitMigration],
    vec_box![event_add_column_timestamp_subsec::Operation]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0002,
    "main",
    "m0002",
    vec_box![crate::sql_migrator::InitMigration],
    vec_box![event_add_column_timestamp_subsec::Operation]
);
