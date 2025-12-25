mod event;
mod snapshot;

use sqlx_migrator::vec_box;

pub struct M0003;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    M0003,
    "main",
    "m0003",
    vec_box![crate::sql_migrator::M0002],
    vec_box![
        snapshot::drop_table::Operation,
        event::alter_name_column::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    M0003,
    "main",
    "m0003",
    vec_box![crate::sql_migrator::M0002],
    vec_box![
        snapshot::drop_table::Operation,
        event::alter_name_column::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    M0003,
    "main",
    "m0003",
    vec_box![crate::sql_migrator::M0002],
    vec_box![
        snapshot::drop_table::Operation,
        event::alter_name_column::Operation,
    ]
);
