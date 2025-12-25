mod event;
mod snapshot;
mod subscriber;

use sqlx_migrator::vec_box;

pub struct InitMigration;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event::create_table::Operation,
        event::create_type_idx::Operation,
        event::create_type_id_idx::Operation,
        event::create_routing_key_type_idx::Operation,
        event::create_type_id_version_uk::Operation,
        snapshot::create_table::Operation,
        subscriber::create_table::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event::create_table::Operation,
        event::create_type_idx::Operation,
        event::create_type_id_idx::Operation,
        event::create_routing_key_type_idx::Operation,
        event::create_type_id_version_uk::Operation,
        snapshot::create_table::Operation,
        subscriber::create_table::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event::create_table::Operation,
        event::create_type_idx::Operation,
        event::create_type_id_idx::Operation,
        event::create_routing_key_type_idx::Operation,
        event::create_type_id_version_uk::Operation,
        snapshot::create_table::Operation,
        subscriber::create_table::Operation,
    ]
);
