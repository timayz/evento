mod event_create_routing_key_type_idx;
mod event_create_table;
mod event_create_type_id_idx;
mod event_create_type_id_version_uk;
mod event_create_type_idx;
mod snapshot_create_table;
mod subscriber_create_table;

use sqlx_migrator::vec_box;

pub struct InitMigration;

#[cfg(feature = "sqlite")]
sqlx_migrator::sqlite_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event_create_table::Operation,
        event_create_type_idx::Operation,
        event_create_type_id_idx::Operation,
        event_create_routing_key_type_idx::Operation,
        event_create_type_id_version_uk::Operation,
        snapshot_create_table::Operation,
        subscriber_create_table::Operation,
    ]
);

#[cfg(feature = "mysql")]
sqlx_migrator::mysql_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event_create_table::Operation,
        event_create_type_idx::Operation,
        event_create_type_id_idx::Operation,
        event_create_routing_key_type_idx::Operation,
        event_create_type_id_version_uk::Operation,
        snapshot_create_table::Operation,
        subscriber_create_table::Operation,
    ]
);

#[cfg(feature = "postgres")]
sqlx_migrator::postgres_migration!(
    InitMigration,
    "main",
    "init_migration",
    vec_box![],
    vec_box![
        event_create_table::Operation,
        event_create_type_idx::Operation,
        event_create_type_id_idx::Operation,
        event_create_routing_key_type_idx::Operation,
        event_create_type_id_version_uk::Operation,
        snapshot_create_table::Operation,
        subscriber_create_table::Operation,
    ]
);
