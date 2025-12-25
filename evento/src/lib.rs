// Re-export everything from evento-core
pub use evento_core::*;

// Re-export projection types at root level for backwards compatibility
pub use evento_core::projection::{
    Action, Aggregator, Event as EventTrait, EventData, Handler, LoadResult, OptionLoadResult,
    Projection, Snapshot, SubscriptionBuilder,
};

#[cfg(feature = "handler")]
pub use evento_core::projection::Subscription;

// Re-export SQL types when SQL features are enabled
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use evento_sql as sql;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use evento_sql::sql_types;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub use evento_sql_migrator as sql_migrator;

/// Database migration utilities
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
pub mod migrator {
    pub use sqlx_migrator::{Migrate, Plan};
}

#[cfg(feature = "mysql")]
pub use evento_sql::MySql;

#[cfg(feature = "postgres")]
pub use evento_sql::Postgres;

#[cfg(feature = "sqlite")]
pub use evento_sql::Sqlite;
