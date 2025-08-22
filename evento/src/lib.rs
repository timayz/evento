pub mod context;
pub mod cursor;
mod executor;
mod load;
mod save;
mod subscribe;

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
pub mod sql;
#[cfg(any(
    feature = "sqlite-migrator",
    feature = "mysql-migrator",
    feature = "postgres-migrator"
))]
pub mod sql_migrator;

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use executor::*;
pub use load::*;
pub use save::*;
pub use subscribe::*;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Debug, ops::Deref};
use ulid::Ulid;

use crate::cursor::Cursor;

pub mod prelude {
    #[cfg(feature = "stream")]
    pub use tokio_stream::StreamExt;

    #[cfg(any(
        feature = "sqlite-migrator",
        feature = "postgres-migrator",
        feature = "mysql-migrator"
    ))]
    pub use sqlx_migrator::{Migrate, Plan};
}

pub struct EventDetails<D, M = bool> {
    inner: Event,
    pub data: D,
    pub metadata: M,
}

impl<D, M> Deref for EventDetails<D, M> {
    type Target = Event;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(feature = "mysql")]
pub use sql::MySql;

#[cfg(feature = "postgres")]
pub use sql::Postgres;

#[cfg(feature = "sqlite")]
pub use sql::Sqlite;

#[derive(Debug, Serialize, Deserialize)]
pub struct EventCursor {
    pub i: Ulid,
    pub v: i32,
    pub t: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub id: Ulid,
    pub aggregator_id: String,
    pub aggregator_type: String,
    pub version: i32,
    pub name: String,
    pub routing_key: Option<String>,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub timestamp: i64,
}

impl Event {
    pub fn to_details<D: AggregatorName + DeserializeOwned, M: DeserializeOwned>(
        &self,
    ) -> Result<Option<EventDetails<D, M>>, ciborium::de::Error<std::io::Error>> {
        if D::name() != self.name {
            return Ok(None);
        }

        let data = ciborium::from_reader(&self.data[..])?;
        let metadata = ciborium::from_reader(&self.metadata[..])?;

        Ok(Some(EventDetails {
            data,
            metadata,
            inner: self.clone(),
        }))
    }
}

impl Cursor for Event {
    type T = EventCursor;

    fn serialize(&self) -> Self::T {
        EventCursor {
            i: self.id,
            v: self.version,
            t: self.timestamp,
        }
    }
}

pub trait Aggregator:
    Default + Send + Sync + Serialize + DeserializeOwned + Clone + AggregatorName + Debug
{
    fn aggregate<'async_trait>(
        &'async_trait mut self,
        event: &'async_trait Event,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'async_trait>,
    >
    where
        Self: Sync + 'async_trait;
    fn revision() -> &'static str;
}

pub trait AggregatorName {
    fn name() -> &'static str;
}
