mod aggregator;
pub mod context;
pub mod cursor;
mod executor;
pub mod metadata;
pub mod projection;

#[cfg(feature = "macro")]
pub use evento_macro::*;

pub use aggregator::*;
pub use executor::*;
pub use projection::RoutingKey;

use std::fmt::Debug;
use ulid::Ulid;

use crate::cursor::Cursor;

/// Cursor for event pagination and positioning
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct EventCursor {
    /// Event ID (ULID string)
    pub i: String,
    /// Event version
    pub v: i32,
    /// Event timestamp (Unix timestamp in seconds)
    pub t: u64,
    pub s: u32,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Event {
    /// Unique event identifier (ULID)
    pub id: Ulid,
    /// ID of the aggregate this event belongs to
    pub aggregator_id: String,
    /// Type name of the aggregate (e.g., "myapp/User")
    pub aggregator_type: String,
    /// Version number of the aggregate after this event
    pub version: i32,
    /// Event type name
    pub name: String,
    /// Optional routing key for event distribution
    pub routing_key: Option<String>,
    /// Serialized event data (bincode format)
    pub data: Vec<u8>,
    /// Serialized event metadata (bincode format)
    pub metadata: Vec<u8>,
    /// Unix timestamp when the event occurred (seconds)
    pub timestamp: u64,
    /// Unix timestamp when the event occurred (subsec millis)
    pub timestamp_subsec: u32,
}

impl Cursor for Event {
    type T = EventCursor;

    fn serialize(&self) -> Self::T {
        EventCursor {
            i: self.id.to_string(),
            v: self.version,
            t: self.timestamp,
            s: self.timestamp_subsec,
        }
    }
}

impl cursor::Bind for Event {
    type T = Self;

    fn sort_by(data: &mut Vec<Self::T>, is_order_desc: bool) {
        if !is_order_desc {
            data.sort_by(|a, b| {
                if a.timestamp_subsec != b.timestamp_subsec {
                    return a.timestamp_subsec.cmp(&b.timestamp_subsec);
                }

                if a.timestamp != b.timestamp {
                    return a.timestamp.cmp(&b.timestamp);
                }

                if a.version != b.version {
                    return a.version.cmp(&b.version);
                }

                a.id.cmp(&b.id)
            });
        } else {
            data.sort_by(|a, b| {
                if a.timestamp_subsec != b.timestamp_subsec {
                    return b.timestamp_subsec.cmp(&a.timestamp_subsec);
                }

                if a.timestamp != b.timestamp {
                    return b.timestamp.cmp(&a.timestamp);
                }

                if a.version != b.version {
                    return b.version.cmp(&a.version);
                }

                b.id.cmp(&a.id)
            });
        }
    }

    fn retain(
        data: &mut Vec<Self::T>,
        cursor: <<Self as cursor::Bind>::T as Cursor>::T,
        is_order_desc: bool,
    ) {
        data.retain(|event| {
            if is_order_desc {
                event.timestamp < cursor.t
                    || (event.timestamp == cursor.t
                        && (event.timestamp_subsec < cursor.s
                            || (event.timestamp_subsec == cursor.s
                                && (event.version < cursor.v
                                    || (event.version == cursor.v
                                        && event.id.to_string() < cursor.i)))))
            } else {
                event.timestamp > cursor.t
                    || (event.timestamp == cursor.t
                        && (event.timestamp_subsec > cursor.s
                            || (event.timestamp_subsec == cursor.s
                                && (event.version > cursor.v
                                    || (event.version == cursor.v
                                        && event.id.to_string() > cursor.i)))))
            }
        });
    }
}

#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
impl<R: sqlx::Row> sqlx::FromRow<'_, R> for Event
where
    i32: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    Vec<u8>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::Type<R::Database> + sqlx::Decode<'r, R::Database>,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
{
    fn from_row(row: &R) -> Result<Self, sqlx::Error> {
        let timestamp: i64 = sqlx::Row::try_get(row, "timestamp")?;
        let timestamp_subsec: i64 = sqlx::Row::try_get(row, "timestamp_subsec")?;

        Ok(Event {
            id: Ulid::from_string(sqlx::Row::try_get(row, "id")?)
                .map_err(|err| sqlx::Error::InvalidArgument(err.to_string()))?,
            aggregator_id: sqlx::Row::try_get(row, "aggregator_id")?,
            aggregator_type: sqlx::Row::try_get(row, "aggregator_type")?,
            version: sqlx::Row::try_get(row, "version")?,
            name: sqlx::Row::try_get(row, "name")?,
            routing_key: sqlx::Row::try_get(row, "routing_key")?,
            data: sqlx::Row::try_get(row, "data")?,
            metadata: sqlx::Row::try_get(row, "metadata")?,
            timestamp: timestamp as u64,
            timestamp_subsec: timestamp_subsec as u32,
        })
    }
}
