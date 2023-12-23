#![deny(missing_docs)]
#![forbid(unsafe_code)]

use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use harsh::Harsh;
use serde::{Deserialize, Serialize};
#[cfg(feature = "pg")]
use sqlx::{postgres::PgArguments, query::QueryAs, FromRow, Postgres};
use std::{fmt::Debug, str::FromStr};

use crate::error::QueryError;

/// `CursorType` represents a cursor for navigating and querying a database.
///
/// A cursor is a mechanism for traversing the results of a query, typically used when dealing
/// with large datasets or when paginating through results. This struct encapsulates the
/// information necessary to manage the state of a cursor and retrieve subsequent rows.
/// 
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct CursorType(pub String);

impl From<String> for CursorType {
    fn from(val: String) -> Self {
        CursorType(val)
    }
}

impl AsRef<[u8]> for CursorType {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Enumeration representing the ordering configuration for a database query result.
///
/// The `CursorOrder` enum allows users to specify the sorting order for one or more columns
/// when retrieving results from a database query. It has two variants: `ASC` for ascending
/// order and `DESC` for descending order.
/// 
/// # Variants
///
/// - `ASC`: Represents ascending order.
/// - `DESC`: Represents descending order.
/// 
#[derive(Debug, Clone)]
pub enum CursorOrder {
    /// Represents ascending order.
    Asc,
    /// Represents descending order.
    Desc,
}

/// Trait representing a cursor for querying a database.
///
/// The `Cursor` trait defines the interface for objects that can be used to manage the state
/// of a database cursor, allowing the execution of queries and retrieval of result sets.
///
/// # Example
///
/// ```
/// use evento_query::{Cursor, QueryError};
/// use sqlx::query::QueryAs;
/// use sqlx::postgres::PgArguments;
/// use sqlx::Postgres;
/// use serde::Deserialize;
/// use chrono::Utc;
/// use uuid::Uuid;
/// use chrono::DateTime;
///
/// // Implement the Cursor trait for a custom CursorType
/// #[derive(Clone, Deserialize, Debug, sqlx::FromRow, Default, PartialEq)]
/// pub struct User {
///     pub id: Uuid,
///     pub name: String,
///     pub age: i32,
///     pub created_at: DateTime<Utc>,
/// }
///
/// impl Cursor for User {
///     fn keys() -> Vec<&'static str> {
///         vec!["created_at", "age", "id"]
///     }
///
///     fn bind<'q, O>(
///         self,
///         query: QueryAs<Postgres, O, PgArguments>,
///     ) -> QueryAs<Postgres, O, PgArguments>
///     where
///         O: for<'r> sqlx::FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
///         O: 'q + std::marker::Send,
///         O: 'q + Unpin,
///         O: 'q + Cursor,
///     {
///         query.bind(self.created_at).bind(self.age).bind(self.id)
///     }
///
///     fn serialize(&self) -> Vec<String> {
///         vec![
///             Self::serialize_utc(self.created_at),
///             self.age.to_string(),
///             self.id.to_string(),
///         ]
///     }
///
///     fn deserialize(values: Vec<&str>) -> Result<Self, QueryError> {
///         let mut values = values.iter();
///         let created_at = Self::deserialize_as_utc("created_at", values.next())?;
///         let age = Self::deserialize_as("age", values.next())?;
///         let id = Self::deserialize_as("id", values.next())?;
///
///         Ok(User {
///             id,
///             age,
///             created_at,
///             ..Default::default()
///         })
///     }
/// }
/// ```
///
/// # Methods
///
/// - `keys`: Returns a vector of column names used as keys for ordering and binding.
/// - `bind`: Binds the cursor values to a given query, allowing for parameterized queries.
/// - `serialize`: Converts the cursor values to a vector of strings for serialization.
/// - `deserialize`: Converts a vector of strings to a result of the cursor type.
/// - `serialize_utc`: Serializes a `DateTime<Utc>` value into a string using Harsh encoding.
/// - `deserialize_as`: Deserializes a string value into a type implementing `FromStr`.
/// - `deserialize_as_utc`: Deserializes a string value into a `DateTime<Utc>` using Harsh decoding.
/// - `to_cursor`: Converts the cursor values into a `CursorType`.
/// - `from_cursor`: Converts a `CursorType` back into the cursor type.
/// - `to_pg_filter_opts`: Creates a PostgreSQL filter with options for cursor-based pagination.
/// - `to_pg_order`: Creates a PostgreSQL order string based on cursor order and direction.
///
pub trait Cursor: Sized {
    /// Returns a vector of column names used as keys for ordering and binding.
    fn keys() -> Vec<&'static str>;

    /// Binds the cursor values to a given query, allowing for parameterized queries.
    #[cfg(feature = "pg")]
    fn bind<'q, O>(
        self,
        query: QueryAs<Postgres, O, PgArguments>,
    ) -> QueryAs<Postgres, O, PgArguments>
    where
        O: for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
        O: 'q + std::marker::Send,
        O: 'q + Unpin,
        O: 'q + Cursor;

    /// Converts the cursor values to a vector of strings for serialization.
    fn serialize(&self) -> Vec<String>;

    /// Converts a vector of strings to a result of the cursor type.
    fn deserialize(values: Vec<&str>) -> Result<Self, QueryError>;

    /// Serializes a `DateTime<Utc>` value into a string using Harsh encoding.
    fn serialize_utc(value: DateTime<Utc>) -> String {
        Harsh::default().encode(&[value.timestamp_micros() as u64])
    }

    /// Deserializes a string value into a type implementing `FromStr`.
    fn deserialize_as<F: Into<String>, D: FromStr>(
        field: F,
        value: Option<&&str>,
    ) -> Result<D, QueryError> {
        let field = field.into();
        value
            .ok_or(QueryError::MissingField(field.to_owned()))
            .and_then(|v| {
                v.to_string().parse::<D>().map_err(|_| {
                    QueryError::Unknown(
                        field,
                        v.to_string(),
                        "failed to deserialize_as_string".to_owned(),
                    )
                })
            })
    }

    /// Deserializes a string value into a `DateTime<Utc>` using Harsh decoding.
    fn deserialize_as_utc<F: Into<String>>(
        field: F,
        value: Option<&&str>,
    ) -> Result<DateTime<Utc>, QueryError> {
        let field = field.into();
        value
            .ok_or(QueryError::MissingField(field))
            .and_then(|v| {
                Harsh::default()
                    .decode(v)
                    .map(|v| v[0])
                    .map_err(QueryError::Harsh)
            })
            .and_then(|timestamp| {
                NaiveDateTime::from_timestamp_micros(timestamp as i64).ok_or(QueryError::Unknown(
                    "field".to_owned(),
                    "NaiveDateTime::from_timestamp_opt".to_owned(),
                    "none".to_owned(),
                ))
            })
            .map(|datetime| DateTime::from_naive_utc_and_offset(datetime, Utc))
    }

    /// Converts the cursor values into a `CursorType`.
    fn to_cursor(&self) -> CursorType {
        let data = self.serialize().join("|");
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        CursorType(engine.encode(data))
    }

    /// Converts a `CursorType` back into the cursor type.
    fn from_cursor(cursor: &CursorType) -> Result<Self, QueryError> {
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(cursor)?;
        let data = std::str::from_utf8(&decoded)?;

        Self::deserialize(data.split('|').collect())
    }

    /// Creates a PostgreSQL filter with options for cursor-based pagination.
    fn to_pg_filter_opts(
        order: &CursorOrder,
        backward: bool,
        keys: Option<Vec<&str>>,
        pos: Option<usize>,
    ) -> String {
        let pos = pos.unwrap_or(1);
        let with_braket = keys.is_some();
        let mut keys = keys.unwrap_or(Self::keys());
        let key = keys.remove(0);

        let sign = match (order, backward) {
            (CursorOrder::Asc, true) | (CursorOrder::Desc, false) => "<",
            (CursorOrder::Asc, false) | (CursorOrder::Desc, true) => ">",
        };
        let filter = format!("{key} {sign} ${pos}");

        if keys.is_empty() {
            return filter;
        }

        let filter = format!(
            "{filter} OR ({key} = ${pos} AND {})",
            Self::to_pg_filter_opts(order, backward, Some(keys), Some(pos + 1))
        );

        if with_braket {
            format!("({filter})")
        } else {
            filter
        }
    }

    /// Creates a PostgreSQL order string based on cursor order and direction.
    fn to_pg_order(order: &CursorOrder, backward: bool) -> String {
        let order = match (order, backward) {
            (CursorOrder::Asc, true) | (CursorOrder::Desc, false) => "DESC",
            (CursorOrder::Asc, false) | (CursorOrder::Desc, true) => "ASC",
        };

        Self::keys()
            .iter()
            .map(|key| format!("{key} {order}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
