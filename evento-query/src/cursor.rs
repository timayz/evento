#![forbid(unsafe_code)]

use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use chrono::{DateTime, Utc};
use harsh::Harsh;
use serde::{Deserialize, Serialize};
#[cfg(feature = "pg")]
use sqlx::{postgres::PgArguments, query::QueryAs, FromRow, Postgres};
use std::{fmt::Debug, str::FromStr};

use crate::error::QueryError;

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

#[derive(Debug, Clone)]
pub enum CursorOrder {
    Asc,
    Desc,
}

pub trait Cursor: Sized {
    fn keys() -> Vec<&'static str>;
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
    fn serialize(&self) -> Vec<String>;
    fn deserialize(values: Vec<&str>) -> Result<Self, QueryError>;

    fn serialize_utc(value: DateTime<Utc>) -> String {
        Harsh::default().encode(&[value.timestamp_micros() as u64])
    }

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
                DateTime::from_timestamp_micros(timestamp as i64).ok_or(QueryError::Unknown(
                    "field".to_owned(),
                    "NaiveDateTime::from_timestamp_opt".to_owned(),
                    "none".to_owned(),
                ))
            })
    }

    fn to_cursor(&self) -> CursorType {
        let data = self.serialize().join("|");
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        CursorType(engine.encode(data))
    }

    fn from_cursor(cursor: &CursorType) -> Result<Self, QueryError> {
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(cursor)?;
        let data = std::str::from_utf8(&decoded)?;

        Self::deserialize(data.split('|').collect())
    }

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
