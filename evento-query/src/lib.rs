#![forbid(unsafe_code)]

use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use evento::SubscirberHandlerError;
use harsh::Harsh;
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::PgArguments, query::QueryAs, Arguments, Encode, Executor, FromRow, Postgres,
    QueryBuilder, Type,
};
use std::{fmt::Debug, marker::PhantomData, str::FromStr};

#[derive(Debug, Clone)]
pub enum QueryOrder {
    Asc,
    Desc,
}

#[derive(thiserror::Error, Debug)]
pub enum CursorError {
    #[error("{0}")]
    MissingField(String),
    #[error("chrono: {0}")]
    ChronoParseError(chrono::ParseError),
    #[error("sqlx: {0}")]
    Sqlx(sqlx::Error),
    #[error("base64: {0}")]
    Base64(base64::DecodeError),
    #[error("str utf8: {0}")]
    StrUtf8(std::str::Utf8Error),
    #[error("harsh: {0}")]
    Harsh(harsh::Error),
    #[error("{0}")]
    Unknown(String, String, String),
}

impl From<chrono::ParseError> for CursorError {
    fn from(value: chrono::ParseError) -> Self {
        Self::ChronoParseError(value)
    }
}

impl From<sqlx::Error> for CursorError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

impl From<base64::DecodeError> for CursorError {
    fn from(value: base64::DecodeError) -> Self {
        Self::Base64(value)
    }
}

impl From<std::str::Utf8Error> for CursorError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::StrUtf8(value)
    }
}

impl From<harsh::Error> for CursorError {
    fn from(value: harsh::Error) -> Self {
        Self::Harsh(value)
    }
}

pub trait Cursor: Sized {
    fn keys() -> Vec<&'static str>;
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
    fn deserialize(values: Vec<&str>) -> Result<Self, CursorError>;

    fn serialize_utc(value: DateTime<Utc>) -> String {
        Harsh::default().encode(&[value.timestamp_micros() as u64])
    }

    fn deserialize_as<F: Into<String>, D: FromStr>(
        field: F,
        value: Option<&&str>,
    ) -> Result<D, CursorError> {
        let field = field.into();
        value
            .ok_or(CursorError::MissingField(field.to_owned()))
            .and_then(|v| {
                v.to_string().parse::<D>().map_err(|_| {
                    CursorError::Unknown(
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
    ) -> Result<DateTime<Utc>, CursorError> {
        let field = field.into();
        value
            .ok_or(CursorError::MissingField(field))
            .and_then(|v| {
                Harsh::default()
                    .decode(v)
                    .map(|v| v[0])
                    .map_err(CursorError::Harsh)
            })
            .and_then(|timestamp| {
                NaiveDateTime::from_timestamp_micros(timestamp as i64).ok_or(CursorError::Unknown(
                    "field".to_owned(),
                    "NaiveDateTime::from_timestamp_opt".to_owned(),
                    "none".to_owned(),
                ))
            })
            .map(|datetime| DateTime::from_naive_utc_and_offset(datetime, Utc))
    }

    fn to_cursor(&self) -> String {
        let data = self.serialize().join("|");
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        engine.encode(data)
    }

    fn from_cursor<C: Into<String>>(cursor: C) -> Result<Self, CursorError> {
        let cursor: String = cursor.into();
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(cursor)?;
        let data = std::str::from_utf8(&decoded)?;

        Self::deserialize(data.split('|').collect())
    }

    fn to_pg_filter_opts(
        order: &QueryOrder,
        backward: bool,
        keys: Option<Vec<&str>>,
        pos: Option<usize>,
    ) -> String {
        let pos = pos.unwrap_or(1);
        let with_braket = keys.is_some();
        let mut keys = keys.unwrap_or(Self::keys());
        let key = keys.remove(0);

        let sign = match (order, backward) {
            (QueryOrder::Asc, true) | (QueryOrder::Desc, false) => "<",
            (QueryOrder::Asc, false) | (QueryOrder::Desc, true) => ">",
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

    fn to_pg_order(order: &QueryOrder, backward: bool) -> String {
        let order = match (order, backward) {
            (QueryOrder::Asc, true) | (QueryOrder::Desc, false) => "DESC",
            (QueryOrder::Asc, false) | (QueryOrder::Desc, true) => "ASC",
        };

        Self::keys()
            .iter()
            .map(|key| format!("{key} {order}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Edge<N> {
    pub cursor: String,
    pub node: N,
}

impl<N: Cursor> From<N> for Edge<N> {
    fn from(value: N) -> Self {
        Self {
            cursor: value.to_cursor(),
            node: value,
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PageInfo {
    pub has_previous_page: bool,
    pub has_next_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult<N> {
    pub edges: Vec<Edge<N>>,
    pub page_info: PageInfo,
}

#[derive(Default, Serialize, Deserialize)]
pub struct QueryArgs {
    pub first: Option<u16>,
    pub after: Option<String>,
    pub last: Option<u16>,
    pub before: Option<String>,
}

impl QueryArgs {
    pub fn backward<C: Into<String>>(last: u16, before: Option<C>) -> Self {
        Self {
            last: Some(last),
            before: before.map(|c| c.into()),
            ..Default::default()
        }
    }

    pub fn forward<C: Into<String>>(first: u16, after: Option<C>) -> Self {
        Self {
            first: Some(first),
            after: after.map(|c| c.into()),
            ..Default::default()
        }
    }

    pub fn is_backward(&self) -> bool {
        (self.last.is_some() || self.before.is_some())
            && self.first.is_none()
            && self.after.is_none()
    }

    pub fn is_none(&self) -> bool {
        self.last.is_none() && self.before.is_none() && self.first.is_none() && self.after.is_none()
    }
}

pub struct Query<'q, O>
where
    O: for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
    O: 'q + std::marker::Send,
    O: 'q + Unpin,
    O: 'q + Cursor,
{
    builder: QueryBuilder<'q, Postgres>,
    phantom: PhantomData<&'q O>,
    cursor: Option<String>,
    is_backward: bool,
    limit: u16,
    bind_pos: usize,
    arguments: PgArguments,
    order: QueryOrder,
}

impl<'q, O> Query<'q, O>
where
    O: for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
    O: 'q + std::marker::Send,
    O: 'q + Unpin,
    O: 'q + Cursor,
{
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            builder: QueryBuilder::new(sql),
            phantom: PhantomData,
            cursor: None,
            is_backward: false,
            limit: 0,
            bind_pos: 1,
            arguments: PgArguments::default(),
            order: QueryOrder::Asc,
        }
    }

    pub fn bind<T: 'q + Send + Encode<'q, Postgres> + Type<Postgres>>(mut self, value: T) -> Self {
        self.arguments.add(value);
        self.bind_pos += 1;
        self
    }

    pub fn order(mut self, value: QueryOrder) -> Self {
        self.order = value;
        self
    }

    pub fn backward(self, last: u16, before: Option<impl Into<String>>) -> Self {
        self.build(QueryArgs::backward(last, before))
    }

    pub fn forward(self, first: u16, after: Option<impl Into<String>>) -> Self {
        self.build(QueryArgs::forward(first, after))
    }

    pub fn build(mut self, args: QueryArgs) -> Self {
        let (limit, cursor) = if args.is_backward() {
            (args.last.unwrap_or(40), args.before.as_ref())
        } else {
            (args.first.unwrap_or(40), args.after.as_ref())
        };

        if cursor.is_some() {
            let filter =
                O::to_pg_filter_opts(&self.order, args.is_backward(), None, Some(self.bind_pos));

            let filter = if self.builder.sql().contains(" WHERE ") {
                format!(" AND ({filter})")
            } else {
                format!(" WHERE {filter}")
            };

            self.builder.push(format!(" {filter}"));
        }

        let order = O::to_pg_order(&self.order, args.is_backward());
        self.builder
            .push(format!(" ORDER BY {order} LIMIT {}", limit + 1));

        self.cursor = cursor.cloned();
        self.is_backward = args.is_backward();
        self.limit = limit;

        self
    }

    pub async fn fetch_all<E>(self, executor: E) -> Result<QueryResult<O>, CursorError>
    where
        E: 'q + Executor<'q, Database = Postgres>,
    {
        let mut query = sqlx::query_as_with::<_, O, _>(self.builder.sql(), self.arguments);

        if let Some(cursor) = &self.cursor {
            let cursor = O::from_cursor(cursor)?;
            query = cursor.bind(query);
        }

        let mut rows = query.fetch_all(executor).await?;
        let has_more = rows.len() > self.limit as usize;

        if has_more {
            rows.pop();
        };

        let edges_iter = rows.into_iter().map(|node| Edge {
            cursor: node.to_cursor(),
            node,
        });

        let edges: Vec<_> = if self.is_backward {
            edges_iter.rev().collect()
        } else {
            edges_iter.collect()
        };

        let page_info = if self.is_backward {
            let start_cursor = edges.first().map(|edge| edge.cursor.to_owned());

            PageInfo {
                has_previous_page: has_more,
                has_next_page: false,
                start_cursor,
                end_cursor: None,
            }
        } else {
            let end_cursor = edges.last().map(|edge| edge.cursor.to_owned());

            PageInfo {
                has_previous_page: false,
                has_next_page: has_more,
                start_cursor: None,
                end_cursor,
            }
        };

        Ok(QueryResult { edges, page_info })
    }
}

impl From<CursorError> for SubscirberHandlerError {
    fn from(e: CursorError) -> Self {
        SubscirberHandlerError::new("query::CursorError", e.to_string())
    }
}
