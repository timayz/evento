#![forbid(unsafe_code)]
mod cursor;
mod error;

use cursor::CursorType;
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::PgArguments, Arguments, Encode, Executor, FromRow, Postgres, QueryBuilder, Type,
};
use std::{fmt::Debug, marker::PhantomData};

pub use cursor::{Cursor, CursorOrder};
pub use error::QueryError;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Edge<N> {
    pub cursor: CursorType,
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
    pub start_cursor: Option<CursorType>,
    pub end_cursor: Option<CursorType>,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult<N> {
    pub edges: Vec<Edge<N>>,
    pub page_info: PageInfo,
}

#[derive(Default, Serialize, Deserialize)]
pub struct QueryArgs {
    pub first: Option<u16>,
    pub after: Option<CursorType>,
    pub last: Option<u16>,
    pub before: Option<CursorType>,
}

impl QueryArgs {
    pub fn backward(last: u16, before: Option<CursorType>) -> Self {
        Self {
            last: Some(last),
            before,
            ..Default::default()
        }
    }

    pub fn forward(first: u16, after: Option<CursorType>) -> Self {
        Self {
            first: Some(first),
            after,
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
    cursor: Option<CursorType>,
    cursor_order: CursorOrder,
    is_backward: bool,
    limit: u16,
    bind_pos: usize,
    arguments: PgArguments,
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
            cursor_order: CursorOrder::Asc,
            is_backward: false,
            limit: 0,
            bind_pos: 1,
            arguments: PgArguments::default(),
        }
    }

    pub fn bind<T: 'q + Send + Encode<'q, Postgres> + Type<Postgres>>(mut self, value: T) -> Self {
        self.arguments.add(value);
        self.bind_pos += 1;
        self
    }

    pub fn cursor_order(mut self, value: CursorOrder) -> Self {
        self.cursor_order = value;
        self
    }

    pub fn backward(self, last: u16, before: Option<CursorType>) -> Self {
        self.build(QueryArgs::backward(last, before))
    }

    pub fn backward_desc(self, last: u16, before: Option<CursorType>) -> Self {
        self.cursor_order(CursorOrder::Desc)
            .build(QueryArgs::backward(last, before))
    }

    pub fn forward(self, first: u16, after: Option<CursorType>) -> Self {
        self.build(QueryArgs::forward(first, after))
    }

    pub fn forward_desc(self, first: u16, after: Option<CursorType>) -> Self {
        self.cursor_order(CursorOrder::Desc)
            .build(QueryArgs::forward(first, after))
    }

    pub fn build_desc(self, args: QueryArgs) -> Self {
        self.cursor_order(CursorOrder::Desc).build(args)
    }

    pub fn build(mut self, args: QueryArgs) -> Self {
        let (limit, cursor) = if args.is_backward() {
            (args.last.unwrap_or(40), args.before.as_ref())
        } else {
            (args.first.unwrap_or(40), args.after.as_ref())
        };

        if cursor.is_some() {
            let filter = O::to_pg_filter_opts(
                &self.cursor_order,
                args.is_backward(),
                None,
                Some(self.bind_pos),
            );

            let filter = if self.builder.sql().contains(" WHERE ") {
                format!(" AND ({filter})")
            } else {
                format!(" WHERE {filter}")
            };

            self.builder.push(format!(" {filter}"));
        }

        let order = O::to_pg_order(&self.cursor_order, args.is_backward());
        self.builder
            .push(format!(" ORDER BY {order} LIMIT {}", limit + 1));

        self.cursor = cursor.cloned();
        self.is_backward = args.is_backward();
        self.limit = limit;

        self
    }

    pub async fn fetch_all<E>(self, executor: E) -> Result<QueryResult<O>, QueryError>
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
            let start_cursor = edges.first().map(|edge| edge.cursor.clone());

            PageInfo {
                has_previous_page: has_more,
                has_next_page: false,
                start_cursor,
                end_cursor: None,
            }
        } else {
            let end_cursor = edges.last().map(|edge| edge.cursor.clone());

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
