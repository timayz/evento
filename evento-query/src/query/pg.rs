use std::marker::PhantomData;

use sqlx::{
    postgres::PgArguments, Arguments, Encode, Executor, FromRow, Postgres, QueryBuilder, Type,
};

use crate::{
    cursor::{Cursor, CursorOrder, CursorType},
    error::QueryError,
    Edge, PageInfo, QueryArgs, QueryResult,
};

/// A builder for constructing PostgreSQL queries with cursor-based pagination.
///
/// The `PgQuery` struct provides methods for building queries with cursor-based pagination
/// for fetching data from a PostgreSQL database. It is generic over the type `O` representing
/// the result type of the query, which must implement the `FromRow`, `Send`, `Unpin`, and `Cursor` traits.
///
pub struct PgQuery<'q, O>
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

impl<'q, O> PgQuery<'q, O>
where
    O: for<'r> FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
    O: 'q + std::marker::Send,
    O: 'q + Unpin,
    O: 'q + Cursor,
{
    /// Creates a new `PgQuery` instance with the provided SQL string.
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

    /// Binds a value to the query, allowing for parameterized queries.
    pub fn bind<T: 'q + Send + Encode<'q, Postgres> + Type<Postgres>>(mut self, value: T) -> Self {
        self.arguments.add(value);
        self.bind_pos += 1;
        self
    }

    /// Sets the cursor order for the query.
    pub fn cursor_order(mut self, value: CursorOrder) -> Self {
        self.cursor_order = value;
        self
    }

    /// Configures the query for backward pagination.
    pub fn backward(self, last: u16, before: Option<CursorType>) -> Self {
        self.build(QueryArgs::backward(last, before))
    }

    /// Configures the query for backward pagination with descending order.
    pub fn backward_desc(self, last: u16, before: Option<CursorType>) -> Self {
        self.cursor_order(CursorOrder::Desc)
            .build(QueryArgs::backward(last, before))
    }

    /// Configures the query for forward pagination.
    pub fn forward(self, first: u16, after: Option<CursorType>) -> Self {
        self.build(QueryArgs::forward(first, after))
    }

    /// Configures the query for forward pagination with descending order.
    pub fn forward_desc(self, first: u16, after: Option<CursorType>) -> Self {
        self.cursor_order(CursorOrder::Desc)
            .build(QueryArgs::forward(first, after))
    }

    /// Configures the query for pagination with descending order.
    pub fn build_desc(self, args: QueryArgs) -> Self {
        self.cursor_order(CursorOrder::Desc).build(args)
    }

    /// Builds the final query based on the provided pagination options.
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

    /// Executes the query and fetches all results with pagination.
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
