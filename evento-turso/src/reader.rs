//! Keyset pagination over any sea-query select: [`Reader`] and [`Bind`].

use std::ops::{Deref, DerefMut};

use sea_query::{Expr, ExprTrait, IntoColumnRef, SelectStatement};
use turso::Connection;

use evento_core::cursor::{self, Args, Cursor, Edge, PageInfo, ReadResult, Value};

use crate::{query, FromRow};

/// Query builder for reading rows with cursor-based pagination — the Turso
/// counterpart of `evento_sql::Reader`.
///
/// `Reader` wraps a sea-query [`SelectStatement`] and adds forward
/// pagination (first N after a cursor), backward pagination (last N before a
/// cursor), and ascending/descending order over the columns a [`Bind`] names.
///
/// ```rust,no_run
/// use evento_turso::{Reader, Turso};
/// use sea_query::Query;
/// # use evento_turso::{EventTable, Bind, FromRow};
/// # #[derive(evento_turso::FromRow)] struct Row { id: String }
/// # impl evento_core::cursor::Cursor for Row { type T = String; fn serialize(&self) -> String { self.id.clone() } }
/// # impl Bind for Row { type T = EventTable; type I = [EventTable; 1]; type V = [sea_query::Expr; 1]; type Cursor = Self;
/// #   fn columns() -> Self::I { [EventTable::Id] } fn values(c: String) -> Self::V { [c.into()] } }
///
/// # async fn run(store: Turso) -> anyhow::Result<()> {
/// let statement = Query::select()
///     .column(EventTable::Id)
///     .from(EventTable::Table)
///     .to_owned();
///
/// let conn = store.connect()?;
/// let page = Reader::new(statement)
///     .forward(10, None)
///     .execute::<Row>(&conn)
///     .await?;
/// if page.page_info.has_next_page {
///     let next = Reader::new(Query::select().column(EventTable::Id).from(EventTable::Table).to_owned())
///         .forward(10, page.page_info.end_cursor)
///         .execute::<Row>(&conn)
///         .await?;
/// #   let _ = next;
/// }
/// # Ok(())
/// # }
/// ```
///
/// `Reader` derefs to the underlying [`SelectStatement`], so sea-query's
/// builder methods are available directly.
pub struct Reader {
    statement: SelectStatement,
    args: Args,
    order: cursor::Order,
}

impl Reader {
    /// Creates a new reader from a sea-query select statement.
    pub fn new(statement: SelectStatement) -> Self {
        Self {
            statement,
            args: Args::default(),
            order: cursor::Order::Asc,
        }
    }

    /// Sets the sort order for results.
    pub fn order(&mut self, order: cursor::Order) -> &mut Self {
        self.order = order;
        self
    }

    /// Sets descending sort order.
    pub fn desc(&mut self) -> &mut Self {
        self.order(cursor::Order::Desc)
    }

    /// Sets pagination arguments directly.
    pub fn args(&mut self, args: Args) -> &mut Self {
        self.args = args;
        self
    }

    /// Configures backward pagination (last N before cursor).
    pub fn backward(&mut self, last: u16, before: Option<Value>) -> &mut Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }

    /// Configures forward pagination (first N after cursor).
    pub fn forward(&mut self, first: u16, after: Option<Value>) -> &mut Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

    /// Executes the query on `conn` and returns one page: rows decoded as
    /// `O`, each with its cursor, plus the page info.
    ///
    /// Matches `evento_core::cursor::Reader` byte for byte: the edges of a
    /// backward page are reversed into ascending order, both boundary cursors
    /// are set, and only the paging direction's "more" flag is computed (the
    /// other stays `false`), per the GraphQL cursor-connection spec.
    pub async fn execute<O>(&self, conn: &Connection) -> anyhow::Result<ReadResult<O>>
    where
        O: FromRow + Cursor + Bind<Cursor = O>,
        <<O as Bind>::I as IntoIterator>::IntoIter: DoubleEndedIterator,
        <<O as Bind>::V as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        // The keyset predicate, the ORDER BY and the LIMIT go on a copy: the
        // stored statement stays the caller's base query, so the same reader
        // can be executed again (a retry, the next page, the other direction)
        // without stacking a second cursor bound on the first.
        let (prepared, limit) = {
            let mut statement = self.statement.clone();
            let limit = self.build_reader::<O>(&mut statement)?;
            (query::prepare(&statement), limit)
        };
        let backward = self.args.is_backward();

        let mut rows: Vec<O> = query::fetch_prepared(conn, prepared).await?;
        let has_more = rows.len() > usize::from(limit);
        if has_more {
            rows.pop();
        }

        let mut edges = Vec::with_capacity(rows.len());
        for node in rows {
            edges.push(Edge {
                cursor: node.serialize_cursor()?,
                node,
            });
        }
        if backward {
            edges.reverse();
        }

        let start_cursor = edges.first().map(|e| e.cursor.clone());
        let end_cursor = edges.last().map(|e| e.cursor.clone());
        let page_info = if backward {
            PageInfo {
                has_previous_page: has_more,
                has_next_page: false,
                start_cursor,
                end_cursor,
            }
        } else {
            PageInfo {
                has_previous_page: false,
                has_next_page: has_more,
                start_cursor,
                end_cursor,
            }
        };

        Ok(ReadResult { edges, page_info })
    }

    /// Adds the keyset predicate, the ORDER BY and the LIMIT (+1 probe row)
    /// for the configured args to `statement`; returns the page size.
    fn build_reader<B>(&self, statement: &mut SelectStatement) -> Result<u16, cursor::CursorError>
    where
        B: Bind,
        <<B as Bind>::I as IntoIterator>::IntoIter: DoubleEndedIterator,
        <<B as Bind>::V as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        let (limit, cursor) = self.args.get_info();
        let is_order_desc = self.is_order_desc();

        if let Some(cursor) = cursor.as_ref() {
            let cursor = B::Cursor::deserialize_cursor(cursor)?;
            let columns = B::columns().into_iter().rev();
            let values = B::values(cursor).into_iter().rev();

            // Nested from the last column out: `a > ? OR (a = ? AND (b > ?
            // OR (b = ? AND c > ?)))`, `<` when descending.
            let mut expr = None::<Expr>;
            for (col, value) in columns.zip(values) {
                let current = if is_order_desc {
                    Expr::col(col.clone()).lt(value.clone())
                } else {
                    Expr::col(col.clone()).gt(value.clone())
                };
                expr = Some(match expr {
                    None => current,
                    Some(prev) => current.or(Expr::col(col).eq(value).and(prev)),
                });
            }
            // `expr` is only `None` when a `Bind` declares zero columns; such
            // a cursor cannot constrain anything, so leave the query as is.
            if let Some(expr) = expr {
                statement.and_where(expr);
            }
        }

        let order = if is_order_desc {
            sea_query::Order::Desc
        } else {
            sea_query::Order::Asc
        };
        for col in B::columns() {
            statement.order_by(col, order.clone());
        }
        statement.limit(u64::from(limit) + 1);

        Ok(limit)
    }

    fn is_order_desc(&self) -> bool {
        matches!(
            (&self.order, self.args.is_backward()),
            (cursor::Order::Asc, true) | (cursor::Order::Desc, false)
        )
    }
}

impl Deref for Reader {
    type Target = SelectStatement;

    fn deref(&self) -> &Self::Target {
        &self.statement
    }
}

impl DerefMut for Reader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.statement
    }
}

/// How a row type is ordered and resumed in a [`Reader`]: the columns the
/// page is sorted by, and their values from a cursor — the Turso counterpart
/// of `evento_sql::Bind`, generated by `#[derive(evento::Cursor)]` with
/// `#[cursor(backend = turso)]`.
pub trait Bind {
    /// Column reference type (a sea-query `Iden` enum).
    type T: IntoColumnRef + Clone;
    /// The columns, as an iterator.
    type I: IntoIterator<Item = Self::T>;
    /// The cursor's values, as an iterator of expressions.
    type V: IntoIterator<Item = Expr>;
    /// The cursor type used for pagination.
    type Cursor: Cursor;

    /// The columns the page is sorted by.
    fn columns() -> Self::I;
    /// The values of `cursor` for those columns, in the same order.
    fn values(cursor: <<Self as Bind>::Cursor as Cursor>::T) -> Self::V;
}
