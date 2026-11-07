//! Keyset pagination over any sea-query select: [`Reader`] and [`Bind`].

use std::ops::{Deref, DerefMut};

use sea_query::{
    Alias, Asterisk, Expr, ExprTrait, IntoColumnRef, Query, SelectStatement, UnionType,
};
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
///
/// The keyset predicate leads with a bound on the first sort column —
/// `a >= ? AND (a > ? OR (a = ? AND …))` — which Turso's planner takes as an
/// index range where the nested `OR` alone would have it read the whole
/// equality range and sort.
pub struct Reader {
    /// The base statements: one for a plain read, several for
    /// [`arms`](Self::arms).
    statements: Vec<SelectStatement>,
    args: Args,
    order: cursor::Order,
}

impl Reader {
    /// Creates a new reader from a sea-query select statement.
    pub fn new(statement: SelectStatement) -> Self {
        Self::arms(vec![statement])
    }

    /// A reader over several base statements, paged as one: each arm is
    /// paged on its own (the keyset predicate, the order and the limit), and
    /// the `UNION ALL` of the arms is paged again. The top rows of the union
    /// of each arm's top rows are the top rows of the whole, so the page is
    /// the one a single `OR` of the arms would give — but each arm is an index
    /// seek where Turso would scan the `OR` (see `query::filter_conditions`).
    /// The arms must be disjoint, or a row is returned twice. [`Deref`] gives
    /// the first arm.
    pub(crate) fn arms(statements: Vec<SelectStatement>) -> Self {
        debug_assert!(!statements.is_empty(), "a reader needs a statement");
        Self {
            statements,
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
    {
        // The keyset predicate, the ORDER BY and the LIMIT go on a copy: the
        // stored statement stays the caller's base query, so the same reader
        // can be executed again (a retry, the next page, the other direction)
        // without stacking a second cursor bound on the first.
        let (prepared, limit) = self.prepared::<O>()?;
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

    /// The statement a read runs — the keyset predicate, the ORDER BY and the
    /// LIMIT (+1 probe row) on every arm, the arms united and paged again —
    /// rendered, with the page size.
    pub(crate) fn prepared<B: Bind>(&self) -> Result<(query::Prepared, u16), cursor::CursorError> {
        let (limit, cursor) = self.args.get_info();
        let keyset = match cursor.as_ref() {
            Some(cursor) => self.keyset::<B>(cursor)?,
            None => None,
        };

        let mut arms = self.statements.iter().cloned().map(|mut arm| {
            if let Some(keyset) = &keyset {
                arm.and_where(keyset.clone());
            }
            self.page::<B>(&mut arm, limit);
            arm
        });
        let first = arms.next().expect("a reader has a statement");
        let statement = match arms.next() {
            None => first,
            Some(second) => {
                // Turso takes no ORDER BY inside a compound member, so each
                // arm is wrapped: `SELECT * FROM (arm) UNION ALL SELECT * FROM
                // (arm) …`, then the union is paged from a subquery.
                let wrap = |arm: SelectStatement| {
                    Query::select()
                        .column(Asterisk)
                        .from_subquery(arm, Alias::new("arm"))
                        .to_owned()
                };
                let mut union = wrap(first);
                union.union(UnionType::All, wrap(second));
                for arm in arms {
                    union.union(UnionType::All, wrap(arm));
                }
                let mut statement = Query::select()
                    .column(Asterisk)
                    .from_subquery(union, Alias::new("arms"))
                    .to_owned();
                self.page::<B>(&mut statement, limit);
                statement
            }
        };
        Ok((query::prepare(&statement), limit))
    }

    /// The keyset predicate for `cursor`: nested from the last column out,
    /// `a > ? OR (a = ? AND (b > ? OR (b = ? AND c > ?)))` (`<` when
    /// descending), led by `a >= ?` (`<=`) when there is more than one
    /// column — the sargable half the planner seeks on. `None` when the
    /// `Bind` declares no columns: such a cursor constrains nothing.
    fn keyset<B: Bind>(&self, cursor: &Value) -> Result<Option<Expr>, cursor::CursorError> {
        let is_order_desc = self.is_order_desc();
        let cursor = B::Cursor::deserialize_cursor(cursor)?;
        let columns: Vec<B::T> = B::columns().into_iter().collect();
        let values: Vec<Expr> = B::values(cursor).into_iter().collect();

        let mut expr = None::<Expr>;
        for (col, value) in columns.iter().cloned().zip(values.iter().cloned()).rev() {
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
        let Some(expr) = expr else {
            return Ok(None);
        };
        if columns.len() < 2 {
            return Ok(Some(expr));
        }
        let (col, value) = (columns[0].clone(), values[0].clone());
        let bound = if is_order_desc {
            Expr::col(col).lte(value)
        } else {
            Expr::col(col).gte(value)
        };
        Ok(Some(bound.and(expr)))
    }

    /// Adds the ORDER BY and the LIMIT (+1 probe row) to `statement`.
    fn page<B: Bind>(&self, statement: &mut SelectStatement, limit: u16) {
        let order = if self.is_order_desc() {
            sea_query::Order::Desc
        } else {
            sea_query::Order::Asc
        };
        for col in B::columns() {
            statement.order_by(col, order.clone());
        }
        statement.limit(u64::from(limit) + 1);
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
        &self.statements[0]
    }
}

impl DerefMut for Reader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.statements[0]
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
