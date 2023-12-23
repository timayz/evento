#![deny(missing_docs)]
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::cursor::{Cursor, CursorType};

#[cfg(feature = "pg")]
mod pg;
#[cfg(feature = "pg")]
pub use pg::*;

/// A struct representing an edge in a paginated result set.
///
/// An `Edge` consists of a cursor of type `CursorType` and the associated node of type `N`.
///
/// # Examples
///
/// ```rust
/// use evento_query::{Edge, CursorType};
///
/// // Create an edge with a cursor and a node.
/// let edge: Edge<String> = Edge {
///     cursor: CursorType::new("example_cursor"),
///     node: "example_node".to_string(),
/// };
/// ```
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Edge<N> {
    /// The cursor associated with the edge.
    pub cursor: CursorType,
    /// The node associated with the edge.
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

/// A struct representing pagination information.
///
/// The `PageInfo` struct includes flags indicating the presence of previous and next pages,
/// as well as optional start and end cursors.
///
/// # Examples
///
/// ```rust
/// use evento_query::PageInfo;
///
/// // Create PageInfo with flags indicating no previous or next pages.
/// let page_info: PageInfo = PageInfo {
///     has_previous_page: false,
///     has_next_page: false,
///     start_cursor: None,
///     end_cursor: None,
/// };
/// ```
#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PageInfo {
    /// Indicates whether there is a previous page.
    pub has_previous_page: bool,
    /// Indicates whether there is a next page.
    pub has_next_page: bool,
    /// Optional start cursor for the paginated range.
    pub start_cursor: Option<CursorType>,
    /// Optional end cursor for the paginated range.
    pub end_cursor: Option<CursorType>,
}

/// A struct representing the result of a paginated query.
///
/// The `QueryResult` struct includes a vector of `Edge` and `PageInfo` providing pagination details.
///
/// # Examples
///
/// ```rust
/// use evento_query::{QueryResult, Edge, PageInfo, CursorType};
///
/// // Create a QueryResult with edges and page information.
/// let query_result: QueryResult<String> = QueryResult {
///     edges: vec![
///         Edge {
///             cursor: CursorType::new("cursor1"),
///             node: "node1".to_string(),
///         },
///         Edge {
///             cursor: CursorType::new("cursor2"),
///             node: "node2".to_string(),
///         },
///     ],
///     page_info: PageInfo {
///         has_previous_page: false,
///         has_next_page: true,
///         start_cursor: Some(CursorType::new("start_cursor")),
///         end_cursor: Some(CursorType::new("end_cursor")),
///     },
/// };
/// ```
#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult<N> {
    /// Vector of edges in the paginated result set.
    pub edges: Vec<Edge<N>>,
    /// Pagination information.
    pub page_info: PageInfo,
}

/// A struct representing arguments for paginated queries.
///
/// The `QueryArgs` struct includes options for `first`, `after`, `last`, and `before` to control
/// pagination direction and limits.
///
/// # Examples
///
/// ```rust
/// use evento_query::{QueryArgs, CursorType};
///
/// // Create QueryArgs for forward pagination.
/// let forward_args = QueryArgs::forward(10, Some(CursorType::new("after_cursor")));
///
/// // Create QueryArgs for backward pagination.
/// let backward_args = QueryArgs::backward(5, Some(CursorType::new("before_cursor")));
/// ```
#[derive(Default, Serialize, Deserialize)]
pub struct QueryArgs {
    /// Limit the number of items to retrieve from the start of the list.
    pub first: Option<u16>,
    /// Retrieve items in the list that appear after the specified cursor.
    pub after: Option<CursorType>,
    /// Limit the number of items to retrieve from the end of the list.
    pub last: Option<u16>,
    /// Retrieve items in the list that appear before the specified cursor.
    pub before: Option<CursorType>,
}

impl QueryArgs {
    /// Creates `QueryArgs` for backward pagination.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evento_query::{QueryArgs, CursorType};
    ///
    /// // Create QueryArgs for backward pagination with a specified number of items and a before cursor.
    /// let args = QueryArgs::backward(5, Some(CursorType::new("before_cursor")));
    /// ```
    pub fn backward(last: u16, before: Option<CursorType>) -> Self {
        Self {
            last: Some(last),
            before,
            ..Default::default()
        }
    }

    /// Creates `QueryArgs` for forward pagination.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evento_query::{QueryArgs, CursorType};
    ///
    /// // Create QueryArgs for forward pagination with a specified number of items and an after cursor.
    /// let args = QueryArgs::forward(10, Some(CursorType::new("after_cursor")));
    /// ```
    pub fn forward(first: u16, after: Option<CursorType>) -> Self {
        Self {
            first: Some(first),
            after,
            ..Default::default()
        }
    }

    /// Checks if the query is configured for backward pagination.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evento_query::{QueryArgs, CursorType};
    ///
    /// // Check if the QueryArgs is configured for backward pagination.
    /// let args = QueryArgs::backward(5, Some(CursorType::new("before_cursor")));
    /// assert!(args.is_backward());
    /// ```
    pub fn is_backward(&self) -> bool {
        (self.last.is_some() || self.before.is_some())
            && self.first.is_none()
            && self.after.is_none()
    }

    /// Checks if all fields in the `QueryArgs` are set to `None`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evento_query::QueryArgs;
    ///
    /// // Check if all fields in QueryArgs are set to None.
    /// let args = QueryArgs::default();
    /// assert!(args.is_none());
    /// ```
    pub fn is_none(&self) -> bool {
        self.last.is_none() && self.before.is_none() && self.first.is_none() && self.after.is_none()
    }
}
