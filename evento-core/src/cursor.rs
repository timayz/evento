//! Cursor-based pagination for event queries.
//!
//! This module provides GraphQL-style cursor pagination for efficiently querying
//! large sets of events. It uses keyset pagination for stable, efficient results.
//!
//! # Key Types
//!
//! - [`Value`] - Base64-encoded cursor string
//! - [`Args`] - Pagination arguments (first/after, last/before)
//! - [`ReadResult`] - Paginated result with edges and page info
//! - [`Reader`] - In-memory pagination executor
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::cursor::{Args, Reader};
//!
//! // Forward pagination: first 10 events
//! let args = Args::forward(10, None);
//!
//! // Continue from cursor
//! let args = Args::forward(10, Some(page_info.end_cursor));
//!
//! // Backward pagination: last 10 events before cursor
//! let args = Args::backward(10, Some(cursor));
//!
//! // In-memory pagination
//! let result = Reader::new(events)
//!     .forward(10, None)
//!     .execute()?;
//! ```

use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use thiserror::Error;

/// Sort order for pagination.
#[derive(Debug, Clone, PartialEq)]
pub enum Order {
    /// Ascending order (oldest first)
    Asc,
    /// Descending order (newest first)
    Desc,
}

/// A paginated item with its cursor.
///
/// Each edge contains a node (the actual data) and its cursor
/// for use in subsequent pagination requests.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Edge<N> {
    /// Cursor for this item's position
    pub cursor: Value,
    /// The actual data item
    pub node: N,
}

/// Pagination metadata for a result set.
#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PageInfo {
    /// Whether there are more items before the first edge
    pub has_previous_page: bool,
    /// Whether there are more items after the last edge
    pub has_next_page: bool,
    /// Cursor of the first edge (for backward pagination)
    pub start_cursor: Option<Value>,
    /// Cursor of the last edge (for forward pagination)
    pub end_cursor: Option<Value>,
}

/// Result of a paginated query.
///
/// Contains the requested edges and pagination metadata.
#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadResult<N> {
    /// The paginated items with their cursors
    pub edges: Vec<Edge<N>>,
    /// Pagination metadata
    pub page_info: PageInfo,
}

impl<N> ReadResult<N> {
    pub fn map<B, F>(self, f: F) -> ReadResult<B>
    where
        Self: Sized,
        F: Fn(N) -> B,
    {
        ReadResult {
            page_info: self.page_info,
            edges: self
                .edges
                .into_iter()
                .map(|e| Edge {
                    cursor: e.cursor.to_owned(),
                    node: f(e.node),
                })
                .collect(),
        }
    }
}

/// A base64-encoded cursor value for pagination.
///
/// Cursors are opaque strings that identify a position in a result set.
/// They are serialized using bitcode and base64-encoded for URL safety.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Value(pub String);

impl Deref for Value {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

// Your encoding traits
pub trait Encode {
    fn encode(&self) -> Result<Vec<u8>, CursorError>;
}

pub trait Decode: Sized {
    fn decode(bytes: &[u8]) -> Result<Self, CursorError>;
}

// Blanket impl: anything with bitcode gets it for free
impl<T: bitcode::Encode> Encode for T {
    fn encode(&self) -> Result<Vec<u8>, CursorError> {
        Ok(bitcode::encode(self))
    }
}

impl<T: bitcode::DecodeOwned> Decode for T {
    fn decode(bytes: &[u8]) -> Result<Self, CursorError> {
        bitcode::decode(bytes).map_err(|e| CursorError::Bitcode(e.to_string()))
    }
}

/// Trait for types that can be used as pagination cursors.
///
/// Implementors define how to serialize their position data to/from
/// base64-encoded cursor values.
pub trait Cursor {
    /// The cursor data type (e.g., `EventCursor`)
    type T: Encode + Decode;

    /// Extracts cursor data from this item.
    fn serialize(&self) -> Self::T;
    /// Serializes cursor data to a base64 [`Value`].
    fn serialize_cursor(&self) -> Result<Value, CursorError> {
        use base64::{alphabet, engine::general_purpose, engine::GeneralPurpose, Engine};

        let bytes = self.serialize().encode()?;
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        Ok(Value(engine.encode(&bytes)))
    }
    /// Deserializes cursor data from a base64 [`Value`].
    fn deserialize_cursor(value: &Value) -> Result<Self::T, CursorError> {
        use base64::{alphabet, engine::general_purpose, engine::GeneralPurpose, Engine};

        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let bytes = engine.decode(value)?;

        Self::T::decode(&bytes)
    }
}

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("base64 decode: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("bitcode: {0}")]
    Bitcode(String),
}

/// Pagination arguments for querying events.
///
/// Supports both forward (first/after) and backward (last/before) pagination.
///
/// # Example
///
/// ```rust,ignore
/// // Forward: first 20 items
/// let args = Args::forward(20, None);
///
/// // Forward: next 20 items after cursor
/// let args = Args::forward(20, Some(end_cursor));
///
/// // Backward: last 20 items before cursor
/// let args = Args::backward(20, Some(start_cursor));
/// ```
#[derive(Default, Serialize, Deserialize, Clone)]
pub struct Args {
    /// Number of items for forward pagination
    pub first: Option<u16>,
    /// Cursor to start after (forward pagination)
    pub after: Option<Value>,
    /// Number of items for backward pagination
    pub last: Option<u16>,
    /// Cursor to end before (backward pagination)
    pub before: Option<Value>,
}

impl Args {
    pub fn forward(first: u16, after: Option<Value>) -> Self {
        Self {
            first: Some(first),
            after,
            last: None,
            before: None,
        }
    }

    pub fn backward(last: u16, before: Option<Value>) -> Self {
        Self {
            first: None,
            after: None,
            last: Some(last),
            before,
        }
    }

    pub fn is_backward(&self) -> bool {
        (self.last.is_some() || self.before.is_some())
            && self.first.is_none()
            && self.after.is_none()
    }

    pub fn get_info(&self) -> (u16, Option<Value>) {
        if self.is_backward() {
            (self.last.unwrap_or(40), self.before.clone())
        } else {
            (self.first.unwrap_or(40), self.after.clone())
        }
    }

    pub fn limit(self, v: u16) -> Self {
        if self.is_backward() {
            Args::backward(self.last.unwrap_or(v).min(v), self.before)
        } else {
            Args::forward(self.first.unwrap_or(v).min(v), self.after)
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("cursor: {0}")]
    Cursor(#[from] CursorError),
}

/// In-memory pagination executor.
///
/// `Reader` performs cursor-based pagination on an in-memory vector of items.
/// It's useful for testing or when data is already loaded.
///
/// # Example
///
/// ```rust,ignore
/// let events = vec![event1, event2, event3];
///
/// let result = Reader::new(events)
///     .forward(2, None)
///     .execute()?;
///
/// assert_eq!(result.edges.len(), 2);
/// assert!(result.page_info.has_next_page);
/// ```
pub struct Reader<T> {
    data: Vec<T>,
    args: Args,
    order: Order,
}

impl<T> Reader<T>
where
    T: Cursor + Clone,
    T: Send + Unpin,
    T: Bind<T = T>,
{
    pub fn new(data: Vec<T>) -> Self {
        Self {
            data,
            args: Args::default(),
            order: Order::Asc,
        }
    }

    pub fn order(&mut self, order: Order) -> &mut Self {
        self.order = order;

        self
    }

    pub fn desc(&mut self) -> &mut Self {
        self.order(Order::Desc)
    }

    pub fn args(&mut self, args: Args) -> &mut Self {
        self.args = args;

        self
    }

    pub fn backward(&mut self, last: u16, before: Option<Value>) -> &mut Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }

    pub fn forward(&mut self, first: u16, after: Option<Value>) -> &mut Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

    pub fn execute(&self) -> Result<ReadResult<T>, ReadError> {
        let is_order_desc = matches!(
            (&self.order, self.args.is_backward()),
            (Order::Asc, true) | (Order::Desc, false)
        );

        let mut data = self.data.clone().into_iter().collect::<Vec<_>>();
        T::sort_by(&mut data, is_order_desc);
        let (limit, cursor) = self.args.get_info();

        if let Some(cursor) = cursor.as_ref() {
            let cursor = T::deserialize_cursor(cursor)?;
            T::retain(&mut data, cursor, is_order_desc);
        }

        let data_len = data.len();
        data = data.into_iter().take((limit + 1).into()).collect();

        let has_more = data_len > data.len();
        if has_more {
            data.pop();
        }

        let mut edges = data
            .into_iter()
            .map(|node| Edge {
                cursor: node
                    .serialize_cursor()
                    .expect("Error while serialize_cursor in assert_read_result"),
                node,
            })
            .collect::<Vec<_>>();

        if self.args.is_backward() {
            edges = edges.into_iter().rev().collect();
        }

        let page_info = if self.args.is_backward() {
            PageInfo {
                has_previous_page: has_more,
                start_cursor: edges.first().map(|e| e.cursor.to_owned()),
                ..Default::default()
            }
        } else {
            PageInfo {
                has_next_page: has_more,
                end_cursor: edges.last().map(|e| e.cursor.to_owned()),
                ..Default::default()
            }
        };

        Ok(ReadResult { edges, page_info })
    }
}

impl<T> Deref for Reader<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for Reader<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// Trait for sorting and filtering data for pagination.
///
/// Implementors define how to sort items and filter by cursor position.
pub trait Bind {
    /// The item type being paginated
    type T: Cursor + Clone;

    /// Sorts items in ascending or descending order.
    fn sort_by(data: &mut Vec<Self::T>, is_order_desc: bool);
    /// Retains only items after/before the cursor position.
    fn retain(
        data: &mut Vec<Self::T>,
        cursor: <<Self as Bind>::T as Cursor>::T,
        is_order_desc: bool,
    );
}

/// Cursor with i64 value for numeric sorts (dates, counts, etc.)
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct CursorInt {
    pub i: String,
    pub v: i64,
}

/// Cursor with String value for text sorts (names, titles, etc.)
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct CursorString {
    pub i: String,
    pub v: String,
}

/// Cursor with bool value for boolean sorts
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct CursorBool {
    pub i: String,
    pub v: bool,
}

/// Cursor with f64 value for floating point sorts (ratings, scores, etc.)
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct CursorFloat {
    pub i: String,
    pub v: f64,
}
