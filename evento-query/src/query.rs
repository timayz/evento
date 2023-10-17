#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::cursor::{Cursor, CursorType};

#[cfg(feature = "pg")]
mod pg;
#[cfg(feature = "pg")]
pub use pg::*;

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
