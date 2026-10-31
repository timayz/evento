//! Reading rows: [`FromRow`] for a struct, [`FromValue`] for a column.

use std::{fmt::Display, str::FromStr};

use evento_core::cursor::Value as CursorValue;
use turso::Value;
use ulid::Ulid;

/// A row with its column names: what a [`FromRow`] reads from.
pub struct Row<'a> {
    columns: &'a [String],
    row: &'a turso::Row,
}

impl<'a> Row<'a> {
    /// Over `row`, whose columns are `columns` (from `Rows::column_names`).
    pub fn new(columns: &'a [String], row: &'a turso::Row) -> Self {
        Self { columns, row }
    }

    /// The column names, in order.
    pub fn columns(&self) -> &'a [String] {
        self.columns
    }

    /// Column `name`, converted.
    pub fn get<T: FromValue>(&self, name: &str) -> anyhow::Result<T> {
        match self.try_get(name)? {
            Some(value) => Ok(value),
            None => anyhow::bail!("no column {name:?} in the row"),
        }
    }

    /// Column `name`, converted, or `None` when the row has no such column
    /// (a `SELECT` that leaves it out).
    pub fn try_get<T: FromValue>(&self, name: &str) -> anyhow::Result<Option<T>> {
        match self.columns.iter().position(|c| c == name) {
            Some(i) => self.get_index(i).map(Some),
            None => Ok(None),
        }
    }

    /// Column `i`, converted.
    pub fn get_index<T: FromValue>(&self, i: usize) -> anyhow::Result<T> {
        let value = self.row.get_value(i)?;
        T::from_value(value).map_err(|err| {
            let name = self.columns.get(i).map(String::as_str).unwrap_or("?");
            anyhow::anyhow!("column {name:?}: {err}")
        })
    }

    /// The underlying Turso row.
    pub fn inner(&self) -> &'a turso::Row {
        self.row
    }
}

/// A struct read from a [`Row`]: the row type of [`fetch_all`](crate::fetch_all),
/// [`fetch_optional`](crate::fetch_optional) and [`Reader`](crate::Reader).
/// Derive it with [`FromRow`](derive@crate::FromRow): one field per column of
/// the same name; `#[turso(default)]` lets a field default when its column
/// is not selected.
pub trait FromRow: Sized {
    /// Reads `row`.
    fn from_row(row: &Row<'_>) -> anyhow::Result<Self>;
}

/// A column value converted to a Rust type.
pub trait FromValue: Sized {
    /// Converts `value`.
    fn from_value(value: Value) -> anyhow::Result<Self>;
}

impl FromValue for Value {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        Ok(value)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Null => Ok(None),
            value => T::from_value(value).map(Some),
        }
    }
}

macro_rules! from_integer {
    ($($ty:ty),*) => {$(
        impl FromValue for $ty {
            fn from_value(value: Value) -> anyhow::Result<Self> {
                match value {
                    Value::Integer(v) => <$ty>::try_from(v)
                        .map_err(|_| anyhow::anyhow!("integer {v} out of range for {}", stringify!($ty))),
                    other => anyhow::bail!("expected an integer, got {other:?}"),
                }
            }
        }
    )*};
}
from_integer!(i8, i16, i32, i64, u8, u16, u32, u64, usize, isize);

impl FromValue for bool {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Integer(v) => Ok(v != 0),
            other => anyhow::bail!("expected an integer (0/1), got {other:?}"),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Real(v) => Ok(v),
            Value::Integer(v) => Ok(v as f64),
            other => anyhow::bail!("expected a number, got {other:?}"),
        }
    }
}

impl FromValue for f32 {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        f64::from_value(value).map(|v| v as f32)
    }
}

impl FromValue for String {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Text(s) => Ok(s),
            other => anyhow::bail!("expected text, got {other:?}"),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Blob(b) => Ok(b),
            // A text column read as bytes: its UTF-8.
            Value::Text(s) => Ok(s.into_bytes()),
            other => anyhow::bail!("expected a blob, got {other:?}"),
        }
    }
}

impl FromValue for CursorValue {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        String::from_value(value).map(CursorValue)
    }
}

impl FromValue for Ulid {
    fn from_value(value: Value) -> anyhow::Result<Self> {
        Ok(Ulid::from_string(&String::from_value(value)?)?)
    }
}

/// A value stored as text through its `Display` and read back through its
/// `FromStr` — an enum column, as `sqlx::types::Text`.
///
/// ```rust
/// # use evento_turso::Text;
/// # #[derive(Debug, PartialEq, Clone, Copy)] enum Role { Admin }
/// # impl std::fmt::Display for Role { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("admin") } }
/// # impl std::str::FromStr for Role { type Err = String; fn from_str(s: &str) -> Result<Self, String> { if s == "admin" { Ok(Role::Admin) } else { Err(s.into()) } } }
/// let role: Text<Role> = "admin".parse().unwrap();
/// let bound: sea_query::Value = role.into(); // binds the text "admin"
/// # let _ = bound;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Text<T>(pub T);

impl<T> std::ops::Deref for Text<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Text<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Display> Display for Text<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: FromStr> FromStr for Text<T> {
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Text)
    }
}

impl<T> FromValue for Text<T>
where
    T: FromStr,
    T::Err: Display,
{
    fn from_value(value: Value) -> anyhow::Result<Self> {
        let text = String::from_value(value)?;
        text.parse()
            .map(Text)
            .map_err(|err: T::Err| anyhow::anyhow!("{text:?}: {err}"))
    }
}

impl<T: Display> From<Text<T>> for sea_query::Value {
    fn from(value: Text<T>) -> Self {
        value.0.to_string().into()
    }
}

impl<T: Display> From<&Text<T>> for sea_query::Value {
    fn from(value: &Text<T>) -> Self {
        value.0.to_string().into()
    }
}
