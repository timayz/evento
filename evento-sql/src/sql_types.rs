//! SQL type wrappers for serialization.
//!
//! This module provides the [`Bitcode`] wrapper type for compact binary serialization
//! of Rust types to/from SQL BLOB columns using the bitcode format.

use std::ops::{Deref, DerefMut};

use sqlx::database::Database;
use sqlx::decode::Decode;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteArgumentValue, SqliteTypeInfo};
use sqlx::types::Type;

/// A wrapper type for bitcode-serialized data in SQL databases.
///
/// `Bitcode<T>` wraps a value of type `T` and provides automatic serialization/deserialization
/// using the [bitcode](https://crates.io/crates/bitcode) binary format when storing to or
/// reading from SQL databases.
///
/// # Features
///
/// - **Compact binary format** - Efficient storage compared to JSON or other text formats
/// - **Type-safe** - Compile-time checking of serialization capabilities
/// - **Fast** - High performance serialization and deserialization
///
/// # Database Support
///
/// Currently implements SQLx traits for SQLite. Data is stored as BLOB.
///
/// # Example
///
/// ```rust,ignore
/// use evento_sql::sql_types::Bitcode;
/// use bitcode::{Encode, Decode};
///
/// #[derive(Encode, Decode)]
/// struct MyData {
///     value: i32,
///     name: String,
/// }
///
/// // Wrap data for storage
/// let data = Bitcode(MyData { value: 42, name: "test".into() });
///
/// // Encode to bytes
/// let bytes = data.encode_to()?;
///
/// // Decode from bytes
/// let decoded = Bitcode::<MyData>::decode_from_bytes(&bytes)?;
/// assert_eq!(decoded.value, 42);
/// ```
///
/// # Deref
///
/// `Bitcode<T>` implements `Deref` and `DerefMut` to `T`, allowing transparent access
/// to the inner value.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bitcode<T: ?Sized>(pub T);

impl<T> From<T> for Bitcode<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for Bitcode<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Bitcode<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> AsRef<T> for Bitcode<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> AsMut<T> for Bitcode<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> Bitcode<T>
where
    T: bitcode::Encode,
{
    /// Serializes the wrapped value to a byte vector using bitcode.
    pub fn encode_to(&self) -> Vec<u8> {
        bitcode::encode(&self.0)
    }
}

impl<T> Bitcode<T>
where
    T: bitcode::DecodeOwned,
{
    /// Deserializes a value from a byte slice using bitcode.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails or the data is invalid.
    pub fn decode_from_bytes(bytes: &[u8]) -> Result<Self, bitcode::Error> {
        let data = bitcode::decode::<T>(bytes)?;
        Ok(Self(data))
    }
}

impl<T> Type<sqlx::Sqlite> for Bitcode<T> {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<sqlx::Sqlite>>::type_info()
    }

    fn compatible(ty: &SqliteTypeInfo) -> bool {
        <&[u8] as Type<sqlx::Sqlite>>::compatible(ty)
    }
}

impl<T> Encode<'_, sqlx::Sqlite> for Bitcode<T>
where
    T: bitcode::Encode,
{
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as Database>::ArgumentBuffer<'_>,
    ) -> Result<IsNull, BoxDynError> {
        buf.push(SqliteArgumentValue::Blob(std::borrow::Cow::Owned(
            self.encode_to(),
        )));

        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, sqlx::Sqlite> for Bitcode<T>
where
    T: bitcode::DecodeOwned,
    Vec<u8>: sqlx::Decode<'r, sqlx::Sqlite>,
{
    fn decode(value: <sqlx::Sqlite as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let decoded = Vec::<u8>::decode(value)?;
        let data = Bitcode::<T>::decode_from_bytes(&decoded[..])?;

        Ok(data)
    }
}
