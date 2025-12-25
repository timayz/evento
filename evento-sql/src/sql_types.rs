//! SQL type wrappers for serialization.
//!
//! This module provides the [`Rkyv`] wrapper type for zero-copy serialization
//! of Rust types to/from SQL BLOB columns using the rkyv format.

use std::ops::{Deref, DerefMut};

use sqlx::database::Database;
use sqlx::decode::Decode;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteArgumentValue, SqliteTypeInfo};
use sqlx::types::Type;

/// A wrapper type for rkyv-serialized data in SQL databases.
///
/// `Rkyv<T>` wraps a value of type `T` and provides automatic serialization/deserialization
/// using the [rkyv](https://rkyv.org/) zero-copy framework when storing to or reading from
/// SQL databases.
///
/// # Features
///
/// - **Zero-copy deserialization** - Data can be accessed directly from the buffer without copying
/// - **Compact binary format** - Efficient storage compared to JSON or other text formats
/// - **Type-safe** - Compile-time checking of serialization capabilities
///
/// # Database Support
///
/// Currently implements SQLx traits for SQLite. Data is stored as BLOB.
///
/// # Example
///
/// ```rust,ignore
/// use evento_sql::sql_types::Rkyv;
/// use rkyv::{Archive, Serialize, Deserialize};
///
/// #[derive(Archive, Serialize, Deserialize)]
/// struct MyData {
///     value: i32,
///     name: String,
/// }
///
/// // Wrap data for storage
/// let data = Rkyv(MyData { value: 42, name: "test".into() });
///
/// // Encode to bytes
/// let bytes = data.encode_to()?;
///
/// // Decode from bytes
/// let decoded = Rkyv::<MyData>::decode_from_bytes(&bytes)?;
/// assert_eq!(decoded.value, 42);
/// ```
///
/// # Deref
///
/// `Rkyv<T>` implements `Deref` and `DerefMut` to `T`, allowing transparent access
/// to the inner value.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Rkyv<T: ?Sized>(pub T);

impl<T> From<T> for Rkyv<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for Rkyv<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Rkyv<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> AsRef<T> for Rkyv<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> AsMut<T> for Rkyv<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> Rkyv<T>
where
    T: for<'a> rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >,
{
    /// Serializes the wrapped value to a byte vector using rkyv.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn encode_to(&self) -> Result<Vec<u8>, rkyv::rancor::Error> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.0)?;
        Ok(bytes.to_vec())
    }
}

impl<T> Rkyv<T>
where
    T: rkyv::Archive,
    <T as rkyv::Archive>::Archived: for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    /// Deserializes a value from a byte slice using rkyv.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails or the data is invalid.
    pub fn decode_from_bytes(bytes: &[u8]) -> Result<Self, rkyv::rancor::Error> {
        let data = rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes)?;
        Ok(Self(data))
    }
}

impl<T> Type<sqlx::Sqlite> for Rkyv<T> {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<sqlx::Sqlite>>::type_info()
    }

    fn compatible(ty: &SqliteTypeInfo) -> bool {
        <&[u8] as Type<sqlx::Sqlite>>::compatible(ty)
    }
}

impl<T> Encode<'_, sqlx::Sqlite> for Rkyv<T>
where
    T: for<'a> rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >,
{
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as Database>::ArgumentBuffer<'_>,
    ) -> Result<IsNull, BoxDynError> {
        let data = self.encode_to()?;
        buf.push(SqliteArgumentValue::Blob(std::borrow::Cow::Owned(data)));

        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, sqlx::Sqlite> for Rkyv<T>
where
    T: rkyv::Archive,
    <T as rkyv::Archive>::Archived: for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    Vec<u8>: sqlx::Decode<'r, sqlx::Sqlite>,
{
    fn decode(value: <sqlx::Sqlite as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let decoded = Vec::<u8>::decode(value)?;
        let data = Rkyv::<T>::decode_from_bytes(&decoded[..])?;

        Ok(data)
    }
}
