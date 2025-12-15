use std::ops::{Deref, DerefMut};

use sqlx::database::Database;
use sqlx::decode::Decode;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteArgumentValue, SqliteTypeInfo};
use sqlx::types::Type;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bincode<T: ?Sized>(pub T);

impl<T> From<T> for Bincode<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for Bincode<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Bincode<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> AsRef<T> for Bincode<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> AsMut<T> for Bincode<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

// UNSTABLE: for driver use only!
#[doc(hidden)]
impl<T: bincode::Encode> Bincode<T> {
    pub fn encode_to(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let config = bincode::config::standard();

        bincode::encode_to_vec(&self.0, config)
    }
}

// UNSTABLE: for driver use only!
#[doc(hidden)]
impl<'a, T: 'a> Bincode<T>
where
    T: bincode::Decode<()>,
{
    pub fn decode_from_bytes(bytes: &'a [u8]) -> Result<Self, bincode::error::DecodeError> {
        let config = bincode::config::standard();
        let (data, _) = bincode::decode_from_slice(bytes, config)?;

        Ok(Self(data))
    }
}

impl<T> Type<sqlx::Sqlite> for Bincode<T> {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<sqlx::Sqlite>>::type_info()
    }

    fn compatible(ty: &SqliteTypeInfo) -> bool {
        <&[u8] as Type<sqlx::Sqlite>>::compatible(ty)
    }
}

impl<T: bincode::Encode> Encode<'_, sqlx::Sqlite> for Bincode<T> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as Database>::ArgumentBuffer<'_>,
    ) -> Result<IsNull, BoxDynError> {
        let data = self.encode_to()?;
        buf.push(SqliteArgumentValue::Blob(std::borrow::Cow::Owned(data)));

        Ok(IsNull::No)
    }
}

impl<'r, T: bincode::Decode<()>> Decode<'r, sqlx::Sqlite> for Bincode<T>
where
    Vec<u8>: sqlx::Decode<'r, sqlx::Sqlite>,
{
    fn decode(value: <sqlx::Sqlite as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let decoded = Vec::<u8>::decode(value)?;
        let data = Bincode::<T>::decode_from_bytes(&decoded[..])?;

        Ok(data)
    }
}
