use std::ops::{Deref, DerefMut};

use sqlx::database::Database;
use sqlx::decode::Decode;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteArgumentValue, SqliteTypeInfo};
use sqlx::types::Type;

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

// UNSTABLE: for driver use only!
#[doc(hidden)]
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
    pub fn encode_to(&self) -> Result<Vec<u8>, rkyv::rancor::Error> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.0)?;
        Ok(bytes.to_vec())
    }
}

// UNSTABLE: for driver use only!
#[doc(hidden)]
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
