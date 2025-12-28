//!
//! #[derive(Debug, Clone, Copy)]
//! pub enum UserList {
//!     Id,
//!     Email,
//!     Name,
//!     CreatedAt,
//! }
//!
//! define_sort_for!(UserListRow, UserList, UserList::Id => {
//!     UserSortByName:    string, UserList::Name,      |s| s.name.clone();
//!     UserSortByCreated: int,    UserList::CreatedAt, |s| s.created_at;
//! });
//! ```

// ============================================================================
// SQLite Implementation
// ============================================================================

/// Internal macro for SQLite implementations.
///
/// This macro is conditionally compiled when the `sqlite` feature is enabled.
/// It generates the [`sql::Bind`] and [`sqlx::FromRow`] implementations for SQLite.
#[cfg(feature = "sqlite")]
#[macro_export]
#[doc(hidden)]
macro_rules! __impl_sqlite {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $column:expr) => {
        impl $crate::sql::Bind for $name {
            type T = $table;
            type I = [Self::T; 2];
            type V = [$crate::sql::Expr; 2];
            type Cursor = Self;

            fn columns() -> Self::I {
                [$column, $id_column]
            }

            fn values(
                cursor: <<Self as $crate::sql::Bind>::Cursor as $crate::cursor::Cursor>::T,
            ) -> Self::V {
                [cursor.v.into(), cursor.i.into()]
            }
        }

        impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for $name {
            fn from_row(row: &'_ sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
                let row = <$row as sqlx::FromRow<'_, sqlx::sqlite::SqliteRow>>::from_row(row)?;
                Ok(Self(row))
            }
        }
    };
}

#[cfg(not(feature = "sqlite"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __impl_sqlite {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $column:expr) => {
        // Empty - no sqlite support
    };
}

// ============================================================================
// Postgres - conditionally defined based on evento's features
// ============================================================================

#[cfg(feature = "postgres")]
#[macro_export]
#[doc(hidden)]
macro_rules! __impl_postgres {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $column:expr) => {
        impl $crate::sql::Bind for $name {
            type T = $table;
            type I = [Self::T; 2];
            type V = [$crate::sql::Expr; 2];
            type Cursor = Self;

            fn columns() -> Self::I {
                [$column, $id_column]
            }

            fn values(
                cursor: <<Self as $crate::sql::Bind>::Cursor as $crate::cursor::Cursor>::T,
            ) -> Self::V {
                [cursor.v.into(), cursor.i.into()]
            }
        }

        impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for $name {
            fn from_row(row: &'_ sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
                let row = <$row as sqlx::FromRow<'_, sqlx::postgres::PgRow>>::from_row(row)?;
                Ok(Self(row))
            }
        }
    };
}

#[cfg(not(feature = "postgres"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __impl_postgres {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $column:expr) => {
        // Empty - no postgres support
    };
}

// ============================================================================
// MySQL - conditionally defined based on evento's features
// ============================================================================

#[cfg(feature = "mysql")]
#[macro_export]
#[doc(hidden)]
macro_rules! __impl_mysql {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $column:expr) => {
        impl $crate::sql::Bind for $name {
            type T = $table;
            type I = [Self::T; 2];
            type V = [$crate::sql::Expr; 2];
            type Cursor = Self;

            fn columns() -> Self::I {
                [$column, $id_column]
            }

            fn values(
                cursor: <<Self as $crate::sql::Bind>::Cursor as $crate::cursor::Cursor>::T,
            ) -> Self::V {
                [cursor.v.into(), cursor.i.into()]
            }
        }

        impl sqlx::FromRow<'_, sqlx::mysql::MySqlRow> for $name {
            fn from_row(row: &'_ sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
                let row = <$row as sqlx::FromRow<'_, sqlx::mysql::MySqlRow>>::from_row(row)?;
                Ok(Self(row))
            }
        }
    };
}

#[cfg(not(feature = "mysql"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __impl_mysql {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $column:expr) => {
        // Empty - no mysql support
    };
}

// ============================================================================
// Main macros
// ============================================================================

#[macro_export]
macro_rules! define_sort_for {
    ($row:ty, $table:ty, $id_column:expr => {
        $($name:ident: $kind:tt, $column:expr, |$s:ident| $value_expr:expr);* $(;)?
    }) => {
        $(
            $crate::define_sort_for!(@impl $name, $row, $table, $id_column, $kind, $column, |$s| $value_expr);
        )*
    };

    (@impl $name:ident, $row:ty, $table:ty, $id_column:expr, int, $column:expr, |$s:ident| $value_expr:expr) => {
        $crate::define_sort_impl!($name, $row, $table, $id_column, $crate::cursor::CursorInt, $column, |$s| $value_expr);
    };

    (@impl $name:ident, $row:ty, $table:ty, $id_column:expr, string, $column:expr, |$s:ident| $value_expr:expr) => {
        $crate::define_sort_impl!($name, $row, $table, $id_column, $crate::cursor::CursorString, $column, |$s| $value_expr);
    };

    (@impl $name:ident, $row:ty, $table:ty, $id_column:expr, bool, $column:expr, |$s:ident| $value_expr:expr) => {
        $crate::define_sort_impl!($name, $row, $table, $id_column, $crate::cursor::CursorBool, $column, |$s| $value_expr);
    };

    (@impl $name:ident, $row:ty, $table:ty, $id_column:expr, float, $column:expr, |$s:ident| $value_expr:expr) => {
        $crate::define_sort_impl!($name, $row, $table, $id_column, $crate::cursor::CursorFloat, $column, |$s| $value_expr);
    };

    (@impl $name:ident, $row:ty, $table:ty, $id_column:expr, ($cursor_type:ty), $column:expr, |$s:ident| $value_expr:expr) => {
        $crate::define_sort_impl!($name, $row, $table, $id_column, $cursor_type, $column, |$s| $value_expr);
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! define_sort_impl {
    ($name:ident, $row:ty, $table:ty, $id_column:expr, $cursor_type:ty, $column:expr, |$s:ident| $value_expr:expr) => {
        pub struct $name($row);

        impl std::ops::Deref for $name {
            type Target = $row;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl From<$row> for $name {
            fn from(row: $row) -> Self {
                Self(row)
            }
        }

        impl From<$name> for $row {
            fn from(sort: $name) -> Self {
                sort.0
            }
        }

        impl AsRef<$row> for $name {
            fn as_ref(&self) -> &$row {
                &self.0
            }
        }

        impl AsMut<$row> for $name {
            fn as_mut(&mut self) -> &mut $row {
                &mut self.0
            }
        }

        impl $crate::cursor::Cursor for $name {
            type T = $cursor_type;

            fn serialize(&self) -> Self::T {
                let $s = self;
                Self::T {
                    i: $s.id.to_owned(),
                    v: $value_expr,
                }
            }
        }

        // Call the cfg-gated helper macros
        // These expand to impls or nothing based on evento's features
        $crate::__impl_sqlite!($name, $row, $table, $id_column, $column);
        $crate::__impl_postgres!($name, $row, $table, $id_column, $column);
        $crate::__impl_mysql!($name, $row, $table, $id_column, $column);
    };
}
