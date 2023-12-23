//! # Evento Query
//!
//! Evento Query is a crate for handling queries with cursor-based pagination.
//!
//! ## Modules
//!
//! - `cursor`: Module containing definitions related to cursor handling.
//! - `error`: Module containing error definitions.
//! - `query`: Module containing query-related functionality.
//!
//! ## Re-exports
//!
//! This module re-exports items from the `cursor` module, `error` module, and `query` module.
//!
//! ## Features
//!
//! - `pg`: Enable PostgreSQL-specific functionality.
//!
//! ## Examples
//!
//! ```rust
//! use evento_query::{Cursor, CursorType, QueryError, PgQuery, Edge, PageInfo, QueryResult};
//! use sqlx::{Postgres, Executor, PgConnection};
//!
//! #[derive(Clone, Deserialize, Debug, sqlx::FromRow, Default, PartialEq)]
//! pub struct User {
//!     pub id: uuid::Uuid,
//!     pub name: String,
//!     pub age: i32,
//!     pub created_at: chrono::DateTime<chrono::Utc>,
//! }
//!
//! impl Cursor for User {
//!     // Implementation details go here...
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // Establish a PostgreSQL connection
//!     let pool = sqlx::PgPool::connect("your_database_url").await.unwrap();
//!
//!     // Create a PgQuery for the User type
//!     let pg_query = PgQuery::new("SELECT * FROM users");
//!
//!     // Execute the query and retrieve the results
//!     let result: Result<QueryResult<User>, QueryError> = pg_query.fetch_all(&pool).await;
//!
//!     match result {
//!         Ok(query_result) => {
//!             for edge in query_result.edges {
//!                 println!("User ID: {:?}", edge.node.id);
//!                 println!("User Name: {:?}", edge.node.name);
//!                 println!("User Age: {:?}", edge.node.age);
//!                 println!("---");
//!             }
//!
//!             println!("Has Previous Page: {:?}", query_result.page_info.has_previous_page);
//!             println!("Has Next Page: {:?}", query_result.page_info.has_next_page);
//!         }
//!         Err(error) => eprintln!("Query Error: {:?}", error),
//!     }
//! }
//! ```
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod cursor;
mod error;
mod query;

pub use cursor::{Cursor, CursorOrder, CursorType};
pub use error::QueryError;
pub use query::*;
