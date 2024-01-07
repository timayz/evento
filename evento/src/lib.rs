#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod command;
mod consumer;
mod context;
mod data;
mod engine;
mod error;
mod producer;
mod query;

pub use command::*;
pub use consumer::*;
pub use context::Context;
pub use data::Data;
pub use engine::*;
pub use error::*;
pub use evento_macro::*;
pub use evento_store as store;
pub use evento_store::Aggregate;
pub use producer::*;
pub use query::*;
