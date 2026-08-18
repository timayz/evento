//! Read-model projections, one per snapshot strategy:
//!
//! - [`account_details`] — the full view used by the web examples, with an
//!   in-memory snapshot table (`#[evento::snapshot(memory)]`) and a co-keyed
//!   `Owner` aggregate.
//! - [`account_balance`] — executor-backed snapshots via bitcode derives.
//! - [`account_status`] — no snapshots (`#[evento::snapshot(none)]`); always
//!   replays.

pub mod account_balance;
pub mod account_details;
pub mod account_status;

pub use account_details::{AccountDetailsView, load as load_account_details};
