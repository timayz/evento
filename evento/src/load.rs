use std::time::Duration;

use thiserror::Error;

use crate::{cursor::Args, Aggregator, Event, Executor};

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("not found")]
    NotFound,

    #[error("too many events to aggregate")]
    TooManyEvents,

    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("bincode.encode >> {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

    #[error("bincode.decode >> {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),

    #[error("base64 decode: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("write: {0}")]
    Write(#[from] super::WriteError),
}

#[derive(Debug, Clone)]
pub struct LoadResult<A: Aggregator> {
    pub item: A,
    pub event: Event,
}

/// Load an aggregate by replaying its events from the event store
///
/// Reconstructs an aggregate's current state by loading and replaying all events
/// for the specified aggregate ID. This function handles snapshots automatically
/// to optimize performance - it loads the latest snapshot and replays only events
/// that occurred after the snapshot.
///
/// # Parameters
///
/// - `executor`: The event store executor (database connection)
/// - `id`: The aggregate ID to load
///
/// # Returns
///
/// Returns a [`LoadResult`] containing:
/// - `item`: The reconstructed aggregate with its current state
/// - `event`: The last event that was applied to the aggregate
///
/// # Errors
///
/// - [`ReadError::NotFound`] if no events exist for the given aggregate ID
/// - [`ReadError::TooManyEvents`] if there are too many events to process (>10 batches)
/// - [`ReadError::BincodeDecode`] if event deserialization fails
/// - [`ReadError::Unknown`] for other database or system errors
///
/// # Examples
///
/// ```no_run
/// use evento::load;
/// # use evento::*;
/// # use bincode::{Encode, Decode};
/// # #[derive(Default, Encode, Decode, Clone, Debug)]
/// # struct User { name: String }
/// # #[evento::aggregator]
/// # impl User {}
///
/// async fn get_user(executor: &evento::Sqlite, user_id: &str) -> anyhow::Result<User> {
///     let result = load::<User, _>(executor, user_id).await?;
///     
///     println!("Loaded user at version {}", result.event.version);
///     println!("Last event timestamp: {}", result.event.timestamp);
///     
///     Ok(result.item)
/// }
/// ```
///
/// # Performance
///
/// The function automatically creates snapshots during loading to speed up future loads.
/// For aggregates with many events, consider the performance implications and ensure
/// your event handlers are efficient.
pub async fn load<A: Aggregator, E: Executor>(
    executor: &E,
    id: impl Into<String>,
) -> Result<LoadResult<A>, ReadError> {
    let id = id.into();
    let (mut aggregator, mut cursor) = match executor
        .get_snapshot(
            A::name().to_owned(),
            A::revision().to_owned(),
            id.to_owned(),
        )
        .await?
    {
        Some((data, cursor)) => {
            let config = bincode::config::standard();
            let (aggregator, _): (A, _) = bincode::decode_from_slice(&data[..], config)?;
            (aggregator, Some(cursor))
        }
        _ => (A::default(), None),
    };

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut loop_count = 0;

    loop {
        let events = executor
            .read_by_aggregator(
                A::name().to_owned(),
                id.to_owned(),
                Args::forward(1000, cursor.clone()),
            )
            .await?;

        for event in events.edges.iter() {
            aggregator.aggregate(&event.node).await?;
        }

        if let (Some(event), Some(cursor)) = (events.edges.last().cloned(), cursor.clone()) {
            let config = bincode::config::standard();
            let data = bincode::encode_to_vec(&aggregator, config)?;

            executor
                .save_snapshot(
                    A::name().to_owned(),
                    A::revision().to_owned(),
                    event.node.aggregator_id,
                    data,
                    cursor,
                )
                .await?;
        }

        if !events.page_info.has_next_page {
            let event = match (cursor, events.edges.last()) {
                (_, Some(event)) => event.node.clone(),
                (Some(cursor), None) => executor.get_event(cursor).await?,
                _ => return Err(ReadError::NotFound),
            };

            return Ok(LoadResult {
                item: aggregator,
                event,
            });
        }

        cursor = events.page_info.end_cursor;

        interval.tick().await;

        loop_count += 1;
        if loop_count > 10 {
            return Err(ReadError::TooManyEvents);
        }
    }
}
