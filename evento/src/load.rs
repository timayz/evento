
use thiserror::Error;

use crate::{
    config::{DEFAULT_BATCH_SIZE, MAX_PAGINATION_LOOPS, PAGINATION_INTERVAL},
    cursor::{Args, Cursor}, 
    Aggregator, 
    Event, 
    Executor
};

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
/// - [`ReadError::TooManyEvents`] if there are too many events to process
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
    
    // Load from snapshot if available
    let (mut aggregator, initial_cursor) = load_from_snapshot::<A, E>(executor, &id).await?;
    
    // Paginate through remaining events
    let last_event = paginate_and_apply_events::<A, E>(executor, &id, &mut aggregator, initial_cursor).await?;
    
    Ok(LoadResult {
        item: aggregator,
        event: last_event,
    })
}

/// Load an aggregate from its latest snapshot, if available
async fn load_from_snapshot<A: Aggregator, E: Executor>(
    executor: &E,
    id: &str,
) -> Result<(A, Option<crate::cursor::Value>), ReadError> {
    match executor.get_snapshot::<A>(id.to_owned()).await? {
        Some((data, cursor)) => {
            let config = bincode::config::standard();
            let (aggregator, _): (A, _) = bincode::decode_from_slice(&data[..], config)?;
            Ok((aggregator, Some(cursor)))
        }
        None => Ok((A::default(), None)),
    }
}

/// Paginate through events and apply them to the aggregate
async fn paginate_and_apply_events<A: Aggregator, E: Executor>(
    executor: &E,
    id: &str,
    aggregator: &mut A,
    mut cursor: Option<crate::cursor::Value>,
) -> Result<Event, ReadError> {
    let mut interval = tokio::time::interval(PAGINATION_INTERVAL);
    let mut loop_count = 0;
    let mut last_event_cursor = cursor.clone();

    loop {
        let events = executor
            .read_by_aggregator::<A>(id.to_owned(), Args::forward(DEFAULT_BATCH_SIZE as u16, cursor.clone()))
            .await?;

        // Apply all events in this batch
        for event in events.edges.iter() {
            aggregator.aggregate(&event.node).await?;
        }

        // Create snapshot after processing this batch
        if let Some(last_event) = events.edges.last() {
            create_snapshot(executor, aggregator, &last_event.node).await?;
            last_event_cursor = Some(last_event.node.serialize_cursor()?);
        }

        // Check if we're done
        if !events.page_info.has_next_page {
            return resolve_final_event::<A, E>(executor, cursor, events.edges.last(), last_event_cursor).await;
        }

        // Prepare for next iteration
        cursor = events.page_info.end_cursor;
        interval.tick().await;

        // Prevent infinite loops
        loop_count += 1;
        if loop_count > MAX_PAGINATION_LOOPS {
            return Err(ReadError::TooManyEvents);
        }
    }
}

/// Create a snapshot of the current aggregate state
async fn create_snapshot<A: Aggregator, E: Executor>(
    executor: &E,
    aggregator: &A,
    last_event: &Event,
) -> Result<(), ReadError> {
    let config = bincode::config::standard();
    let data = bincode::encode_to_vec(aggregator, config)?;
    let cursor = last_event.serialize_cursor()?;

    executor
        .save_snapshot::<A>(last_event.aggregator_id.clone(), data, cursor)
        .await?;

    Ok(())
}

/// Resolve the final event to return in the LoadResult
async fn resolve_final_event<A: Aggregator, E: Executor>(
    executor: &E,
    initial_cursor: Option<crate::cursor::Value>,
    last_edge: Option<&crate::cursor::Edge<Event>>,
    last_event_cursor: Option<crate::cursor::Value>,
) -> Result<Event, ReadError> {
    match (last_edge, initial_cursor, last_event_cursor) {
        // We processed events, return the last one
        (Some(edge), _, _) => Ok(edge.node.clone()),
        // No events processed, but we had a snapshot cursor
        (None, Some(cursor), _) => executor.get_event::<A>(cursor).await,
        // No events processed, and we had events but got a cursor from them
        (None, None, Some(cursor)) => executor.get_event::<A>(cursor).await,
        // No events at all
        _ => Err(ReadError::NotFound),
    }
}
