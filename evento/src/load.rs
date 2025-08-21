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

    #[error("ciborium.ser >> {0}")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("ciborium.de >> {0}")]
    CiboriumDe(#[from] ciborium::de::Error<std::io::Error>),

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

pub async fn load<A: Aggregator, E: Executor>(
    executor: &E,
    id: impl Into<String>,
) -> Result<LoadResult<A>, ReadError> {
    let id = id.into();
    let (mut aggregator, mut cursor) = match executor.get_snapshot::<A>(id.to_owned()).await? {
        Some((data, cursor)) => {
            let aggregator: A = ciborium::from_reader(&data[..])?;
            (aggregator, Some(cursor))
        }
        _ => (A::default(), None),
    };

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut loop_count = 0;

    loop {
        let events = executor
            .read_by_aggregator::<A>(id.to_owned(), Args::forward(1000, cursor.clone()))
            .await?;

        for event in events.edges.iter() {
            aggregator.aggregate(&event.node).await?;
        }

        if let (Some(event), Some(cursor)) = (events.edges.last().cloned(), cursor.clone()) {
            let mut data = vec![];
            ciborium::into_writer(&aggregator, &mut data)?;

            executor
                .save_snapshot::<A>(event.node.aggregator_id, data, cursor)
                .await?;
        }

        if !events.page_info.has_next_page {
            let event = match (cursor, events.edges.last()) {
                (_, Some(event)) => event.node.clone(),
                (Some(cursor), None) => executor.get_event::<A>(cursor).await?,
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
