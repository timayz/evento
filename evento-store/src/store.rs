use evento_query::{CursorType, QueryResult};
use serde_json::Value;

use crate::{engine::Engine, error::Result, event::Event, Aggregate, WriteEvent};

#[derive(Clone, Debug)]
pub struct Store<E: Engine>(pub(crate) E);

impl<E: Engine> Store<E> {
    pub async fn load<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
    ) -> Result<Option<(A, u16)>> {
        self.load_with(aggregate_id, 100).await
    }

    pub async fn load_with<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
        first: u16,
    ) -> Result<Option<(A, u16)>> {
        let aggregate_id = aggregate_id.into();
        let mut aggregate = A::default();
        let mut cursor = None;
        let mut version = 0;

        loop {
            let events = self.read_of::<A>(&aggregate_id, first, cursor).await?;

            if events.edges.is_empty() {
                return Ok(None);
            }

            for event in events.edges.iter() {
                aggregate.apply(&event.node);
                version = u16::try_from(event.node.version)?;
            }

            if !events.page_info.has_next_page {
                break;
            }

            cursor = events.page_info.end_cursor;
        }

        Ok(Some((aggregate, version)))
    }

    pub async fn write<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
        event: WriteEvent,
        original_version: u16,
    ) -> Result<Event> {
        let events = self
            .write_all::<A>(aggregate_id, vec![event], original_version)
            .await?;

        match events.first() {
            Some(event) => Ok(event.clone()),
            _ => Err(crate::StoreError::EmptyWriteEvent),
        }
    }

    pub async fn write_all<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
        events: Vec<WriteEvent>,
        original_version: u16,
    ) -> Result<Vec<Event>> {
        self.0
            .write(
                A::aggregate_id(aggregate_id).as_str(),
                events,
                original_version,
            )
            .await
    }

    pub async fn read<A: Aggregate>(
        &self,
        first: u16,
        after: Option<CursorType>,
        filters: Option<Vec<Value>>,
    ) -> Result<QueryResult<Event>> {
        self.0.read(first, after, filters, None).await
    }

    pub async fn read_of<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
        first: u16,
        after: Option<CursorType>,
    ) -> Result<QueryResult<Event>> {
        let aggregate_id = A::aggregate_id(aggregate_id);

        self.0
            .read(first, after, None, Some(aggregate_id.as_str()))
            .await
    }

    pub async fn first_of<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
    ) -> Result<Option<Event>> {
        let aggregate_id = A::aggregate_id(aggregate_id);

        let events = self
            .0
            .read(1, None, None, Some(aggregate_id.as_str()))
            .await?;

        Ok(events.edges.first().map(|e| e.node.clone()))
    }

    pub async fn last(&self) -> Result<Option<Event>> {
        self.0.last().await
    }
}
