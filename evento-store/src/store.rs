use chrono::{DateTime, Utc};
use evento_query::{CursorType, QueryResult};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{engine::Engine, error::Result, Aggregate, StoreError};

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub cursor: Option<CursorType>,
    pub version: u16,
    pub snapshot_version: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Store {
    pub(crate) engine: Box<dyn Engine>,
}

impl Store {
    pub fn new<E: Engine + 'static>(engine: E) -> Self {
        Self {
            engine: Box::new(engine),
        }
    }

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
        let snapshot_id = format!("snapshot-{aggregate_id}");

        let (mut aggregate, mut cursor, mut version) =
            match self.first_of::<A>(&snapshot_id).await? {
                Some(e) => match (e.version, e.to_metadata::<SnapshotMetadata>()?) {
                    (0, Some(metadata)) => {
                        if A::version() == metadata.snapshot_version {
                            (e.to_data::<A>()?, metadata.cursor, metadata.version)
                        } else {
                            (A::default(), None, 0)
                        }
                    }
                    _ => (A::default(), None, 0),
                },
                _ => (A::default(), None, 0),
            };

        loop {
            let events = self.read_of::<A>(&aggregate_id, first, cursor).await?;

            if events.edges.is_empty() {
                return Ok(None);
            }

            for event in events.edges.iter() {
                aggregate.apply(&event.node);
                version = u16::try_from(event.node.version)?;
            }

            let snapshot = Event {
                name: "_snapshot".to_owned(),
                aggregate_id: A::aggregate_id(&snapshot_id),
                data: serde_json::to_value(&aggregate)?,
                metadata: Some(serde_json::to_value(SnapshotMetadata {
                    cursor: events.page_info.end_cursor.clone(),
                    version,
                    snapshot_version: A::version(),
                    created_at: Utc::now(),
                })?),
                ..Event::default()
            };
            // snapshot.aggregate_id = A::aggregate_id(&snapshot_id);
            // snapshot.name = "_snapshot".to_owned();
            // snapshot.version = 0;
            // snapshot.data = serde_json::to_value(&aggregate)?;
            // snapshot.metadata = Some(serde_json::to_value(SnapshotMetadata {
            //     cursor: events.page_info.end_cursor.clone(),
            //     version,
            //     snapshot_version: A::version(),
            //     created_at: Utc::now(),
            // })?);

            self.engine.upsert(snapshot).await?;

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
        self.engine
            .write(
                A::aggregate_id(aggregate_id).as_str(),
                events,
                original_version,
            )
            .await
    }

    pub async fn insert(&self, events: Vec<Event>) -> Result<()> {
        self.engine.insert(events).await
    }

    pub async fn read(
        &self,
        first: u16,
        after: Option<CursorType>,
        filters: Option<Vec<Value>>,
    ) -> Result<QueryResult<Event>> {
        self.engine.read(first, after, filters, None).await
    }

    pub async fn read_of<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
        first: u16,
        after: Option<CursorType>,
    ) -> Result<QueryResult<Event>> {
        let aggregate_id = A::aggregate_id(aggregate_id);

        self.engine
            .read(first, after, None, Some(aggregate_id.as_str()))
            .await
    }

    pub async fn first_of<A: Aggregate>(
        &self,
        aggregate_id: impl Into<String>,
    ) -> Result<Option<Event>> {
        let events = self.read_of::<A>(aggregate_id, 1, None).await?;

        Ok(events.edges.first().map(|e| e.node.clone()))
    }

    pub async fn last(&self) -> Result<Option<Event>> {
        self.engine.last().await
    }
}

#[derive(Debug, Clone, Default)]
pub struct WriteEvent {
    pub name: String,
    pub data: Value,
    pub metadata: Option<Value>,
}

impl WriteEvent {
    pub fn new<N: Into<String>>(name: N) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn to_event(&self, aggregate_id: impl Into<String>, version: u16) -> Event {
        Event {
            name: self.name.to_owned(),
            aggregate_id: aggregate_id.into(),
            version: i32::from(version),
            data: self.data.clone(),
            metadata: self.metadata.clone(),
            ..Default::default()
        }
    }

    pub fn data<D: Serialize>(mut self, value: D) -> Result<Self> {
        self.data = serde_json::to_value(&value)?;

        Ok(self)
    }

    pub fn metadata<M: Serialize>(mut self, value: M) -> Result<Self> {
        let metadata = serde_json::to_value(&value)?;

        if !metadata.is_object() {
            return Err(StoreError::MetadataInvalidObjectType);
        }

        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn to_metadata<D: DeserializeOwned>(&self) -> Result<Option<D>> {
        if let Some(metadata) = self.metadata.clone() {
            Ok(Some(serde_json::from_value(metadata)?))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "pg", derive(sqlx::FromRow))]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub aggregate_id: String,
    pub version: i32,
    pub data: Value,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl Event {
    pub fn to_data<D: DeserializeOwned>(&self) -> Result<D> {
        Ok(serde_json::from_value(self.data.clone())?)
    }

    pub fn data<M: Serialize>(mut self, value: M) -> Result<Self> {
        let data = serde_json::to_value(&value)?;

        self.data = data;

        Ok(self)
    }

    pub fn to_metadata<D: DeserializeOwned>(&self) -> Result<Option<D>> {
        if let Some(metadata) = self.metadata.clone() {
            Ok(Some(serde_json::from_value(metadata)?))
        } else {
            Ok(None)
        }
    }

    pub fn metadata<M: Serialize>(mut self, value: M) -> Result<Self> {
        let metadata = serde_json::to_value(&value)?;

        if !metadata.is_object() {
            return Err(StoreError::MetadataInvalidObjectType);
        }

        self.metadata = Some(metadata);

        Ok(self)
    }

    pub fn aggregate_details(&self) -> Option<(String, String)> {
        self.aggregate_id
            .split_once('#')
            .map(|(aggregate_type, id)| (aggregate_type.to_owned(), id.to_owned()))
    }
}

impl Default for Event {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            name: String::default(),
            aggregate_id: String::default(),
            version: i32::default(),
            data: Value::default(),
            metadata: None,
            created_at: Utc::now(),
        }
    }
}
