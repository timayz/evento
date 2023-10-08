use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Result, StoreError};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
