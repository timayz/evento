use async_trait::async_trait;
use evento_query::{Cursor, CursorType, Edge, PageInfo, QueryResult};
use parking_lot::RwLock;
use serde_json::Value;
use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use crate::{
    engine::Engine,
    error::{Result, StoreError},
    store::{Event, Store, WriteEvent},
};

pub type MemoryStore = Store<Memory>;

#[derive(Debug, Clone, Default)]
pub struct Memory(Arc<RwLock<HashMap<String, Vec<Event>>>>);

impl MemoryStore {
    pub fn new() -> Self {
        Store(Memory::default())
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Engine for Memory {
    async fn write(
        &self,
        aggregate_id: &'_ str,
        write_events: Vec<WriteEvent>,
        original_version: u16,
    ) -> Result<Vec<Event>> {
        if write_events.is_empty() {
            return Ok(vec![]);
        }

        let mut data = self.0.write();
        let events = data.entry(aggregate_id.to_owned()).or_default();

        let mut version = events.last().map(|e| e.version).unwrap_or(0);

        if version != i32::from(original_version) {
            return Err(StoreError::UnexpectedOriginalVersion);
        }

        let start_at = events.len();

        for event in write_events {
            version += 1;

            events.push(event.to_event(aggregate_id, u16::try_from(version)?));
        }

        Ok(events[start_at..events.len()].to_vec())
    }

    async fn insert(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut data = self.0.write();

        for event in events {
            let events = data.entry(event.aggregate_id.to_owned()).or_default();
            events.push(event);
        }

        Ok(())
    }

    async fn read(
        &self,
        first: u16,
        after: Option<CursorType>,
        filters: Option<Vec<Value>>,
        aggregate_id: Option<&'_ str>,
    ) -> Result<QueryResult<Event>> {
        let filters = filters.and_then(|filters| {
            if filters.is_empty() {
                None
            } else {
                Some(filters)
            }
        });

        let mut events = match aggregate_id {
            Some(aggregate_id) => match self.0.read().get(aggregate_id) {
                Some(events) => events.clone(),
                _ => return Ok(QueryResult::default()),
            },
            _ => self
                .0
                .read()
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<Event>>(),
        };

        events.sort_by(|a, b| {
            let cmp = a.created_at.partial_cmp(&b.created_at).unwrap();

            match cmp {
                Ordering::Equal => {}
                _ => return cmp,
            };

            let cmp = a.version.partial_cmp(&b.version).unwrap();

            match cmp {
                Ordering::Equal => a.id.partial_cmp(&b.id).unwrap(),
                _ => cmp,
            }
        });

        let start = (after
            .map(|cursor| Event::from_cursor(&cursor).unwrap_or_default().id)
            .map(|id| events.iter().position(|event| event.id == id).unwrap() as i32)
            .unwrap_or(-1)
            + 1) as usize;

        let end = std::cmp::min(events.len(), usize::from(first + 1));
        let mut events = events[start..end].to_vec();

        let has_more = events.len() == usize::from(first + 1);
        if has_more {
            events.pop();
        }

        let mut edges = Vec::new();

        for event in events.iter() {
            if let Some(filters) = &filters {
                let Some(metadata) = event.to_metadata::<HashMap<String, Value>>()? else {
                    continue;
                };

                let mut matched = false;

                for filter in filters {
                    let filter = serde_json::from_value::<HashMap<String, Value>>(filter.clone())?;

                    matched = filter.iter().all(|(key, v)| metadata.get(key) == Some(v));

                    if matched {
                        break;
                    }
                }

                if !matched {
                    continue;
                }
            }

            edges.push(Edge {
                node: event.clone(),
                cursor: event.to_cursor(),
            });
        }

        let page_info = PageInfo {
            has_next_page: has_more,
            end_cursor: edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        };

        Ok(QueryResult { edges, page_info })
    }

    async fn last(&self) -> Result<Option<Event>> {
        Ok(self.0.read().values().flatten().last().cloned())
    }
}
