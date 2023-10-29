use async_trait::async_trait;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::context::Context;

#[derive(Clone)]
pub struct Query {
    inner: Arc<RwLock<Context>>,
}

impl Query {
    pub fn new() -> Self {
        Self {
            inner: Arc::default(),
        }
    }

    pub fn data<V: Send + Sync + 'static>(self, v: V) -> Self {
        self.inner.write().insert(v);
        self
    }

    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.inner.read().extract::<T>().clone()
    }

    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.inner.read().get::<T>().cloned()
    }
}

impl Query {
    pub async fn execute<I, T>(&self, input: &I) -> Result<I::Output, QueryError>
    where
        I: QueryHandler,
    {
        input.handle(self).await
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum QueryError {
    Server(String),
    NotFound(String),
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for QueryError {
    fn from(value: E) -> Self {
        QueryError::Server(value.to_string())
    }
}

pub type QueryOutput<O> = Result<O, QueryError>;

#[async_trait]
pub trait QueryHandler {
    type Output;
    async fn handle(&self, query: &Query) -> QueryOutput<Self::Output>;
}
