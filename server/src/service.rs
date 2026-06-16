//! The tonic service implementation, generic over any [`Executor`].

use std::sync::Arc;

use evento_core::{cursor::Value, Executor};
use tonic::{Request, Response, Status, Streaming};

use crate::{convert, error, proto, subscribe, write};

/// gRPC service exposing an evento [`Executor`].
///
/// Generic over the backend, so it works with any executor — `Sql`, `Fjall`,
/// `AccordExecutor`, the type-erased `Evento` wrapper, `EventoGroup`, or `Rw`.
pub struct EventStoreService<E: Executor> {
    executor: Arc<E>,
}

impl<E: Executor> EventStoreService<E> {
    /// Wraps an executor in a new service.
    pub fn new(executor: E) -> Self {
        Self {
            executor: Arc::new(executor),
        }
    }

    /// Wraps an already-shared executor.
    pub fn from_arc(executor: Arc<E>) -> Self {
        Self { executor }
    }
}

#[tonic::async_trait]
impl<E: Executor> proto::event_store_server::EventStore for EventStoreService<E> {
    async fn write(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::WriteResponse>, Status> {
        let response = write::write(self.executor.as_ref(), request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn read(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<proto::ReadResponse>, Status> {
        let req = request.into_inner();
        let aggregators = convert::event_filters(req.aggregators);
        let routing_key = convert::routing_key(req.routing_key);
        let args = convert::args(req.args)?;

        let result = self
            .executor
            .read(aggregators, routing_key, args)
            .await
            .map_err(error::internal)?;

        Ok(Response::new(convert::read_result_to_proto(result)))
    }

    async fn latest_timestamp(
        &self,
        request: Request<proto::LatestTimestampRequest>,
    ) -> Result<Response<proto::LatestTimestampResponse>, Status> {
        let req = request.into_inner();
        let aggregators = convert::event_filters(req.aggregators);
        let routing_key = convert::routing_key(req.routing_key);

        let timestamp = self
            .executor
            .latest_timestamp(aggregators, routing_key)
            .await
            .map_err(error::internal)?;

        Ok(Response::new(proto::LatestTimestampResponse { timestamp }))
    }

    type SubscribeStream = subscribe::SubscribeStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<proto::SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let inbound = request.into_inner();
        let stream = subscribe::subscribe(self.executor.clone(), inbound);
        Ok(Response::new(stream))
    }

    async fn get_snapshot(
        &self,
        request: Request<proto::GetSnapshotRequest>,
    ) -> Result<Response<proto::GetSnapshotResponse>, Status> {
        let req = request.into_inner();
        let found = self
            .executor
            .get_snapshot(req.aggregate_type, req.aggregate_revision, req.id)
            .await
            .map_err(error::internal)?;

        let snapshot = found.map(|(data, cursor)| proto::Snapshot {
            data,
            cursor: cursor.0,
        });
        Ok(Response::new(proto::GetSnapshotResponse { snapshot }))
    }

    async fn save_snapshot(
        &self,
        request: Request<proto::SaveSnapshotRequest>,
    ) -> Result<Response<proto::SaveSnapshotResponse>, Status> {
        let req = request.into_inner();
        self.executor
            .save_snapshot(
                req.aggregate_type,
                req.aggregate_revision,
                req.id,
                req.data,
                Value(req.cursor),
            )
            .await
            .map_err(error::internal)?;
        Ok(Response::new(proto::SaveSnapshotResponse {}))
    }

    async fn delete_snapshot(
        &self,
        request: Request<proto::DeleteSnapshotRequest>,
    ) -> Result<Response<proto::DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        self.executor
            .delete_snapshot(req.aggregate_type, req.id)
            .await
            .map_err(error::internal)?;
        Ok(Response::new(proto::DeleteSnapshotResponse {}))
    }
}
