use async_trait::async_trait;
use axum::{
    body::HttpBody,
    extract::{
        rejection::{ExtensionRejection, QueryRejection as AxumQueryRejection},
        FromRequest, Query as AxumQuery,
    },
    http::Request,
    response::IntoResponse,
    response::Response,
    BoxError, Extension, RequestPartsExt,
};
use evento::{QueryError, QueryHandler};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use tracing::error;

#[derive(Clone, Debug)]
pub struct Query<T, R, O>
where
    T: QueryHandler<Output = O>,
{
    pub output: O,
    input: PhantomData<T>,
    render: PhantomData<R>,
}

#[async_trait]
impl<S, B, T, R, O> FromRequest<S, B> for Query<T, R, O>
where
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
    T: Send + Sync,
    T: DeserializeOwned,
    T: QueryHandler<Output = O>,
    R: QueryRender,
{
    type Rejection = QueryRejection;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();
        let query = parts.extract::<Extension<evento::Query>>().await?;
        let req = Request::from_parts(parts, body);
        let AxumQuery(input) = AxumQuery::<T>::from_request(req, state).await?;
        let output = query.0.execute::<T, R>(&input).await;

        match output {
            Ok(output) => Ok(Self {
                output,
                input: PhantomData,
                render: PhantomData,
            }),
            Err(err) => match err {
                QueryError::Server(msg) => Err(QueryRejection::Query(R::server(&query.0, msg))),
                QueryError::NotFound(msg) => {
                    Err(QueryRejection::Query(R::not_found(&query.0, msg)))
                }
            },
        }
    }
}

pub trait QueryRender {
    fn not_found(cmd: &evento::Query, msg: String) -> Response;
    fn server(cmd: &evento::Query, msg: String) -> Response;
}

#[derive(thiserror::Error, Debug)]
pub enum QueryRejection {
    #[error("{0}")]
    AxumQuery(#[from] AxumQueryRejection),

    #[error("{0}")]
    Extension(#[from] ExtensionRejection),

    #[error("query")]
    Query(Response),

    #[error("infallible")]
    Infallible,
}

impl IntoResponse for QueryRejection {
    fn into_response(self) -> Response {
        match self {
            QueryRejection::AxumQuery(rejection) => rejection.into_response(),
            QueryRejection::Extension(rejection) => rejection.into_response(),
            QueryRejection::Query(res) => res,
            _ => unreachable!(),
        }
    }
}
