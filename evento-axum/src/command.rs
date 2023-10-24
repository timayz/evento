use async_trait::async_trait;
use axum::{
    body::HttpBody,
    extract::{
        rejection::{ExtensionRejection, FormRejection},
        FromRequest, Form,
    },
    http::{Request, StatusCode},
    response::IntoResponse,
    response::Response,
    BoxError, Extension, RequestPartsExt,
};
use evento::{CommandContext, CommandError, CommandHandler};
use evento_store::Event;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use tracing::error;
use validator::Validate;

#[derive(Clone)]
pub struct CommandOutput<T> {
    pub events: Vec<Event>,
    phantom_data: PhantomData<T>,
}

#[derive(Clone)]
pub struct Command<T>(evento::Command<T>);

#[async_trait]
impl<S, B, T> FromRequest<S, B> for Command<T>
where
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
    T: Send + Sync,
    T: DeserializeOwned,
    T: Validate,
    T: CommandHandler,
{
    type Rejection = CommandRejection;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();

        let (mut inner_parts, body) = Request::new(body).into_parts();
        inner_parts.headers = parts.headers.clone();

        let inner_request = Request::from_parts(inner_parts, body);
        let Form(input) = Form::<T>::from_request(inner_request, state).await?;

        let ctx = parts.extract::<Extension<CommandContext>>().await?;
        let output = evento::Command::<T>::execute("fr".to_owned(), ctx.0, input).await?;

        Ok(Self(output))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CommandRejection {
    #[error("{0}")]
    Json(#[from] FormRejection),

    #[error("{0}")]
    Extension(#[from] ExtensionRejection),

    #[error("{0}")]
    Command(#[from] CommandError),
}

impl IntoResponse for CommandRejection {
    fn into_response(self) -> Response {
        if let CommandRejection::Command(CommandError::ValidationErrors(errors)) = self {
            let body = errors
                .iter()
                .map(|error| {
                    format!(
                        r#"<li><span class="evento-error_field">{}</span>: {}</li>"#,
                        error.field, error.message
                    )
                })
                .collect::<Vec<_>>()
                .join("");

            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(r#"<ul class="evento-errors">{body}</ul>"#),
            )
                .into_response();
        }

        if let CommandRejection::Json(rejection) = self {
            return rejection.into_response();
        }

        if let CommandRejection::Extension(rejection) = self {
            return rejection.into_response();
        }

        error!("{self}");

        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}
