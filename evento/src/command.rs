mod en;
mod fr;

use async_trait::async_trait;
use evento_store::{Aggregate, Event, Result as StoreResult, WriteEvent};
use parking_lot::RwLock;
use std::fmt::Write;
use std::{marker::PhantomData, sync::Arc};
use tracing::{error, warn};
use validator::{Validate, ValidationError, ValidationErrors, ValidationErrorsKind};

use crate::{context::Context, Producer};

#[derive(Clone)]
pub struct CommandContext {
    inner: Arc<RwLock<Context>>,
    producer: Producer,
}

impl CommandContext {
    pub fn new(producer: &Producer) -> Self {
        Self {
            inner: Arc::default(),
            producer: producer.clone(),
        }
    }

    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.inner.read().extract::<T>().clone()
    }

    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.inner.read().get::<T>().cloned()
    }

    pub async fn publish<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        event: WriteEvent,
        original_version: u16,
    ) -> StoreResult<Vec<Event>> {
        self.publish_all::<A, I>(id, vec![event], original_version)
            .await
    }

    pub async fn publish_all<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
        events: Vec<WriteEvent>,
        original_version: u16,
    ) -> StoreResult<Vec<Event>> {
        self.producer
            .publish_all::<A, I>(id, events, original_version)
            .await
    }

    pub async fn load<A: Aggregate, I: Into<String>>(
        &self,
        id: I,
    ) -> StoreResult<Option<(A, u16)>> {
        self.producer.load::<A, I>(id).await
    }
}

#[derive(Clone)]
pub struct Command<I> {
    pub events: Vec<Event>,
    phantom_data: PhantomData<I>,
}

impl<I> Command<I>
where
    I: Validate,
    I: CommandHandler,
{
    pub async fn execute(
        translater: impl Translater,
        ctx: CommandContext,
        input: I,
    ) -> Result<Self, CommandError> {
        if let Err(errors) = input.validate() {
            let errors = errors
                .errors()
                .iter()
                .flat_map(|(path, err)| display_errors(&translater, err, path))
                .collect::<Vec<_>>();

            return Err(CommandError::ValidationErrors(errors));
        }

        let events = input.handle(&ctx).await?;

        Ok(Self {
            events,
            phantom_data: PhantomData,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CommandError {
    #[error("{0}")]
    Any(#[from] anyhow::Error),

    #[error("{0:?}")]
    ValidationErrors(Vec<ErrorMessage>),
}

#[async_trait]
pub trait CommandHandler {
    async fn handle(&self, ctx: &CommandContext) -> anyhow::Result<Vec<Event>>;
}

#[derive(Debug)]
pub struct ErrorMessage {
    pub field: String,
    pub message: String,
}

fn display_error(translater: &impl Translater, err: &ValidationError) -> String {
    if let Some(msg) = err.message.as_ref() {
        return msg.to_string();
    }

    let t = match err.code.to_string().as_str() {
        "email" => TranslateType::Email,
        "required" => TranslateType::Required,
        "phone" => TranslateType::Phone,
        "length" => {
            match (
                err.params.get("min").and_then(|v| v.as_i64()),
                err.params.get("max").and_then(|v| v.as_i64()),
                err.params.get("equal").and_then(|v| v.as_i64()),
            ) {
                (None, None, Some(equal)) => TranslateType::LengthEqual(equal),
                (None, Some(max), None) => TranslateType::LengthMax(max),
                (Some(min), None, None) => TranslateType::LengthMin(min),
                (Some(min), Some(max), None) => TranslateType::LengthMinMax(min, max),
                _ => TranslateType::Unknown(err.to_string()),
            }
        }
        _ => {
            warn!(
                "unknown {} when trying to translate validation error",
                err.code
            );

            TranslateType::Unknown(err.to_string())
        }
    };

    translater.translate(t)
}

fn display_struct(
    translater: &impl Translater,
    errs: &ValidationErrors,
    path: &str,
) -> Vec<ErrorMessage> {
    let mut full_path = String::new();

    if let Err(e) = write!(&mut full_path, "{}.", path) {
        error!("{e}");
    }

    let base_len = full_path.len();
    let mut views = vec![];

    for (path, err) in errs.errors() {
        if let Err(e) = write!(&mut full_path, "{}", path) {
            error!("{e}");
        }

        views.extend(display_errors(translater, err, &full_path));
        full_path.truncate(base_len);
    }

    views
}

fn display_errors(
    translater: &impl Translater,
    errs: &ValidationErrorsKind,
    path: &str,
) -> Vec<ErrorMessage> {
    match errs {
        ValidationErrorsKind::Field(errs) => errs
            .iter()
            .map(|err| ErrorMessage {
                field: path.to_owned(),
                message: display_error(translater, err),
            })
            .collect(),
        ValidationErrorsKind::Struct(errs) => display_struct(translater, errs, path),
        ValidationErrorsKind::List(errs) => {
            let mut full_path = String::new();

            if let Err(e) = write!(&mut full_path, "{}", path) {
                error!("{e}");
            }

            let base_len = full_path.len();
            let mut views = vec![];

            for (idx, err) in errs.iter() {
                if let Err(e) = write!(&mut full_path, "[{}]", idx) {
                    error!("{e}");
                }

                views.extend(display_struct(translater, err, &full_path));
                full_path.truncate(base_len);
            }

            views
        }
    }
}

pub enum TranslateType {
    Unknown(String),
    Email,
    Required,
    Phone,
    LengthMin(i64),
    LengthMax(i64),
    LengthMinMax(i64, i64),
    LengthEqual(i64),
}

pub trait Translater {
    fn translate(&self, t: TranslateType) -> String;
}

impl Translater for String {
    fn translate(&self, t: TranslateType) -> String {
        match self.as_str() {
            "fr" => fr::Translater.translate(t),
            _ => en::Translater.translate(t),
        }
    }
}
