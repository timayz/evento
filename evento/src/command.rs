mod en;
mod fr;

use async_trait::async_trait;
use evento_store::{Aggregate, Event, Result as StoreResult, WriteEvent};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use tracing::{error, warn};
use validator::{Validate, ValidationError, ValidationErrors, ValidationErrorsKind};

use crate::{context::Context, Producer};

#[derive(Clone)]
pub struct Command {
    inner: Arc<RwLock<Context>>,
    producer: Producer,
}

impl Command {
    pub fn new(producer: &Producer) -> Self {
        Self {
            inner: Arc::default(),
            producer: producer.clone(),
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

impl Command {
    pub async fn execute<I, T>(&self, translater: T, input: &I) -> Result<Vec<Event>, CommandError>
    where
        T: Translater,
        I: Validate,
        I: CommandHandler,
    {
        if let Err(errs) = input.validate() {
            let mut errors = HashMap::new();

            for (path, err) in errs.errors() {
                errors.extend(display_errors(&translater, err, path));
            }

            return Err(CommandError::Validation(errors));
        }

        input.handle(self).await
    }
}

#[derive(Debug, Clone)]
pub enum CommandError {
    Server(String),
    Validation(HashMap<String, Vec<String>>),
    NotFound(String),
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for CommandError {
    fn from(value: E) -> Self {
        CommandError::Server(value.to_string())
    }
}

pub type CommandOutput = Result<Vec<Event>, CommandError>;

#[async_trait]
pub trait CommandHandler {
    async fn handle(&self, ctx: &Command) -> CommandOutput;
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
) -> HashMap<String, Vec<String>> {
    let mut full_path = String::new();

    if let Err(e) = write!(&mut full_path, "{}.", path) {
        error!("{e}");
    }

    let base_len = full_path.len();
    let mut field_errors = HashMap::new();

    for (path, err) in errs.errors() {
        if let Err(e) = write!(&mut full_path, "{}", path) {
            error!("{e}");
        }

        field_errors.extend(display_errors(translater, err, &full_path));
        full_path.truncate(base_len);
    }

    field_errors
}

fn display_errors(
    translater: &impl Translater,
    errs: &ValidationErrorsKind,
    path: &str,
) -> HashMap<String, Vec<String>> {
    match errs {
        ValidationErrorsKind::Field(errs) => {
            let errors = errs
                .iter()
                .map(|err| display_error(translater, err))
                .collect();

            let mut field_errors = HashMap::new();
            field_errors.insert(path.to_owned(), errors);

            field_errors
        }
        ValidationErrorsKind::Struct(errs) => display_struct(translater, errs, path),
        ValidationErrorsKind::List(errs) => {
            let mut full_path = String::new();

            if let Err(e) = write!(&mut full_path, "{}", path) {
                error!("{e}");
            }

            let base_len = full_path.len();
            let mut field_errors = HashMap::new();

            for (idx, err) in errs.iter() {
                if let Err(e) = write!(&mut full_path, "[{}]", idx) {
                    error!("{e}");
                }

                field_errors.extend(display_struct(translater, err, &full_path));
                full_path.truncate(base_len);
            }

            field_errors
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

pub trait Translater: Sized {
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
