use super::{
    sources::{AcceptLanguageSource, PathSource, QuerySource},
    UserLanguageConfig, UserLanguageSource,
};
use axum::{
    async_trait, extract::FromRequestParts, http::request::Parts, Extension, RequestPartsExt,
};
use std::{
    convert::Infallible,
    sync::{Arc, OnceLock},
};

/// TBD
#[derive(Debug, Clone)]
pub struct UserLanguage {
    preferred_languages: Vec<String>,
}

impl UserLanguage {
    /// TBD
    pub fn default_sources() -> &'static Vec<Arc<dyn UserLanguageSource>> {
        static DEFAULT_SOURCES: OnceLock<Vec<Arc<dyn UserLanguageSource>>> = OnceLock::new();

        DEFAULT_SOURCES.get_or_init(|| {
            vec![
                Arc::new(QuerySource::new("lang")),
                Arc::new(PathSource::new("lang")),
                Arc::new(AcceptLanguageSource),
            ]
        })
    }

    /// TBD
    pub fn preferred_languages(&self) -> &[String] {
        self.preferred_languages.as_slice()
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for UserLanguage
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let (sources, _fallback_language) =
            match parts.extract::<Extension<UserLanguageConfig>>().await {
                Ok(Extension(config)) => (Some(config.sources), Some(config.fallback_language)),
                Err(_) => (None, None),
            };

        let sources = sources.as_ref().unwrap_or(Self::default_sources());

        let mut preferred_languages = Vec::<String>::new();

        for source in sources {
            let languages = source.languages_from_parts(parts).await;
            preferred_languages.extend(languages);
        }

        Ok(UserLanguage {
            preferred_languages,
        })
    }
}
