use std::convert::Infallible;

use async_trait::async_trait;
use axum::{extract::FromRequestParts, http::request::Parts};

pub struct UserLanguage(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for UserLanguage
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let lang = evento_axum::UserLanguage::from_request_parts(parts, state).await?;
        Ok(UserLanguage(
            lang.preferred_languages()
                .first()
                .cloned()
                .unwrap_or("en".to_owned()),
        ))
    }
}
