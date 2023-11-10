use evento::store::PgStore;
use evento::Rule;
use parse_display::{Display, FromStr};
use sqlx::{migrate::MigrateDatabase, Any, PgPool};

use crate::product::ProductDetailsHandler;

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum ProductRule {
    ProductDetails,
}

impl From<ProductRule> for String {
    fn from(value: ProductRule) -> Self {
        value.to_string()
    }
}

pub async fn rules() -> anyhow::Result<Vec<Rule>> {
    let dsn = "postgres://postgres:postgres@localhost:5432/inventory";
    let _ = Any::create_database(dsn).await;
    let db = PgPool::connect(dsn).await?;
    let store = PgStore::create(&db);

    Ok(vec![Rule::new(ProductRule::ProductDetails)
        .store(store)
        .handler("product/**", ProductDetailsHandler)])
}
