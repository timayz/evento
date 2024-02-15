use evento::Rule;
use parse_display::{Display, FromStr};

use crate::product::{ProductDetailsHandler, ProductTaskHandler};

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum ProductRule {
    ProductDetails,
    ProductTask,
}

impl From<ProductRule> for String {
    fn from(value: ProductRule) -> Self {
        value.to_string()
    }
}

pub fn rules() -> Vec<Rule> {
    vec![
        Rule::new(ProductRule::ProductDetails).handler("product/**", ProductDetailsHandler),
        Rule::new(ProductRule::ProductTask).handler("product-task/**", ProductTaskHandler),
    ]
}
