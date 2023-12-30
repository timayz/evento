use std::time::Duration;

use async_trait::async_trait;
use evento::{
    store::{Applier, Event, WriteEvent},
    Aggregate, Command, CommandError, CommandHandler, CommandOutput, ConsumerContext, RuleHandler,
};
use nanoid::nanoid;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use validator::Validate;

use super::{
    Created, Deleted, Edited, GenerateProductsRequested, ProductEvent, ProductTaskEvent,
    ThumbnailChanged, VisibilityChanged,
};

#[derive(Debug, Deserialize, Validate)]
pub struct CreateProductInput {
    #[validate(length(min = 3, max = 25))]
    pub name: String,
}

#[async_trait]
impl CommandHandler for CreateProductInput {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let id = nanoid!(10);

        let events = cmd
            .publish::<Product, _>(
                &id,
                ProductEvent::Created.data(Created {
                    name: self.name.to_owned(),
                })?,
                0,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct DeleteProductInput {
    #[validate(length(equal = 10))]
    pub id: String,
}

#[async_trait]
impl CommandHandler for DeleteProductInput {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Deleted.data(Deleted { deleted: true })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct ChangeProductVisibilityInput {
    #[validate(length(equal = 10))]
    pub id: String,
    pub visible: bool,
}

#[async_trait]
impl CommandHandler for ChangeProductVisibilityInput {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::VisibilityChanged.data(VisibilityChanged {
                    visible: self.visible,
                })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Deserialize, Validate)]
pub struct EditProductInput {
    #[validate(length(equal = 10))]
    pub id: String,
    #[validate(length(min = 3, max = 25))]
    pub name: String,
    #[validate(length(min = 3, max = 255))]
    pub description: String,
    #[validate(length(min = 3, max = 50))]
    pub category: String,
    #[validate(length(equal = 2))]
    pub visible: Option<String>,
    #[validate(range(min = 0))]
    pub stock: i32,
    #[validate(range(min = 0))]
    pub price: f32,
}

#[async_trait]
impl CommandHandler for EditProductInput {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let (_, original_version) = load_product(cmd, &self.id).await?;

        let events = cmd
            .publish::<Product, _>(
                &self.id,
                ProductEvent::Edited.data(Edited {
                    name: self.name.to_owned(),
                    description: self.description.to_owned(),
                    category: self.category.to_owned(),
                    price: self.price,
                    stock: self.stock,
                    visible: self.visible.is_some(),
                })?,
                original_version,
            )
            .await?;

        Ok(events)
    }
}

async fn load_product(ctx: &Command, id: &str) -> Result<(Product, u16), CommandError> {
    let Some((product, e)) = ctx.load::<Product, _>(id).await? else {
        return Err(CommandError::NotFound(format!("product {id} not found")));
    };

    if product.deleted {
        return Err(CommandError::NotFound(format!("product {id} not found")));
    }

    Ok((product, e))
}

#[derive(Default, Serialize, Deserialize, Aggregate)]
pub struct Product {
    pub name: String,
    pub description: String,
    pub category: String,
    pub thumbnail: String,
    pub visible: bool,
    pub stock: i32,
    pub price: f32,
    pub deleted: bool,
}

impl Applier for Product {
    fn apply(&mut self, event: &Event) {
        let product_event: ProductEvent = event.name.parse().unwrap();

        match product_event {
            ProductEvent::Created => {
                let data: Created = event.to_data().unwrap();
                self.name = data.name;
            }
            ProductEvent::Edited => {
                let data: Edited = event.to_data().unwrap();
                self.name = data.name;
                self.description = data.description;
                self.stock = data.stock;
                self.visible = data.visible;
                self.price = data.price;
            }
            ProductEvent::VisibilityChanged => {
                let data: VisibilityChanged = event.to_data().unwrap();
                self.visible = data.visible;
            }
            ProductEvent::Deleted => {
                let data: Deleted = event.to_data().unwrap();
                self.deleted = data.deleted;
            }
            ProductEvent::ThumbnailChanged => {
                let data: ThumbnailChanged = event.to_data().unwrap();
                self.thumbnail = data.thumbnail;
            }
        }
    }
}

impl ProductEvent {
    pub fn data<D: Serialize>(&self, value: D) -> evento::store::Result<WriteEvent> {
        WriteEvent::new(self.to_string()).data(value)
    }
}

#[derive(Deserialize, Validate)]
pub struct GenerateProductsInput {
    #[validate(range(min = 0))]
    pub skip: u16,
}

#[async_trait]
impl CommandHandler for GenerateProductsInput {
    async fn handle(&self, cmd: &Command) -> CommandOutput {
        let id = nanoid!(10);

        let events = cmd
            .publish::<ProductTask, _>(
                &id,
                ProductTaskEvent::GenerateProductsRequested.data(GenerateProductsRequested {
                    skip: self.skip.to_owned(),
                })?,
                0,
            )
            .await?;

        Ok(events)
    }
}

#[derive(Debug, Deserialize)]
pub struct DummyJsonProduct {
    pub title: String,
    pub description: String,
    pub price: f32,
    pub stock: i32,
    pub category: String,
    pub thumbnail: String,
}

#[derive(Debug, Deserialize)]
pub struct DummyJsonPayload {
    pub products: Vec<DummyJsonProduct>,
}

#[derive(Clone)]
pub struct ProductTaskHandler;

#[async_trait]
impl RuleHandler for ProductTaskHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> anyhow::Result<()> {
        let event_name: ProductTaskEvent = event.name.parse()?;

        match event_name {
            ProductTaskEvent::GenerateProductsRequested => {
                let data: GenerateProductsRequested = event.to_data().unwrap();

                let payload: DummyJsonPayload = ureq::get(&format!(
                    "https://dummyjson.com/products?skip={}",
                    data.skip
                ))
                .call()?
                .into_json()?;

                for product in payload.products {
                    let id = nanoid!(10);

                    let num = rand::thread_rng().gen_range(0..1000);
                    sleep(Duration::from_millis(num)).await;

                    ctx.publish_all::<Product, _>(
                        &id,
                        vec![
                            ProductEvent::Created.data(Created {
                                name: product.title.to_owned(),
                            })?,
                            ProductEvent::Edited.data(Edited {
                                name: product.title.to_owned(),
                                description: product.description.to_owned(),
                                price: product.price.to_owned(),
                                category: product.category.to_owned(),
                                stock: product.stock.to_owned(),
                                visible: true,
                            })?,
                            ProductEvent::ThumbnailChanged.data(ThumbnailChanged {
                                thumbnail: product.thumbnail.to_owned(),
                            })?,
                        ],
                        0,
                    )
                    .await?;
                }
            }
        };

        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize, Aggregate)]
pub struct ProductTask;

impl Applier for ProductTask {
    fn apply(&mut self, _event: &Event) {
        unreachable!();
    }
}

impl ProductTaskEvent {
    pub fn data<D: Serialize>(&self, value: D) -> evento::store::Result<WriteEvent> {
        WriteEvent::new(self.to_string()).data(value)
    }
}
