use anyhow::{anyhow, Result};
use async_trait::async_trait;
use evento::{Consumer, ConsumerContext, Engine, Producer, PublisherEvent, Rule, RuleHandler};
use evento_macro::Aggregate;
use evento_store::{Aggregate, Applier, Event, Store, WriteEvent};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};
use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};

#[derive(Display, FromStr, PublisherEvent)]
#[display(style = "kebab-case")]
pub enum UserEvent {
    Created,
    DisplayNameUpdated,
    AccountDeleted,
}

#[derive(Serialize, Deserialize)]
pub struct Created {
    pub display_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct DisplayNameUpdated {
    pub display_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct AccountDeleted {
    pub deleted: bool,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq, Aggregate)]
pub struct User {
    pub display_name: String,
    pub deleted: bool,
}

impl Applier for User {
    fn apply(&mut self, event: &Event) {
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created => {
                let data: Created = event.to_data().unwrap();
                self.display_name = data.display_name;
            }
            UserEvent::DisplayNameUpdated => {
                let data: DisplayNameUpdated = event.to_data().unwrap();
                self.display_name = data.display_name;
            }
            UserEvent::AccountDeleted => {
                let data: AccountDeleted = event.to_data().unwrap();
                self.deleted = data.deleted;
            }
        }
    }
}

type Users = Arc<RwLock<HashMap<String, User>>>;

#[derive(Clone)]
struct UsersHandler(bool);

#[async_trait]
impl RuleHandler for UsersHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<()> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created => {
                let mut users = state.users.write().await;
                let data: Created = event.to_data().unwrap();
                let user = User {
                    display_name: data.display_name,
                    deleted: false,
                };
                users.insert(User::from_aggregate_id(&event.aggregate_id), user.clone());
            }
            UserEvent::DisplayNameUpdated => {
                let mut users = state.users.write().await;
                let data: DisplayNameUpdated = event.to_data().unwrap();
                let Some(user) =
                    users.get_mut(User::from_aggregate_id(&event.aggregate_id).as_str())
                else {
                    return Ok(());
                };

                user.display_name = if self.0 {
                    format!(
                        "{} owned by {}",
                        data.display_name,
                        ctx.name.unwrap_or_default()
                    )
                } else {
                    data.display_name
                };
            }
            UserEvent::AccountDeleted => {
                let mut users = state.users.write().await;
                users.remove(User::from_aggregate_id(&event.aggregate_id).as_str());
            }
        };

        Ok(())
    }
}

#[derive(Clone)]
struct UsersCheckDisplayNameHandler;

#[async_trait]
impl RuleHandler for UsersCheckDisplayNameHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<()> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created => {
                let mut users = state.users.write().await;
                let data: Created = event.to_data().unwrap();
                let user = User {
                    display_name: data.display_name,
                    deleted: false,
                };
                users.insert(User::from_aggregate_id(&event.aggregate_id), user.clone());
            }
            UserEvent::DisplayNameUpdated => {
                let mut users = state.users.write().await;
                let data: DisplayNameUpdated = event.to_data().unwrap();
                let Some(user) =
                    users.get_mut(User::from_aggregate_id(&event.aggregate_id).as_str())
                else {
                    return Ok(());
                };

                if user.display_name == "Albert" {
                    return Err(anyhow!("not accepted has display name"));
                }

                user.display_name = data.display_name;
            }
            UserEvent::AccountDeleted => {
                let mut users = state.users.write().await;
                users.remove(User::from_aggregate_id(&event.aggregate_id).as_str());
            }
        };

        Ok(())
    }
}

#[derive(Clone)]
struct UsersCountHandler;

#[async_trait]
impl RuleHandler for UsersCountHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<()> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        if let UserEvent::Created = user_event {
            state.users_count.fetch_add(1, Ordering::SeqCst);
        };

        Ok(())
    }
}

#[derive(Default, Clone)]
struct ConsumerState {
    users: Users,
    users_count: Arc<AtomicU8>,
}

pub async fn test_multiple_consumer<E: Engine + Clone + 'static>(
    consumer: &Consumer<E>,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users").handler("user/**", UsersHandler(true));

    let eu_west_3a = consumer
        .name("eu-west-3a")
        .data(state.clone())
        .rule(users_rule.clone())
        .start(0)
        .await?;

    let eu_west_3b = consumer
        .name("eu-west-3b")
        .data(state.clone())
        .rule(users_rule.clone())
        .start(0)
        .await?;

    let us_east_1a = consumer
        .name("us-east-1a")
        .data(state.clone())
        .rule(users_rule.clone())
        .start(0)
        .await?;

    eu_west_3a
        .aggregate::<User>("1")
        .event(Created {
            display_name: "John Wick".to_owned(),
        })?
        .publish()
        .await?;

    sleep(Duration::from_millis(300)).await;

    let user1 = {
        let users = state.users.read().await;
        users.get("1").cloned().unwrap()
    };

    assert_eq!(user1.display_name, "John Wick".to_owned());

    eu_west_3b
        .publish::<User, _>(
            "2",
            WriteEvent::new(UserEvent::Created).data(Created {
                display_name: "Albert".to_owned(),
            })?,
            0,
        )
        .await?;

    eu_west_3b
        .publish::<User, _>(
            "1",
            WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                display_name: "John Wick from eu-west-3b".to_owned(),
            })?,
            1,
        )
        .await?;

    sleep(Duration::from_millis(300)).await;

    let (user1, user2) = {
        let users = state.users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned().unwrap(),
        )
    };

    assert_eq!(
        user1.display_name,
        "John Wick from eu-west-3b owned by eu-west-3a".to_owned()
    );
    assert_eq!(user2.display_name, "Albert".to_owned());

    us_east_1a
        .publish::<User, _>(
            "2",
            WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                display_name: "Albert from us-east-1a".to_owned(),
            })?,
            1,
        )
        .await?;

    us_east_1a
        .publish::<User, _>(
            "1",
            WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                display_name: "John Wick from us-east-1a".to_owned(),
            })?,
            2,
        )
        .await?;

    sleep(Duration::from_millis(300)).await;

    let (user1, user2) = {
        let users = state.users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned().unwrap(),
        )
    };

    assert_eq!(
        user1.display_name,
        "John Wick from us-east-1a owned by eu-west-3a".to_owned()
    );

    assert_eq!(
        user2.display_name,
        "Albert from us-east-1a owned by eu-west-3b".to_owned()
    );

    Ok(())
}

pub async fn test_filter<E: Engine + Clone + 'static>(
    consumer: &Consumer<E>,
    with_name: bool,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users").handler("user/[13]/*", UsersHandler(false));
    let users_last_name_count_rule =
        Rule::new("users-lastname-count").handler("user/**", UsersCountHandler);

    let consumer = if with_name {
        consumer.name("eu-west-3a")
    } else {
        consumer.clone()
    };

    let producer = consumer
        .data(state.clone())
        .rule(users_rule.clone())
        .rule(users_last_name_count_rule.clone())
        .start(0)
        .await?;

    init(&producer).await?;

    sleep(Duration::from_millis(300)).await;

    assert_eq!(state.users_count.load(Ordering::SeqCst), 3);

    let (john, albert, nina) = {
        let users = state.users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned(),
            users.get("3").cloned().unwrap(),
        )
    };

    assert_eq!(john.display_name, "John".to_owned());
    assert_eq!(albert, None);
    assert_eq!(nina.display_name, "Nina Doe".to_owned());

    Ok(())
}

#[derive(Deserialize)]
struct DeadletterMetadata {
    #[serde(rename(deserialize = "_evento_errors"))]
    pub errors: Vec<String>,
}

pub async fn test_deadletter<E: Engine + Clone + 'static>(
    consumer: &Consumer<E>,
    with_name: bool,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users").handler("user/**", UsersCheckDisplayNameHandler);

    let new_consumer = if with_name {
        consumer.name("eu-west-3a")
    } else {
        consumer.clone()
    };

    let producer = new_consumer
        .data(state.clone())
        .rule(users_rule.clone())
        .start(0)
        .await?;

    init(&producer).await?;

    sleep(Duration::from_millis(300)).await;

    let (john, albert, nina) = {
        let users = state.users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned().unwrap(),
            users.get("3").cloned().unwrap(),
        )
    };

    assert_eq!(john.display_name, "John".to_owned());
    assert_eq!(albert.display_name, "Albert".to_owned());
    assert_eq!(nina.display_name, "Nina Doe".to_owned());

    let deadletter_events = consumer.deadletter.read(10, None, None).await?;

    assert_eq!(deadletter_events.edges.len(), 1);
    assert_eq!(
        deadletter_events
            .edges
            .first()
            .unwrap()
            .node
            .to_metadata::<DeadletterMetadata>()?
            .unwrap()
            .errors,
        vec!["not accepted has display name".to_owned()]
    );

    Ok(())
}

pub async fn test_no_cdc<E: Engine + Clone + 'static>(
    consumer: &Consumer<E>,
    with_name: bool,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users")
        .cdc(false)
        .handler("user/**", UsersHandler(false));

    let consumer = if with_name {
        consumer.name("eu-west-3a")
    } else {
        consumer.clone()
    };

    let producer = consumer
        .data(state.clone())
        .rule(users_rule.clone())
        .start(1)
        .await?;

    init(&producer).await?;

    sleep(Duration::from_millis(1500)).await;

    producer
        .publish::<User, _>(
            "1",
            WriteEvent::new(UserEvent::Created).data(DisplayNameUpdated {
                display_name: "John Doe".to_owned(),
            })?,
            1,
        )
        .await?;

    producer
        .aggregate::<User>("4")
        .event(Created {
            display_name: "Luffy".to_owned(),
        })?
        .event(DisplayNameUpdated {
            display_name: "Monkey D. Luffy".to_owned(),
        })?
        .publish()
        .await?;

    sleep(Duration::from_millis(301)).await;

    let (john, albert, nina, luffy) = {
        let users = state.users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned(),
            users.get("3").cloned(),
            users.get("4").cloned().unwrap(),
        )
    };

    assert_eq!(john.display_name, "John Doe".to_owned());
    assert_eq!(albert, None);
    assert_eq!(nina, None);
    assert_eq!(luffy.display_name, "Monkey D. Luffy".to_owned());

    Ok(())
}

pub async fn test_external_store<E: Engine + Clone + 'static>(
    consumer: &Consumer<E>,
    store: &Store,
    with_name: bool,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users")
        .store(store.clone())
        .handler("user/**", UsersHandler(false));
    let users_last_name_count_rule =
        Rule::new("users-lastname-count").handler("user/**", UsersCountHandler);

    let consumer = if with_name {
        consumer.name("eu-west-3a")
    } else {
        consumer.clone()
    };

    let producer = consumer
        .data(state.clone())
        .rule(users_rule.clone())
        .rule(users_last_name_count_rule.clone())
        .start(0)
        .await?;

    let producer_ext = Producer {
        name: producer.name.to_owned(),
        store: store.clone(),
    };

    init(&producer_ext).await?;

    producer
        .publish_all::<User, _>(
            "1",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    display_name: "Nami".to_owned(),
                })?,
                WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                    display_name: "Vegapunk D. Nami".to_owned(),
                })?,
            ],
            0,
        )
        .await?;

    sleep(Duration::from_millis(300)).await;

    assert_eq!(state.users_count.load(Ordering::SeqCst), 1);

    let (john, albert, nina) = {
        let users = state.users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned().unwrap(),
            users.get("3").cloned().unwrap(),
        )
    };

    assert_eq!(john.display_name, "John".to_owned());
    assert_eq!(albert.display_name, "Albert Dupont".to_owned());
    assert_eq!(nina.display_name, "Nina Doe".to_owned());

    Ok(())
}

async fn init(producer: &Producer) -> Result<()> {
    producer
        .publish::<User, _>(
            "1",
            WriteEvent::new(UserEvent::Created).data(Created {
                display_name: "John".to_owned(),
            })?,
            0,
        )
        .await?;

    producer
        .publish_all::<User, _>(
            "2",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    display_name: "Albert".to_owned(),
                })?,
                WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                    display_name: "Albert Dupont".to_owned(),
                })?,
            ],
            0,
        )
        .await?;

    producer
        .publish::<User, _>(
            "3",
            WriteEvent::new(UserEvent::Created).data(Created {
                display_name: "Nina Doe".to_owned(),
            })?,
            0,
        )
        .await?;

    Ok(())
}
