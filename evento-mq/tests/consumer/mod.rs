use anyhow::{anyhow, Result};
use async_trait::async_trait;
use evento_mq::{Consumer, ConsumerContext, Engine, Producer, Rule, RuleHandler, RulePostHandler};
use evento_store::{Aggregate, Event, Store, WriteEvent};
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

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum UserEvent {
    Created,
    DisplayNameUpdated,
    AccountDeleted,
}

impl From<UserEvent> for String {
    fn from(val: UserEvent) -> Self {
        val.to_string()
    }
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

#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct User {
    pub display_name: String,
    pub deleted: bool,
}

impl Aggregate for User {
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

    fn aggregate_type<'a>() -> &'a str {
        "user"
    }
}

type Users = Arc<RwLock<HashMap<String, User>>>;

#[derive(Clone)]
struct UsersHandler(bool);

#[async_trait]
impl RuleHandler for UsersHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
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
                users.insert(User::to_id(&event.aggregate_id), user.clone());

                Ok(Some(event.data(user)?))
            }
            UserEvent::DisplayNameUpdated => {
                let mut users = state.users.write().await;
                let data: DisplayNameUpdated = event.to_data().unwrap();
                let Some(user) = users.get_mut(User::to_id(&event.aggregate_id).as_str()) else {
                    return Ok(None);
                };

                user.display_name = if self.0 {
                    format!(
                        "{} owned by {}",
                        data.display_name,
                        ctx.name().unwrap_or_default()
                    )
                } else {
                    data.display_name
                };

                Ok(Some(event.data(user)?))
            }
            UserEvent::AccountDeleted => {
                let mut users = state.users.write().await;
                users.remove(User::to_id(&event.aggregate_id).as_str());

                Ok(None)
            }
        }
    }
}

#[async_trait]
impl RulePostHandler for UsersHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<()> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created | UserEvent::DisplayNameUpdated => {
                let mut users: tokio::sync::RwLockWriteGuard<'_, HashMap<String, User>> =
                    state.post_users.write().await;
                let data: User = event.to_data().unwrap();
                users.insert(User::to_id(&event.aggregate_id), data);
            }
            _ => {}
        };

        Ok(())
    }
}

#[derive(Clone)]
struct UsersCheckDisplayNameHandler;

#[async_trait]
impl RuleHandler for UsersCheckDisplayNameHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
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
                users.insert(User::to_id(&event.aggregate_id), user.clone());

                Ok(Some(event.data(user)?))
            }
            UserEvent::DisplayNameUpdated => {
                let mut users = state.users.write().await;
                let data: DisplayNameUpdated = event.to_data().unwrap();
                let Some(user) = users.get_mut(User::to_id(&event.aggregate_id).as_str()) else {
                    return Ok(None);
                };

                if user.display_name == "Albert" {
                    return Err(anyhow!("not accepted has display name"));
                }

                user.display_name = data.display_name;

                Ok(Some(event.data(user)?))
            }
            UserEvent::AccountDeleted => {
                let mut users = state.users.write().await;
                users.remove(User::to_id(&event.aggregate_id).as_str());

                Ok(None)
            }
        }
    }
}

#[derive(Clone)]
struct UsersCountHandler;

#[async_trait]
impl RuleHandler for UsersCountHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        if let UserEvent::Created = user_event {
            state.users_count.fetch_add(1, Ordering::SeqCst);
        };

        Ok(None)
    }
}

#[derive(Default, Clone)]
struct ConsumerState {
    users: Users,
    post_users: Users,
    users_count: Arc<AtomicU8>,
}

pub async fn test_multiple_consumer<
    E: Engine + Clone + 'static,
    S: evento_store::Engine + Clone + 'static,
>(
    consumer: &Consumer<E, S>,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users")
        .handler("user/**", UsersHandler(true))
        .post_handler(UsersHandler(true));

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
        .publish::<User, _>(
            "1",
            WriteEvent::new(UserEvent::Created).data(Created {
                display_name: "John Wick".to_owned(),
            })?,
            0,
        )
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

pub async fn test_post_handler<
    E: Engine + Clone + 'static,
    S: evento_store::Engine + Clone + 'static,
>(
    consumer: &Consumer<E, S>,
    with_name: bool,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users")
        .handler("user/1/*", UsersHandler(false))
        .handler("user/2/*", UsersHandler(false))
        .handler("user/[!1-2]/*", UsersHandler(false))
        .post_handler(UsersHandler(false))
        .post_handler(UsersHandler(false));

    let consumer = if with_name {
        consumer.name("eu-west-3a")
    } else {
        consumer.clone()
    };

    let producer = consumer
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
    assert_eq!(albert.display_name, "Albert Dupont".to_owned());
    assert_eq!(nina.display_name, "Nina Doe".to_owned());

    let (john, albert, nina) = {
        let users = state.post_users.read().await;
        (
            users.get("1").cloned().unwrap(),
            users.get("2").cloned().unwrap(),
            users.get("3").cloned(),
        )
    };

    assert_eq!(john.display_name, "John".to_owned());
    assert_eq!(albert.display_name, "Albert Dupont".to_owned());
    assert_eq!(nina, None);

    Ok(())
}

pub async fn test_filter<E: Engine + Clone + 'static, S: evento_store::Engine + Clone + 'static>(
    consumer: &Consumer<E, S>,
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

pub async fn test_deadletter<
    E: Engine + Clone + 'static,
    S: evento_store::Engine + Clone + 'static,
>(
    consumer: &Consumer<E, S>,
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

pub async fn test_no_cdc<E: Engine + Clone + 'static, S: evento_store::Engine + Clone + 'static>(
    consumer: &Consumer<E, S>,
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
        .publish_all::<User, _>(
            "4",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    display_name: "Luffy".to_owned(),
                })?,
                WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                    display_name: "Monkey D. Luffy".to_owned(),
                })?,
            ],
            0,
        )
        .await?;

    sleep(Duration::from_millis(300)).await;

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

pub async fn test_external_store<
    E: Engine + Clone + 'static,
    S: evento_store::Engine + Clone + 'static,
>(
    consumer: &Consumer<E, S>,
    store: &Store<S>,
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

async fn init<S: evento_store::Engine + Clone + 'static>(producer: &Producer<S>) -> Result<()> {
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
