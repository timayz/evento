use anyhow::Result;
use async_trait::async_trait;
use chrono::naive;
use evento_mq::{Consumer, ConsumerContext, Engine, Rule, RuleHandler, RulePostHandler};
use evento_store::{Aggregate, Event, Store, WriteEvent};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
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
type UsersCount = Arc<RwLock<HashMap<String, u16>>>;

#[derive(Clone)]
struct UsersHandler;

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

                user.display_name = format!(
                    "{} owned by {}",
                    data.display_name,
                    ctx.name().unwrap_or_default()
                );

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
struct JohnAlbertUsersHandler;

#[async_trait]
impl RuleHandler for JohnAlbertUsersHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created => {
                let mut users = state.john_albert_users.write().await;
                let data: Created = event.to_data().unwrap();
                let user = User {
                    display_name: data.display_name,
                    deleted: false,
                };
                users.insert(User::to_id(&event.aggregate_id), user.clone());
            }
            UserEvent::DisplayNameUpdated => {
                let mut users = state.john_albert_users.write().await;
                let data: DisplayNameUpdated = event.to_data().unwrap();
                let Some(user) = users.get_mut(User::to_id(&event.aggregate_id).as_str()) else {
                    return Ok(None);
                };

                user.display_name = data.display_name;
            }
            UserEvent::AccountDeleted => {
                let mut users = state.john_albert_users.write().await;
                users.remove(User::to_id(&event.aggregate_id).as_str());
            }
        };

        Ok(None)
    }
}

#[derive(Clone)]
struct UsersLastNameCountHandler;

#[async_trait]
impl RuleHandler for UsersLastNameCountHandler {
    async fn handle(&self, event: Event, ctx: ConsumerContext) -> Result<Option<Event>> {
        let state = ctx.extract::<ConsumerState>();
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created => {
                let mut users = state.users_count.write().await;
                let data: Created = event.to_data().unwrap();
                let user = users.entry(data.display_name).or_insert(0);

                *user += 1;
            }
            _ => {}
        };

        Ok(None)
    }
}

#[derive(Default, Clone)]
struct ConsumerState {
    users: Users,
    post_users: Users,
    john_albert_users: Users,
    users_count: UsersCount,
}

pub async fn test_cdc<E: Engine + Clone + 'static, S: evento_store::Engine + Clone + 'static>(
    consumer: &Consumer<E, S>,
) -> Result<()> {
    let state = ConsumerState::default();
    let users_rule = Rule::new("users")
        .handler("user/**", UsersHandler)
        .post_handler(UsersHandler);
    let john_albert_rule = Rule::new("john-albert").handler("user/{1,2}/*", JohnAlbertUsersHandler);
    let users_last_name_count_rule =
        Rule::new("users-lastname-count").handler("user/**", UsersLastNameCountHandler);

    let eu_west_3a = consumer
        .name("eu-west-3a")
        .data(state.clone())
        .rule(users_rule.clone())
        .rule(john_albert_rule.clone())
        .rule(users_last_name_count_rule.clone())
        .start(0)
        .await?;

    let eu_west_3b = consumer
        .name("eu-west-3b")
        .data(state.clone())
        .rule(users_rule.clone())
        .rule(john_albert_rule.clone())
        .rule(users_last_name_count_rule.clone())
        .start(0)
        .await?;

    let us_east_1a = consumer
        .name("us-east-1a")
        .data(state.clone())
        .rule(users_rule.clone())
        .rule(john_albert_rule.clone())
        .rule(users_last_name_count_rule.clone())
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

pub async fn test_no_cdc<E: Engine + Clone, S: evento_store::Engine>(
    consumer: &Consumer<E, S>,
) -> Result<()> {
    Ok(())
}

pub async fn test_external_store<E: Engine + Clone, S: evento_store::Engine>(
    consumer: &Consumer<E, S>,
    ext_store: &Store<S>,
) -> Result<()> {
    Ok(())
}
