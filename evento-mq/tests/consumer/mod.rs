use evento_mq::{Consumer, Engine};
use evento_store::{Aggregate, Event, Store, StoreError, WriteEvent};
use futures_util::future::join_all;
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
pub enum UserEvent {
    Created,
    DisplayNameUpdated,
    ProfileUpdated,
    PasswordUpdated,
    AccountDeleted,
}

impl From<UserEvent> for String {
    fn from(val: UserEvent) -> Self {
        val.to_string()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Created {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize)]
pub struct DisplayNameUpdated {
    pub display_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct ProfileUpdated {
    pub first_name: String,
    pub last_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct PasswordUpdated {
    pub old_password: String,
    pub new_password: String,
}

#[derive(Serialize, Deserialize)]
pub struct AccountDeleted {
    pub deleted: bool,
}

#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct User {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub display_name: Option<String>,
    pub username: String,
    pub password: String,
    pub deleted: bool,
    pub first_names: Vec<String>,
}

impl Aggregate for User {
    fn apply(&mut self, event: &Event) {
        let user_event: UserEvent = event.name.parse().unwrap();

        match user_event {
            UserEvent::Created => {
                let data: Created = event.to_data().unwrap();
                self.username = data.username;
                self.password = data.password;
            }
            UserEvent::DisplayNameUpdated => {
                let data: DisplayNameUpdated = event.to_data().unwrap();
                self.display_name = Some(data.display_name);
            }
            UserEvent::ProfileUpdated => {
                let data: ProfileUpdated = event.to_data().unwrap();
                self.first_names.push(data.first_name.to_owned());
                self.first_name = Some(data.first_name);
                self.last_name = Some(data.last_name);
            }
            UserEvent::PasswordUpdated => {
                let data: PasswordUpdated = event.to_data().unwrap();
                self.password = data.new_password;
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

pub async fn test_cdc<E: Engine + Clone, S: evento_store::Engine>(
    consumer: &Consumer<E, S>,
) -> anyhow::Result<()> {
    todo!()
}

pub async fn test_no_cdc<E: Engine + Clone, S: evento_store::Engine>(
    consumer: &Consumer<E, S>,
) -> anyhow::Result<()> {
    todo!()
}

pub async fn test_external_store<E: Engine + Clone, S: evento_store::Engine>(
    consumer: &Consumer<E, S>,
    ext_store: &Store<S>,
) -> anyhow::Result<()> {
    todo!()
}
