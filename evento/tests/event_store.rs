use evento::{Aggregate, Engine, Event, MemoryStore};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
enum UserEvent {
    Created,
    DisplayNameUpdated,
    ProfileUpdated,
    PasswordUpdated,
    AccountDeleted,
}

impl Into<String> for UserEvent {
    fn into(self) -> String {
        self.to_string()
    }
}

#[derive(Serialize, Deserialize)]
struct Created {
    username: String,
    password: String,
}

#[derive(Serialize, Deserialize)]
struct DisplayNameUpdated {
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct ProfileUpdated {
    first_name: String,
    last_name: String,
}

#[derive(Serialize, Deserialize)]
struct PasswordUpdated {
    old_password: String,
    new_password: String,
}

#[derive(Serialize, Deserialize)]
struct AccountDeleted;

#[derive(Default, Serialize, Deserialize)]
struct User {
    first_name: Option<String>,
    last_name: Option<String>,
    display_name: Option<String>,
    username: String,
    password: String,
    deleted: bool,
}

impl Aggregate for User {
    fn apply(&mut self, event: evento::Event) {
        let user_event: UserEvent = event.name.parse().unwrap();

        match (user_event, event.data) {
            (UserEvent::Created, Some(v)) => {
                let data: Created = serde_json::from_value(v).unwrap();
                self.username = data.username;
                self.password = data.password;
            }
            (UserEvent::DisplayNameUpdated, Some(v)) => {
                let data: DisplayNameUpdated = serde_json::from_value(v).unwrap();
                self.display_name = Some(data.display_name);
            }
            (UserEvent::ProfileUpdated, Some(v)) => {
                let data: ProfileUpdated = serde_json::from_value(v).unwrap();
                self.first_name = Some(data.first_name);
                self.last_name = Some(data.last_name);
            }
            (UserEvent::PasswordUpdated, Some(v)) => {
                let data: PasswordUpdated = serde_json::from_value(v).unwrap();
                self.password = data.new_password;
            }
            (UserEvent::AccountDeleted, None) => {
                self.deleted = true;
            }
            (_, _) => todo!(),
        }
    }
}

#[test]
fn apply_events() {
    let mut user = User::default();

    user.apply(
        Event::new(UserEvent::Created)
            .aggregate_id("1")
            .aggregate_type("user")
            .aggregate_version(1)
            .data(Created {
                username: "john.doe".to_owned(),
                password: "azerty".to_owned(),
            })
            .unwrap(),
    );

    assert_eq!(user.deleted, false);

    user.apply(
        Event::new(UserEvent::AccountDeleted)
            .aggregate_id("1")
            .aggregate_type("user")
            .aggregate_version(2),
    );

    assert_eq!(user.deleted, true)
}

#[tokio::test]
async fn memory_save() {
    let store = MemoryStore::new();
    let last_version = store
        .save(
            vec![
                Event::new(UserEvent::Created)
                    .aggregate_id("1")
                    .aggregate_type("user")
                    .aggregate_version(1)
                    .data(Created {
                        username: "john.doe".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::AccountDeleted)
                    .aggregate_id("1")
                    .aggregate_type("user")
                    .aggregate_version(2),
            ],
            0,
        )
        .await
        .unwrap();

    assert_eq!(last_version, 2)
}

#[test]
fn memory_load() {
    assert_eq!(true, false)
}

#[test]
fn memory_save_wrong_version() {
    assert_eq!(true, false)
}
