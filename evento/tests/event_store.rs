use chrono::Utc;
use evento::{Aggregate, Event};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Display, FromStr)]
#[display(style = "kebab-case")]
enum UserEvent {
    Created,
    DisplayNameUpdated,
    ProfileUpdated,
    PasswordUpdated,
    AccountDeleted,
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

    user.apply(Event {
        id: Uuid::new_v4(),
        name: UserEvent::Created.to_string(),
        aggregate_id: "1".to_owned(),
        aggregate_type: "user".to_owned(),
        aggregate_version: 1,
        data: Some(
            serde_json::to_value(Created {
                username: "john.doe".to_owned(),
                password: "azerty".to_owned(),
            })
            .unwrap(),
        ),
        metadata: None,
        created_at: Utc::now(),
    });

    assert_eq!(user.deleted, false);

    user.apply(Event {
        id: Uuid::new_v4(),
        name: UserEvent::AccountDeleted.to_string(),
        aggregate_id: "1".to_owned(),
        aggregate_type: "user".to_owned(),
        aggregate_version: 2,
        data: None,
        metadata: None,
        created_at: Utc::now(),
    });

    assert_eq!(user.deleted, true)
}

#[test]
fn save() {
    assert_eq!(true, false)
}

#[test]
fn load() {
    assert_eq!(true, false)
}
