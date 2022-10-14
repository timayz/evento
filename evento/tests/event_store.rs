use std::collections::HashMap;

use evento::{Aggregate, Engine, Error, Event, EventStore, MemoryStore};
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
    fn apply(&mut self, event: &evento::Event) {
        let user_event: UserEvent = event.name.parse().unwrap();

        match (user_event, event.data.clone()) {
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

    fn aggregate_id<I: Into<String>>(id: I) -> String {
        format!("user_{}", id.into())
    }
}

#[test]
fn apply_events() {
    let mut user = User::default();

    user.apply(
        &Event::new(UserEvent::Created)
            .aggregate_id(User::aggregate_id("1"))
            .version(1)
            .data(Created {
                username: "john.doe".to_owned(),
                password: "azerty".to_owned(),
            })
            .unwrap(),
    );

    assert_eq!(user.deleted, false);

    user.apply(
        &Event::new(UserEvent::AccountDeleted)
            .aggregate_id(User::aggregate_id("1"))
            .version(2),
    );

    assert_eq!(user.deleted, true)
}

#[tokio::test]
async fn memory_save() {
    let store = MemoryStore::new();
    save(store).await
}

#[tokio::test]
async fn memory_load_save() {
    let store = create_memory_store().await;
    load_save(store).await;
}

#[tokio::test]
async fn memory_save_wrong_version() {
    let store = create_memory_store().await;
    save_wrong_version(store).await;
}

async fn save<E: Engine>(store: EventStore<E>) {
    let last_version = store
        .save::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "john.doe".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::AccountDeleted),
            ],
            0,
        )
        .await
        .unwrap();

    assert_eq!(last_version, 2)
}

async fn load_save<E: Engine>(store: EventStore<E>) {
    let (john, last_event) = store.load::<User, _>("1").await.unwrap().unwrap();

    assert_eq!(john.username, "john.doe");
    assert_eq!(john.password, "azerty");
    assert_eq!(john.first_name, None);
    assert_eq!(john.last_name, None);

    let last_version = store
        .save::<User, _>(
            "1",
            vec![Event::new(UserEvent::ProfileUpdated)
                .data(ProfileUpdated {
                    first_name: "John".to_owned(),
                    last_name: "Doe".to_owned(),
                })
                .unwrap()],
            last_event.version,
        )
        .await
        .unwrap();

    assert_eq!(last_version, 3);

    let last_version = store
        .save::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::PasswordUpdated)
                    .data(PasswordUpdated {
                        old_password: "azerty".to_owned(),
                        new_password: "securepwd".to_owned(),
                    })
                    .unwrap()
                    .metadata(HashMap::from([("algo", "RS256")]))
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "john007".to_owned(),
                    })
                    .unwrap(),
            ],
            last_version,
        )
        .await
        .unwrap();

    assert_eq!(last_version, 5);

    let (john, last_event) = store.load::<User, _>("1").await.unwrap().unwrap();

    assert_eq!(john.username, "john.doe");
    assert_eq!(john.first_name, Some("John".to_owned()));
    assert_eq!(john.last_name, Some("Doe".to_owned()));
    assert_eq!(john.password, "securepwd");
    assert_eq!(john.display_name, Some("john007".to_owned()));

    assert_eq!(last_event.version, 5);
}

async fn save_wrong_version<E: Engine>(store: EventStore<E>) {
    let (_, last_event) = store.load::<User, _>("1").await.unwrap().unwrap();

    store
        .save::<User, _>(
            "1",
            vec![Event::new(UserEvent::ProfileUpdated)
                .data(ProfileUpdated {
                    first_name: "John".to_owned(),
                    last_name: "Doe".to_owned(),
                })
                .unwrap()],
            last_event.version,
        )
        .await
        .unwrap();

    let err = store
        .save::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::ProfileUpdated)
                    .data(ProfileUpdated {
                        first_name: "Albert".to_owned(),
                        last_name: "Dupont".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "callmedady".to_owned(),
                    })
                    .unwrap(),
            ],
            last_event.version,
        )
        .await
        .unwrap_err();

    assert_eq!(err, Error::UnexpectedOriginalVersion)
}

async fn create_memory_store() -> EventStore<MemoryStore> {
    let store = MemoryStore::new();
    init_store(&store).await;

    store
}

async fn init_store<'a, E: Engine>(store: &'a E) {
    store
        .save::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "john.doe".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::AccountDeleted),
            ],
            0,
        )
        .await
        .unwrap();

    store
        .save::<User, _>(
            "2",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "albert.dupont".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::ProfileUpdated)
                    .data(ProfileUpdated {
                        first_name: "albert".to_owned(),
                        last_name: "dupont".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();
}
