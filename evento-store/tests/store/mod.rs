use evento_store::{Aggregate, Engine, Event, Store, StoreError, WriteEvent};
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

pub async fn init<E: Engine>(store: &Store<E>) -> anyhow::Result<()> {
    store
        .write_all::<User>(
            "1",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    username: "john.doe".to_owned(),
                    password: "azerty".to_owned(),
                })?,
                WriteEvent::new(UserEvent::AccountDeleted)
                    .data(AccountDeleted { deleted: true })?,
            ],
            0,
        )
        .await?;

    store
        .write_all::<User>(
            "2",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    username: "albert.dupont".to_owned(),
                    password: "azerty".to_owned(),
                })?,
                WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                    first_name: "albert".to_owned(),
                    last_name: "dupont".to_owned(),
                })?,
            ],
            0,
        )
        .await?;

    Ok(())
}

pub async fn test_concurrency<E: Engine>(store: &Store<E>) -> anyhow::Result<()> {
    join_all(vec![
        store.write_all::<User>(
            "concurrency_save",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    username: "albert.dupont".to_owned(),
                    password: "azerty".to_owned(),
                })?,
                WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                    first_name: "albert".to_owned(),
                    last_name: "dupont".to_owned(),
                })?,
            ],
            0,
        ),
        store.write_all::<User>(
            "concurrency_save",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    username: "john.doe".to_owned(),
                    password: "azertypoiu".to_owned(),
                })?,
                WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                    first_name: "john".to_owned(),
                    last_name: "doe".to_owned(),
                })?,
            ],
            0,
        ),
        store.write_all::<User>(
            "concurrency_save",
            vec![
                WriteEvent::new(UserEvent::Created).data(Created {
                    username: "nina.dupont".to_owned(),
                    password: "azertyuiop".to_owned(),
                })?,
                WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                    first_name: "nina".to_owned(),
                    last_name: "dupont".to_owned(),
                })?,
            ],
            0,
        ),
    ])
    .await;

    let events = store.read_of::<User>("concurrency_save", 10, None).await?;

    assert_eq!(events.edges.len(), 2);
    assert_eq!(
        events.edges[0].node.data,
        serde_json::to_value(Created {
            username: "albert.dupont".to_owned(),
            password: "azerty".to_owned(),
        })?
    );
    assert_eq!(
        events.edges[1].node.data,
        serde_json::to_value(ProfileUpdated {
            first_name: "albert".to_owned(),
            last_name: "dupont".to_owned(),
        })?
    );

    let (user, _) = store
        .load_with::<User>("concurrency_save", 1)
        .await?
        .unwrap();

    assert_eq!(
        user,
        User {
            username: "albert.dupont".to_owned(),
            password: "azerty".to_owned(),
            first_name: Some("albert".to_owned()),
            last_name: Some("dupont".to_owned()),
            first_names: vec!["albert".to_owned()],
            ..Default::default()
        }
    );

    Ok(())
}

pub async fn test_save<E: Engine>(store: &Store<E>) -> anyhow::Result<()> {
    let (john, version) = store.load::<User>("1").await?.unwrap();

    assert_eq!(
        john,
        User {
            username: "john.doe".to_owned(),
            password: "azerty".to_owned(),
            deleted: true,
            ..Default::default()
        }
    );

    let last_event = store
        .write::<User>(
            "1",
            WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                first_name: "John".to_owned(),
                last_name: "Doe".to_owned(),
            })?,
            version,
        )
        .await?;

    assert_eq!(last_event.version, 3);

    let last_event = store
        .write_all::<User>(
            "1",
            vec![
                WriteEvent::new(UserEvent::PasswordUpdated)
                    .data(PasswordUpdated {
                        old_password: "azerty".to_owned(),
                        new_password: "securepwd".to_owned(),
                    })?
                    .metadata(HashMap::from([("algo".to_owned(), "RS256".to_owned())]))?,
                WriteEvent::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "john007".to_owned(),
                    })
                    .unwrap(),
            ],
            u16::try_from(last_event.version)?,
        )
        .await
        .map(|events| events.last().cloned())
        .unwrap()
        .unwrap();

    assert_eq!(last_event.version, 5);

    let (john, version) = store.load::<User>("1").await?.unwrap();

    assert_eq!(
        john,
        User {
            username: "john.doe".to_owned(),
            first_name: Some("John".to_owned()),
            last_name: Some("Doe".to_owned()),
            password: "securepwd".to_owned(),
            display_name: Some("john007".to_owned()),
            first_names: vec!["John".to_owned()],
            deleted: true,
        }
    );

    assert_eq!(version, 5);

    Ok(())
}

pub async fn test_wrong_version<E: Engine>(store: &Store<E>) -> anyhow::Result<()> {
    let ov = store
        .write::<User>(
            "wrong_version",
            WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                first_name: "John".to_owned(),
                last_name: "Doe".to_owned(),
            })?,
            0,
        )
        .await?;

    let err = store
        .write_all::<User>(
            "wrong_version",
            vec![
                WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                    first_name: "Albert".to_owned(),
                    last_name: "Dupont".to_owned(),
                })?,
                WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                    display_name: "callmedady".to_owned(),
                })?,
            ],
            0,
        )
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        StoreError::UnexpectedOriginalVersion.to_string()
    );

    let ovf = store
        .write_all::<User>(
            "wrong_version",
            vec![
                WriteEvent::new(UserEvent::ProfileUpdated).data(ProfileUpdated {
                    first_name: "Albert".to_owned(),
                    last_name: "Dupont".to_owned(),
                })?,
                WriteEvent::new(UserEvent::DisplayNameUpdated).data(DisplayNameUpdated {
                    display_name: "callmedady".to_owned(),
                })?,
            ],
            u16::try_from(ov.version)?,
        )
        .await?
        .last()
        .cloned()
        .unwrap();

    assert_eq!(ov.version + 2, ovf.version);

    Ok(())
}

pub async fn test_insert<E: Engine>(store: &Store<E>) -> anyhow::Result<()> {
    let query = store.read(10, None, None).await.unwrap();

    assert_eq!(query.edges.len(), 4);
    assert_eq!(
        query.edges[0].node.data,
        serde_json::to_value(Created {
            username: "john.doe".to_owned(),
            password: "azerty".to_owned(),
        })?,
    );
    assert_eq!(
        query.edges[1].node.data,
        serde_json::to_value(AccountDeleted { deleted: true })?,
    );

    assert_eq!(
        query.edges[2].node.data,
        serde_json::to_value(Created {
            username: "albert.dupont".to_owned(),
            password: "azerty".to_owned(),
        })?,
    );
    assert_eq!(
        query.edges[3].node.data,
        serde_json::to_value(ProfileUpdated {
            first_name: "albert".to_owned(),
            last_name: "dupont".to_owned(),
        })?,
    );

    store
        .insert(vec![
            WriteEvent::new(UserEvent::Created)
                .data(Created {
                    username: "john.doe".to_owned(),
                    password: "azerty".to_owned(),
                })?
                .to_event("user#3", 0),
            WriteEvent::new(UserEvent::AccountDeleted)
                .data(AccountDeleted { deleted: true })?
                .to_event("user#3", 1),
        ])
        .await
        .unwrap();

    let query = store.read(10, None, None).await.unwrap();

    assert_eq!(query.edges.len(), 6);
    assert_eq!(
        query.edges[0].node.data,
        serde_json::to_value(Created {
            username: "john.doe".to_owned(),
            password: "azerty".to_owned(),
        })?,
    );

    assert_eq!(
        query.edges[1].node.data,
        serde_json::to_value(AccountDeleted { deleted: true })?,
    );

    assert_eq!(
        query.edges[2].node.data,
        serde_json::to_value(Created {
            username: "albert.dupont".to_owned(),
            password: "azerty".to_owned(),
        })?,
    );

    assert_eq!(
        query.edges[3].node.data,
        serde_json::to_value(ProfileUpdated {
            first_name: "albert".to_owned(),
            last_name: "dupont".to_owned(),
        })?,
    );

    assert_eq!(
        query.edges[4].node.data,
        serde_json::to_value(Created {
            username: "john.doe".to_owned(),
            password: "azerty".to_owned(),
        })?,
    );

    assert_eq!(
        query.edges[5].node.data,
        serde_json::to_value(AccountDeleted { deleted: true })?,
    );

    Ok(())
}
