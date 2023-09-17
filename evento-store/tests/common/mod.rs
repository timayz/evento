use evento_store::{Aggregate, Engine, Event, EventStore, PgEngine};
use futures_util::{Future, TryFutureExt};
use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    Any, PgPool,
};
use std::{io, path::Path, time::Duration};

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

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub display_name: Option<String>,
    pub username: String,
    pub password: String,
    pub deleted: bool,
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

pub async fn create_pg_store(
    table_prefix: Option<&str>,
    init: bool,
) -> (EventStore<PgEngine>, PgPool) {
    let dsn = "postgres://postgres:postgres@localhost:5432/evento_test";
    let exists = retry_connect_errors(dsn, Any::database_exists)
        .await
        .unwrap();

    if !exists {
        let _ = Any::create_database(dsn).await;
    }

    let pool = PgPool::connect(dsn).await.unwrap();

    Migrator::new(Path::new("../migrations"))
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();

    let table_events = get_table_name(table_prefix, "events");

    sqlx::query::<_>(format!("TRUNCATE {table_events};").as_str())
        .execute(&pool)
        .await
        .unwrap();

    let store = table_prefix.map_or(PgEngine::new(pool.clone()), |table_prefix| {
        PgEngine::new_prefix(pool.clone(), table_prefix)
    });

    if init {
        init_store(&store).await;
    }

    (store, pool)
}

pub async fn init_store<E: Engine>(store: &E) {
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
                Event::new(UserEvent::AccountDeleted)
                    .data(AccountDeleted { deleted: true })
                    .unwrap(),
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

fn get_table_name(table_prefix: Option<&str>, name: impl Into<String>) -> String {
    match table_prefix.as_ref() {
        Some(prefix) => format!("evento_{prefix}_{}", name.into()),
        _ => format!("evento_{}", name.into()),
    }
}

/// Attempt to connect to the database server, retrying up to `ops.connect_timeout`.
// async fn connect(database_url: &String) -> sqlx::Result<AnyConnection> {
//     retry_connect_errors(database_url, AnyConnection::connect).await
// }

/// Attempt an operation that may return errors like `ConnectionRefused`,
/// retrying up until `ops.connect_timeout`.
///
/// The closure is passed `&ops.database_url` for easy composition.
async fn retry_connect_errors<'a, F, Fut, T>(
    database_url: &'a str,
    mut connect: F,
) -> sqlx::Result<T>
where
    F: FnMut(&'a str) -> Fut,
    Fut: Future<Output = sqlx::Result<T>> + 'a,
{
    sqlx::any::install_default_drivers();

    backoff::future::retry(
        backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(10)))
            .build(),
        || {
            connect(database_url).map_err(|e| -> backoff::Error<sqlx::Error> {
                if let sqlx::Error::Io(ref ioe) = e {
                    match ioe.kind() {
                        io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted => {
                            return backoff::Error::transient(e);
                        }
                        _ => (),
                    }
                }

                backoff::Error::permanent(e)
            })
        },
    )
    .await
}
