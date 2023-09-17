use evento::{Aggregate, Event, PgEngine, PgEvento};
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

pub async fn create_pg_store(table_prefix: &str, reset: bool) -> PgEvento {
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

    if !reset {
        return PgEngine::new_prefix(pool, table_prefix);
    }

    for table in vec!["events", "subscriptions", "deadletters"] {
        sqlx::query::<_>(format!("TRUNCATE evento_{table_prefix}_{table};").as_str())
            .execute(&pool)
            .await
            .unwrap();
    }

    PgEngine::new_prefix(pool, table_prefix)
}

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
