use evento::store::{Engine as StoreEngine, MemoryEngine as StoreMemoryEngine};
use evento::{
    Aggregate, Engine, Event, Evento, MemoryEngine, PgEngine, SubscirberHandlerError, Subscriber,
};
use futures_util::FutureExt;
use serde_json::json;
use sqlx::{Executor, PgPool};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::common::{Created, DisplayNameUpdated, User, UserEvent};

mod common;

#[tokio::test]
async fn memory_publish() {
    let store = StoreMemoryEngine::new();
    let eu_west_3a = MemoryEngine::new(store.clone());
    let eu_west_3b = MemoryEngine::new(store.clone());
    let us_east_1a = MemoryEngine::new(store);

    publish(eu_west_3a, eu_west_3b, us_east_1a).await
}

#[tokio::test]
async fn memory_filter() {
    let store = StoreMemoryEngine::new();
    let eu_west_3a = MemoryEngine::new(store);
    filter(eu_west_3a).await
}

#[tokio::test]
async fn memory_deadletter() {
    let store = StoreMemoryEngine::new();
    let eu_west_3a = MemoryEngine::new(store);
    deadletter(eu_west_3a).await
}

#[tokio::test]
async fn pg_publish() {
    let eu_west_3a = create_pg_store("lib_publish", true).await;
    let eu_west_3b = create_pg_store("lib_publish", false).await;
    let us_east_1a = create_pg_store("lib_publish", false).await;

    publish(eu_west_3a, eu_west_3b, us_east_1a).await
}

#[tokio::test]
async fn pg_filter() {
    let eu_west_3a = create_pg_store("lib_filter", true).await;
    filter(eu_west_3a).await
}

#[tokio::test]
async fn pg_deadletter() {
    let eu_west_3a = create_pg_store("lib_deadletter", true).await;
    deadletter(eu_west_3a).await
}

async fn publish<E: Engine + Sync + Send + 'static, S: StoreEngine + Sync + Send + 'static>(
    eu_west_3a: Evento<E, S>,
    eu_west_3b: Evento<E, S>,
    us_east_1a: Evento<E, S>,
) {
    let subscriber = Subscriber::new("users")
        .filter("user/#")
        .handler(|event, ctx| {
            let bus_name = ctx.name();
            let users = ctx
                .0
                .read()
                .extract::<Arc<RwLock<HashMap<String, User>>>>()
                .clone();

            async move {
                let user_event: UserEvent = event.name.parse().unwrap();

                match user_event {
                    UserEvent::Created => {
                        let data: Created = event.to_data().unwrap();
                        let mut w_users = users.write().await;
                        w_users.insert(
                            User::to_id(event.aggregate_id),
                            User {
                                username: bus_name
                                    .map(|name| format!("{} ({name})", data.username))
                                    .unwrap_or(data.username),
                                password: data.password,
                                ..Default::default()
                            },
                        );
                    }
                    UserEvent::DisplayNameUpdated => {
                        let data: DisplayNameUpdated = event.to_data().unwrap();
                        let mut w_users = users.write().await;

                        if let Some(user) = w_users.get_mut(&User::to_id(event.aggregate_id)) {
                            user.display_name = Some(
                                bus_name
                                    .map(|name| format!("{} ({name})", data.display_name))
                                    .unwrap_or(data.display_name),
                            );
                        }
                    }
                    _ => {}
                };

                Ok(())
            }
            .boxed()
        });

    let users: Arc<RwLock<HashMap<String, User>>> = Arc::new(RwLock::new(HashMap::new()));
    let eu_west_3a = eu_west_3a
        .name("eu-west-3a")
        .data(users.clone())
        .subscribe(subscriber.clone())
        .run_with_delay(Duration::from_secs(0))
        .await
        .unwrap();
    let eu_west_3b = eu_west_3b
        .name("eu-west-3b")
        .data(users.clone())
        .subscribe(subscriber.clone())
        .run_with_delay(Duration::from_secs(0))
        .await
        .unwrap();
    let us_east_1a = us_east_1a
        .name("us-east-1a")
        .data(users.clone())
        .subscribe(subscriber.clone())
        .run_with_delay(Duration::from_secs(0))
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "john.doe".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "John doe".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();

    eu_west_3b
        .publish::<User, _>(
            "1",
            vec![Event::new(UserEvent::DisplayNameUpdated)
                .data(DisplayNameUpdated {
                    display_name: "John Wick".to_owned(),
                })
                .unwrap()],
            2,
        )
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let user1 = {
        let r_users = users.read().await;
        r_users.get("1").cloned().unwrap()
    };

    assert_eq!(user1.username, "john.doe (eu-west-3a)");
    assert_eq!(
        user1.display_name,
        Some("John Wick (eu-west-3a)".to_owned())
    );

    us_east_1a
        .publish::<User, _>(
            "1",
            vec![Event::new(UserEvent::DisplayNameUpdated)
                .data(DisplayNameUpdated {
                    display_name: "Nina Wick".to_owned(),
                })
                .unwrap()],
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let user1 = {
        let r_users = users.read().await;
        r_users.get("1").cloned().unwrap()
    };

    assert_eq!(
        user1.display_name,
        Some("Nina Wick (eu-west-3a)".to_owned())
    );
}

async fn filter<E: Engine + Sync + Send + 'static, S: StoreEngine + Sync + Send + 'static>(
    eu_west_3a: Evento<E, S>,
) {
    let users: Arc<RwLock<HashMap<String, User>>> = Arc::new(RwLock::new(HashMap::new()));
    let users_count: Arc<RwLock<i32>> = Arc::new(RwLock::new(0));
    let eu_west_3a = eu_west_3a
        .name("eu-west-3a")
        .data(users.clone())
        .data(users_count.clone())
        .subscribe(
            Subscriber::new("users")
                .filter("user/#")
                .handler(|event, ctx| {
                    let users = ctx
                        .0
                        .read()
                        .extract::<Arc<RwLock<HashMap<String, User>>>>()
                        .clone();

                    async move {
                        let user_event: UserEvent = event.name.parse().unwrap();

                        match user_event {
                            UserEvent::Created => {
                                let data: Created = event.to_data().unwrap();
                                let mut w_users = users.write().await;
                                w_users.insert(
                                    User::to_id(event.aggregate_id),
                                    User {
                                        username: data.username,
                                        password: data.password,
                                        ..Default::default()
                                    },
                                );
                            }
                            UserEvent::DisplayNameUpdated => {
                                let data: DisplayNameUpdated = event.to_data().unwrap();
                                let mut w_users = users.write().await;

                                if let Some(user) =
                                    w_users.get_mut(&User::to_id(event.aggregate_id))
                                {
                                    user.display_name = Some(data.display_name);
                                }
                            }
                            _ => {}
                        };

                        Ok(())
                    }
                    .boxed()
                }),
        )
        .subscribe(
            Subscriber::new("user-count")
                .filter("user/1/+")
                .filter("user/2/+")
                .handler(|event, ctx| {
                    let users_count = ctx.0.read().extract::<Arc<RwLock<i32>>>().clone();
                    async move {
                        let user_event: UserEvent = event.name.parse().unwrap();

                        if let UserEvent::Created = user_event {
                            let mut users_count = users_count.write().await;
                            *users_count += 1;
                        };

                        Ok(())
                    }
                    .boxed()
                }),
        )
        .run_with_delay(Duration::from_secs(0))
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "john.doe".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "John doe".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "2",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "albert.dupont".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "Albert Dupont".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "3",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "cataleya.restrepo".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "Cataleya Restrepo".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let users = users.read().await;
    let user1 = users.get("1").unwrap();
    let user2 = users.get("2").unwrap();
    let user3 = users.get("3").unwrap();

    assert_eq!(user1.username, "john.doe");
    assert_eq!(user1.display_name, Some("John doe".to_owned()));
    assert_eq!(user2.username, "albert.dupont");
    assert_eq!(user2.display_name, Some("Albert Dupont".to_owned()));
    assert_eq!(user3.username, "cataleya.restrepo");
    assert_eq!(user3.display_name, Some("Cataleya Restrepo".to_owned()));

    let count = users_count.read().await;
    assert_eq!(count.to_owned(), 2);
}

async fn deadletter<E: Engine + Sync + Send + 'static, S: StoreEngine + Sync + Send + 'static>(
    eu_west_3a: Evento<E, S>,
) {
    let users: Arc<RwLock<HashMap<String, User>>> = Arc::new(RwLock::new(HashMap::new()));
    let engine = eu_west_3a.engine.clone();
    let eu_west_3a = eu_west_3a
        .name("eu-west-3a")
        .data(users.clone())
        .subscribe(
            Subscriber::new("users")
                .filter("user/#")
                .handler(|event, ctx| {
                    let users = ctx
                        .0
                        .read()
                        .extract::<Arc<RwLock<HashMap<String, User>>>>()
                        .clone();

                    async move {
                        let user_event: UserEvent = event.name.parse().unwrap();

                        match user_event {
                            UserEvent::Created => {
                                let data: Created = event.to_data().unwrap();
                                let mut w_users = users.write().await;
                                w_users.insert(
                                    User::to_id(event.aggregate_id),
                                    User {
                                        username: data.username,
                                        password: data.password,
                                        ..Default::default()
                                    },
                                );
                            }
                            UserEvent::DisplayNameUpdated => {
                                let data: DisplayNameUpdated = event.to_data().unwrap();
                                let mut w_users = users.write().await;

                                if let Some(user) =
                                    w_users.get_mut(&User::to_id(event.aggregate_id))
                                {
                                    user.display_name = Some(data.display_name);
                                }
                            }
                            _ => {}
                        };

                        Ok(())
                    }
                    .boxed()
                })
                .handler(|event, _ctx| {
                    async move {
                        let user_event: UserEvent = event.name.parse().unwrap();

                        if let UserEvent::DisplayNameUpdated = user_event {
                            return Err(SubscirberHandlerError {
                                code: "send_email".to_owned(),
                                reason: "Connection refused.".to_owned(),
                            });
                        };

                        Ok(())
                    }
                    .boxed()
                }),
        )
        .run_with_delay(Duration::from_secs(0))
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "john.doe".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "John doe".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "2",
            vec![Event::new(UserEvent::Created)
                .data(Created {
                    username: "albert.dupont".to_owned(),
                    password: "azerty".to_owned(),
                })
                .unwrap()],
            0,
        )
        .await
        .unwrap();

    eu_west_3a
        .publish::<User, _>(
            "3",
            vec![
                Event::new(UserEvent::Created)
                    .data(Created {
                        username: "cataleya.restrepo".to_owned(),
                        password: "azerty".to_owned(),
                    })
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "Cataleya Restrepo".to_owned(),
                    })
                    .unwrap(),
            ],
            0,
        )
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let users = users.read().await;
    let user1 = users.get("1").unwrap();
    let user2 = users.get("2").unwrap();
    let user3 = users.get("3").unwrap();

    assert_eq!(user1.username, "john.doe");
    assert_eq!(user1.display_name, Some("John doe".to_owned()));
    assert_eq!(user2.username, "albert.dupont");
    assert_eq!(user2.display_name, None);
    assert_eq!(user3.username, "cataleya.restrepo");
    assert_eq!(user3.display_name, Some("Cataleya Restrepo".to_owned()));

    let event = engine.read_deadletters(10, None).await.unwrap()[0].clone();
    let metadata = event.metadata.unwrap();

    let subscription_key = metadata.get("_evento_subscription_key").unwrap();
    let errors = metadata.get("_evento_errors").unwrap();

    assert_eq!(subscription_key.clone(), json!("eu-west-3a.users"));
    assert_eq!(
        errors.clone(),
        json!([{
            "code":"send_email",
            "reason": "Connection refused."
        }])
    );
}

async fn create_pg_store(db_name: &str, reset: bool) -> Evento<PgEngine, evento::store::PgEngine> {
    if reset {
        let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/postgres")
            .await
            .unwrap();

        let mut conn = pool.acquire().await.unwrap();

        conn.execute(&format!("drop database if exists evento_{db_name};")[..])
            .await
            .unwrap();

        conn.execute(&format!("create database evento_{db_name};")[..])
            .await
            .unwrap();

        drop(pool);
    }

    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@localhost:5432/evento_{db_name}"
    ))
    .await
    .unwrap();

    sqlx::migrate!("../migrations").run(&pool).await.unwrap();

    PgEngine::new(pool)
}
