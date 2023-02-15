use evento::store::{Engine as StoreEngine, MemoryEngine as StoreMemoryEngine};
use evento::{Engine, Event, Evento, MemoryEngine, Subscriber};
use futures_util::FutureExt;
use tokio::sync::RwLock;
// use sqlx::{Executor, PgPool};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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

// #[tokio::test]
// async fn pg_save() {
//     let store = create_pg_store("save", false).await;
//     save(store).await
// }

// #[tokio::test]
// async fn pg_load_save() {
//     let store = create_pg_store("load_save", true).await;
//     load_save(store).await;
// }

// #[tokio::test]
// async fn pg_save_wrong_version() {
//     let store = create_pg_store("save_wrong_version", true).await;
//     save_wrong_version(store).await;
// }

async fn publish<E: Engine, S: StoreEngine>(
    eu_west_3a: Evento<E, S>,
    eu_west_3b: Evento<E, S>,
    us_east_1a: Evento<E, S>,
) {
    let subscriber = Subscriber::new("users")
        .filter("user/+")
        .handler(|event, ctx| {
            let users = ctx.extract::<Arc<RwLock<HashMap<String, User>>>>().clone();
            async move {
                let user_event: UserEvent = event.name.parse().unwrap();

                match user_event {
                    UserEvent::Created => {
                        let data: Created = event.to_data().unwrap();
                        let mut w_users = users.write().await;
                        w_users.insert(
                            event.aggregate_id.to_owned(),
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

                        if let Some(user) = w_users.get_mut(&event.aggregate_id.to_owned()) {
                            user.display_name = Some(data.display_name);
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
        .subscribe(subscriber.clone());
    let eu_west_3b = eu_west_3b
        .name("eu-west-3b")
        .data(users.clone())
        .subscribe(subscriber.clone());
    let us_east_1a = us_east_1a
        .name("us-east-1a")
        .data(users.clone())
        .subscribe(subscriber.clone());

    eu_west_3a.run().await;
    eu_west_3b.run().await;
    us_east_1a.run().await;

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
            vec![
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "John Wick".to_owned(),
                    })
                    .unwrap(),
            ],
            2,
        )
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let users = users.read().await;
    let user1 = users.get("1").unwrap();

    assert_eq!(user1.username, "john.doe (eu-west-3a)");
    assert_eq!(user1.display_name, Some("John Wick (eu-west-3a)".to_owned()));
}

async fn filter<E: Engine, S: StoreEngine>(eu_west_3a: Evento<E, S>) {
    let users: Arc<RwLock<HashMap<String, User>>> = Arc::new(RwLock::new(HashMap::new()));
    let users_count: Arc<RwLock<i32>> = Arc::new(RwLock::new(0));
    let eu_west_3a = eu_west_3a
        .name("eu-west-3a")
        .data(users.clone())
        .subscribe(
            Subscriber::new("users")
                .filter("user/+")
                .handler(|event, ctx| {
                    let users = ctx.extract::<Arc<RwLock<HashMap<String, User>>>>().clone();
                    async move {
                        let user_event: UserEvent = event.name.parse().unwrap();

                        match user_event {
                            UserEvent::Created => {
                                let data: Created = event.to_data().unwrap();
                                let mut w_users = users.write().await;
                                w_users.insert(
                                    event.aggregate_id.to_owned(),
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

                                if let Some(user) = w_users.get_mut(&event.aggregate_id.to_owned())
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
                    let users_count = ctx.extract::<Arc<RwLock<i32>>>().clone();
                    async move {
                        let user_event: UserEvent = event.name.parse().unwrap();

                        match user_event {
                            UserEvent::Created => {
                                let mut users_count = users_count.write().await;
                                *users_count += 1;
                            }
                            _ => {}
                        };

                        Ok(())
                    }
                    .boxed()
                }),
        );

    eu_west_3a.run().await;

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

// async fn create_pg_store(db_name: &str, init: bool) -> EventStore<PgEngine> {
//     let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/postgres")
//         .await
//         .unwrap();

//     let mut conn = pool.acquire().await.unwrap();

//     conn.execute(&format!("drop database if exists evento_{};", db_name)[..])
//         .await
//         .unwrap();

//     conn.execute(&format!("create database evento_{};", db_name)[..])
//         .await
//         .unwrap();

//     drop(pool);

//     let pool = PgPool::connect(&format!(
//         "postgres://postgres:postgres@localhost:5432/evento_{}",
//         db_name
//     ))
//     .await
//     .unwrap();

//     sqlx::migrate!("../migrations").run(&pool).await.unwrap();

//     let store = PgEngine::new(pool);

//     if init {
//         init_store(&store).await;
//     }

//     store
// }
