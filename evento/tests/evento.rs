use evento::store::{Engine as StoreEngine, MemoryEngine as StoreMemoryEngine};
use evento::{Engine, Event, Evento, MemoryEngine};
use lazy_static::lazy_static;
// use sqlx::{Executor, PgPool};
use std::collections::HashMap;

use crate::common::{AccountDeleted, Created, ProfileUpdated, User, UserEvent};

mod common;

lazy_static! {
    static ref USERS: HashMap<&'static str, User> = HashMap::new();
}

#[tokio::test]
async fn memory_publish() {
    let evento = create_memory_store().await;

    publish(evento).await
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

async fn publish<E: Engine, S: StoreEngine>(evento: Evento<E, S>) {
    let user_1 = USERS.get("1").unwrap();

    assert_eq!(user_1.username, "john.doe");
    assert_eq!(user_1.password, "azerty");
    assert_eq!(user_1.deleted, false);
}

async fn create_memory_store() -> Evento<MemoryEngine, StoreMemoryEngine> {
    let store = StoreMemoryEngine::new();
    let evento = MemoryEngine::new(store);
    init_evento(&evento).await;

    evento
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

async fn init_evento<'a, E: Engine, S: StoreEngine>(evento: &'a Evento<E, S>) {
    evento
        .publish::<User, _>(
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

    evento
        .publish::<User, _>(
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
