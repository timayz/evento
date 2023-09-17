use evento_store::{Aggregate, Engine, Error, Event, EventStore, MemoryEngine};
use std::collections::HashMap;

mod common;

use crate::common::{
    create_pg_store, init_store, AccountDeleted, Created, DisplayNameUpdated, PasswordUpdated,
    ProfileUpdated, User, UserEvent,
};

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

    assert!(!user.deleted);

    user.apply(
        &Event::new(UserEvent::AccountDeleted)
            .aggregate_id(User::aggregate_id("1"))
            .data(AccountDeleted { deleted: true })
            .unwrap()
            .version(2),
    );

    assert!(user.deleted)
}

#[tokio::test]
async fn memory_save() {
    let store = MemoryEngine::new();
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

#[tokio::test]
async fn pg_save() {
    let (store, _) = create_pg_store(Some("save"), false).await;
    save(store).await
}

#[tokio::test]
async fn pg_load_save() {
    let (store, _) = create_pg_store(Some("load_save"), true).await;
    load_save(store).await;
}

#[tokio::test]
async fn pg_save_wrong_version() {
    let (store, _) = create_pg_store(None, true).await;
    save_wrong_version(store).await;
}

async fn save<E: Engine>(store: EventStore<E>) {
    let last_event = store
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
        .map(|events| events.last().cloned())
        .unwrap()
        .unwrap();

    assert_eq!(last_event.version, 2);
    assert_eq!(last_event.aggregate_id, "user_1");
}

async fn load_save<E: Engine>(store: EventStore<E>) {
    let (john, last_event) = store.load::<User, _>("1").await.unwrap().unwrap();

    assert_eq!(john.username, "john.doe");
    assert_eq!(john.password, "azerty");
    assert_eq!(john.first_name, None);
    assert_eq!(john.last_name, None);

    let last_event = store
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
        .map(|events| events.last().cloned())
        .unwrap()
        .unwrap();

    assert_eq!(last_event.version, 3);

    let last_event = store
        .save::<User, _>(
            "1",
            vec![
                Event::new(UserEvent::PasswordUpdated)
                    .data(PasswordUpdated {
                        old_password: "azerty".to_owned(),
                        new_password: "securepwd".to_owned(),
                    })
                    .unwrap()
                    .metadata(HashMap::from([("algo".to_owned(), "RS256".to_owned())]))
                    .unwrap(),
                Event::new(UserEvent::DisplayNameUpdated)
                    .data(DisplayNameUpdated {
                        display_name: "john007".to_owned(),
                    })
                    .unwrap(),
            ],
            last_event.version,
        )
        .await
        .map(|events| events.last().cloned())
        .unwrap()
        .unwrap();

    assert_eq!(last_event.version, 5);

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

    let ov = store
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
        .map(|events| events.last().cloned())
        .unwrap()
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

    assert_eq!(err, Error::UnexpectedOriginalVersion);

    let ovf = store
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
            ov.version,
        )
        .await
        .map(|events| events.last().cloned())
        .unwrap()
        .unwrap();

    assert_eq!(ov.version + 2, ovf.version);
}

async fn create_memory_store() -> EventStore<MemoryEngine> {
    let store = MemoryEngine::new();
    init_store(&store).await;

    store
}
