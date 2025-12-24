use anyhow::Ok;
use bank::DepositMoney;
use evento::{metadata::Metadata, AggregatorBuilder, Event, Executor};
use ulid::Ulid;

pub async fn load<E: Executor>(executor: &E) -> anyhow::Result<()> {
    let john_id = bank::Command::open_account(
        bank::OpenAccount {
            owner_id: Ulid::new().to_string(),
            owner_name: "john".to_owned(),
            account_type: bank::AccountType::Business,
            currency: "EUR".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    let projection = bank::create_projection::<E>();
    let cmd = projection.load(executor, &john_id).await?.unwrap();
    cmd.transfer_money(
        bank::TransferMoney {
            amount: 350,
            to_account_id: Ulid::new().to_string(),
            transaction_id: Ulid::new().to_string(),
            description: "".to_owned(),
        },
        executor,
    )
    .await?;

    let cmd = bank::create_projection::<E>()
        .load(executor, &john_id)
        .await?
        .unwrap();

    assert_eq!(cmd.balance, 650);

    cmd.transfer_money(
        bank::TransferMoney {
            amount: 150,
            to_account_id: Ulid::new().to_string(),
            transaction_id: Ulid::new().to_string(),
            description: "".to_owned(),
        },
        executor,
    )
    .await?;

    let cmd = bank::create_projection::<E>()
        .load(executor, &john_id)
        .await?
        .unwrap();

    assert_eq!(cmd.balance, 500);

    cmd.deposit_money(
        DepositMoney {
            amount: 10_000,
            description: "".to_owned(),
            transaction_id: Ulid::new().to_string(),
        },
        executor,
    )
    .await?;

    let cmd = bank::create_projection::<E>()
        .load(executor, &john_id)
        .await?
        .unwrap();
    assert_eq!(cmd.balance, 10500);

    Ok(())
}

pub async fn version<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // let id = evento::create()
    //     .metadata(&true)?
    //     .event(&Added { value: 2 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    // assert_eq!(calcul.event.version, 1);
    //
    // evento::aggregator(&id)
    //     .metadata(&true)?
    //     .event(&Added { value: 9 })?
    //     .event(&Added { value: 3 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id).await?;
    // assert_eq!(calcul.event.version, 3);
    //
    // let id = evento::create()
    //     .metadata(&true)?
    //     .event(&Added { value: 2 })?
    //     .event(&Added { value: 32 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    // assert_eq!(calcul.event.version, 2);
    //
    // evento::aggregator_from(calcul.clone())
    //     .metadata(&true)?
    //     .event(&Added { value: 9 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id).await?;
    // assert_eq!(calcul.event.version, 3);

    Ok(())
}

pub async fn routing_key<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // let id = evento::create()
    //     .metadata(&true)?
    //     .event(&Added { value: 2 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    // assert_eq!(calcul.event.routing_key, None);
    //
    // evento::aggregator_from(calcul.clone())
    //     .routing_key("routing1")
    //     .metadata(&true)?
    //     .event(&Added { value: 9 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id).await?;
    // assert_eq!(calcul.event.routing_key, None);
    //
    // let id = evento::create()
    //     .routing_key("routing1")
    //     .metadata(&true)?
    //     .event(&Added { value: 2 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    // assert_eq!(calcul.event.routing_key, Some("routing1".to_owned()));
    //
    // evento::aggregator(&id)
    //     .metadata(&true)?
    //     .event(&Added { value: 9 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id).await?;
    // assert_eq!(calcul.event.routing_key, Some("routing1".to_owned()));

    Ok(())
}

pub async fn invalid_original_version<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // let id = evento::create()
    //     .metadata(&true)?
    //     .event(&Added { value: 2 })?
    //     .commit(executor)
    //     .await?;
    //
    // let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    // evento::aggregator(&id)
    //     .metadata(&true)?
    //     .event(&Added { value: 9 })?
    //     .commit(executor)
    //     .await?;
    //
    // let res = evento::aggregator_from(calcul)
    //     .metadata(&true)?
    //     .event(&Multiplied { value: 3 })?
    //     .commit(executor)
    //     .await;
    //
    // assert_eq!(
    //     res.map_err(|e| e.to_string()),
    //     Err(WriteError::InvalidOriginalVersion.to_string())
    // );
    //
    // let calcul = evento::load::<Calcul, _>(executor, id).await?;
    // evento::aggregator_from(calcul)
    //     .metadata(&true)?
    //     .event(&Subtracted { value: 39 })?
    //     .commit(executor)
    //     .await?;

    Ok(())
}

pub async fn subscriber_running<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    // let sub1 = evento::subscribe("sub").all().aggregator::<Calcul>();
    //
    // sub1.init(executor).await?;
    // let sub2 = evento::subscribe("sub").all().aggregator::<Calcul>();
    //
    // sub2.init(executor).await?;
    //
    // assert!(!sub1.is_subscriber_running(executor).await?);
    // assert!(sub2.is_subscriber_running(executor).await?);

    Ok(())
}

pub async fn subscribe<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    // let events = events
    //     .into_iter()
    //     .filter(|e| e.aggregator_type == Calcul::name())
    //     .collect::<Vec<_>>();
    //
    // let events = evento::cursor::Reader::new(events)
    //     .forward(1000, None)
    //     .execute()?;
    //
    // let sub1 = evento::subscribe("sub1").all().aggregator::<Calcul>();
    //
    // sub1.init(executor).await?;
    //
    // let sub2 = evento::subscribe("sub2")
    //     .chunk_size(5)
    //     .all()
    //     .aggregator::<Calcul>();
    //
    // sub2.init(executor).await?;
    //
    // let sub1_events = sub1.read(executor).await?;
    // for (index, edge) in events.edges.iter().enumerate() {
    //     let sub1_event = sub1_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub1_event, Some(&edge.node));
    //     sub1_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub1_events = sub1.read(executor).await?;
    // assert!(sub1_events.is_empty());
    //
    // let sub2_events = sub2.read(executor).await?;
    // for (index, edge) in events.edges.iter().take(5).enumerate() {
    //     let sub2_event = sub2_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub2_event, Some(&edge.node));
    //     sub2_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub2_events = sub2.read(executor).await?;
    // for (index, edge) in events.edges.iter().skip(5).enumerate() {
    //     let sub2_event = sub2_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub2_event, Some(&edge.node));
    //     sub2_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub2_events = sub2.read(executor).await?;
    // assert!(sub2_events.is_empty());

    Ok(())
}

pub async fn subscribe_routing_key<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    // let events = events
    //     .into_iter()
    //     .filter(|e| {
    //         e.aggregator_type == Calcul::name() && e.routing_key == Some("eu-west-3".to_owned())
    //     })
    //     .collect::<Vec<_>>();
    //
    // let events = evento::cursor::Reader::new(events)
    //     .forward(1000, None)
    //     .execute()?;
    //
    // let sub1 = evento::subscribe("sub1")
    //     .all()
    //     .routing_key("eu-west-3")
    //     .aggregator::<Calcul>();
    //
    // sub1.init(executor).await?;
    //
    // let sub1_events = sub1.read(executor).await?;
    // for (index, edge) in events.edges.iter().enumerate() {
    //     let sub1_event = sub1_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub1_event, Some(&edge.node));
    //     sub1_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub1_events = sub1.read(executor).await?;
    // assert!(sub1_events.is_empty());

    Ok(())
}

pub async fn subscribe_default<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    // let events = events
    //     .into_iter()
    //     .filter(|e| e.aggregator_type == Calcul::name() && e.routing_key.is_none())
    //     .collect::<Vec<_>>();
    //
    // let events = evento::cursor::Reader::new(events)
    //     .forward(1000, None)
    //     .execute()?;
    //
    // let sub1 = evento::subscribe("sub1").aggregator::<Calcul>();
    //
    // sub1.init(executor).await?;
    //
    // let sub1_events = sub1.read(executor).await?;
    // for (index, edge) in events.edges.iter().enumerate() {
    //     let sub1_event = sub1_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub1_event, Some(&edge.node));
    //     sub1_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub1_events = sub1.read(executor).await?;
    // assert!(sub1_events.is_empty());

    Ok(())
}

pub async fn subscribe_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    // let events = evento::cursor::Reader::new(events)
    //     .forward(1000, None)
    //     .execute()?;
    //
    // let sub1 = evento::subscribe("sub1")
    //     .all()
    //     .aggregator::<Calcul>()
    //     .aggregator::<MyCalcul>();
    //
    // sub1.init(executor).await?;
    //
    // let sub2 = evento::subscribe("sub2")
    //     .chunk_size(5)
    //     .all()
    //     .aggregator::<Calcul>()
    //     .aggregator::<MyCalcul>();
    //
    // sub2.init(executor).await?;
    //
    // let sub1_events = sub1.read(executor).await?;
    // for (index, edge) in events.edges.iter().enumerate() {
    //     let sub1_event = sub1_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub1_event, Some(&edge.node));
    //     sub1_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub1_events = sub1.read(executor).await?;
    // assert!(sub1_events.is_empty());
    //
    // let sub2_events = sub2.read(executor).await?;
    // for (index, edge) in events.edges.iter().take(5).enumerate() {
    //     let sub2_event = sub2_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub2_event, Some(&edge.node));
    //     sub2_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub2_events = sub2.read(executor).await?;
    // for (index, edge) in events.edges.iter().skip(5).enumerate() {
    //     let sub2_event = sub2_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub2_event, Some(&edge.node));
    //     sub2_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub2_events = sub2.read(executor).await?;
    // assert!(sub2_events.is_empty());

    Ok(())
}

pub async fn subscribe_routing_key_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    // let events = events
    //     .into_iter()
    //     .filter(|e| e.routing_key == Some("eu-west-3".to_owned()))
    //     .collect::<Vec<_>>();
    //
    // let events = evento::cursor::Reader::new(events)
    //     .forward(1000, None)
    //     .execute()?;
    //
    // let sub1 = evento::subscribe("sub1")
    //     .all()
    //     .routing_key("eu-west-3")
    //     .aggregator::<Calcul>()
    //     .aggregator::<MyCalcul>();
    //
    // sub1.init(executor).await?;
    //
    // let sub1_events = sub1.read(executor).await?;
    // for (index, edge) in events.edges.iter().enumerate() {
    //     let sub1_event = sub1_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub1_event, Some(&edge.node));
    //     sub1_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub1_events = sub1.read(executor).await?;
    // assert!(sub1_events.is_empty());

    Ok(())
}

pub async fn subscribe_default_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    // let events = events
    //     .into_iter()
    //     .filter(|e| e.routing_key.is_none())
    //     .collect::<Vec<_>>();
    //
    // let events = evento::cursor::Reader::new(events)
    //     .forward(1000, None)
    //     .execute()?;
    //
    // let sub1 = evento::subscribe("sub1")
    //     .aggregator::<Calcul>()
    //     .aggregator::<MyCalcul>();
    //
    // sub1.init(executor).await?;
    //
    // let sub1_events = sub1.read(executor).await?;
    // for (index, edge) in events.edges.iter().enumerate() {
    //     let sub1_event = sub1_events.get(index).map(|c| &c.event);
    //     assert_eq!(sub1_event, Some(&edge.node));
    //     sub1_events.get(index).unwrap().acknowledge().await?;
    // }
    //
    // let sub1_events = sub1.read(executor).await?;
    // assert!(sub1_events.is_empty());

    Ok(())
}

#[evento::aggregator]
pub enum User {
    NameChanged { value: String },
}

#[derive(Debug, Default, Clone)]
struct UserCommand {
    pub id: String,
    pub name: String,
    pub version: u16,
    pub routing_key: Option<String>,
}

#[evento::snapshot]
async fn restore<E: Executor>(
    context: &evento::context::RwContext,
    id: String,
) -> anyhow::Result<Option<UserCommand>> {
    Ok(None)
}

#[derive(Debug, Clone)]
pub struct ChangeName {
    pub name: String,
}

impl UserCommand {
    fn aggregator(&self) -> AggregatorBuilder {
        evento::aggregator(&self.id)
            .original_version(self.version)
            .routing_key_opt(self.routing_key.to_owned())
    }

    /// Handle ChangeDailyWithdrawalLimit command
    pub async fn change_name<E: Executor>(
        &self,
        cmd: ChangeName,
        executor: &E,
    ) -> anyhow::Result<()> {
        self.aggregator()
            .event(&NameChanged { value: cmd.name })?
            .metadata(&Metadata::default())?
            .commit(executor)
            .await?;

        Ok(())
    }
}

// #[evento::handler]
// async fn handle_name_changed<E: Executor>(
//     event: evento::metadata::Event<NameChanged>,
//     action: Action<'_, UserCommand, E>,
// ) -> anyhow::Result<()> {
//     Ok(())
// }
