use bank::{
    ChangeDailyWithdrawalLimit, Command, Created, DepositMoney, MoneyReceived, MoneyTransferred,
    COMMAND_ROWS,
};
use evento::{metadata::Metadata, Event, Executor, WriteError};
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

    let cmd = bank::load(executor, &john_id).await?.unwrap();
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

    assert_eq!(cmd.0.version, 1);

    let cmd = bank::load(executor, &john_id).await?.unwrap();
    cmd.change_daily_withdrawal_limit(ChangeDailyWithdrawalLimit { new_limit: 233 }, executor)
        .await?;

    let cmd = bank::load(executor, &john_id).await?.unwrap();

    assert_eq!(cmd.balance, 650);
    assert_eq!(cmd.0.version, 3);

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

    let cmd = bank::load(executor, &john_id).await?.unwrap();

    assert_eq!(cmd.balance, 500);
    assert_eq!(cmd.0.version, 4);

    cmd.deposit_money(
        DepositMoney {
            amount: 10_000,
            description: "".to_owned(),
            transaction_id: Ulid::new().to_string(),
        },
        executor,
    )
    .await?;

    let cmd = bank::load(executor, &john_id).await?.unwrap();
    assert_eq!(cmd.balance, 10500);
    assert_eq!(cmd.0.version, 5);

    Ok(())
}

pub async fn routing_key<E: Executor>(executor: &E) -> anyhow::Result<()> {
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
    let cmd = bank::load(executor, &john_id).await?.unwrap();
    assert_eq!(cmd.0.routing_key, None);

    cmd.transfer_money_with_routing(
        bank::TransferMoney {
            amount: 350,
            to_account_id: Ulid::new().to_string(),
            transaction_id: Ulid::new().to_string(),
            description: "".to_owned(),
        },
        executor,
        "routing1",
    )
    .await?;
    let cmd = bank::load(executor, &john_id).await?.unwrap();
    assert_eq!(cmd.0.routing_key, None);

    let albert_id = bank::Command::open_account_with_routing(
        bank::OpenAccount {
            owner_id: Ulid::new().to_string(),
            owner_name: "albert".to_owned(),
            account_type: bank::AccountType::Savings,
            currency: "EUR".to_owned(),
            initial_balance: 1000,
        },
        executor,
        "routing1",
    )
    .await?;

    let cmd = bank::load(executor, &albert_id).await?.unwrap();
    assert_eq!(cmd.0.routing_key, Some("routing1".to_owned()));

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

    let cmd = bank::load(executor, &albert_id).await?.unwrap();
    assert_eq!(cmd.0.routing_key, Some("routing1".to_owned()));

    Ok(())
}

pub async fn invalid_original_version<E: Executor>(executor: &E) -> anyhow::Result<()> {
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
    let cmd = bank::load(executor, &john_id).await?.unwrap();
    let cmd2 = bank::load(executor, &john_id).await?.unwrap();
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

    assert_eq!(cmd.0.version, 1);

    let res = cmd2
        .transfer_money(
            bank::TransferMoney {
                amount: 350,
                to_account_id: Ulid::new().to_string(),
                transaction_id: Ulid::new().to_string(),
                description: "".to_owned(),
            },
            executor,
        )
        .await;

    assert_eq!(
        res.map_err(|e| e.to_string()),
        Err(WriteError::InvalidOriginalVersion.to_string())
    );
    let cmd2 = bank::load(executor, &john_id).await?.unwrap();
    cmd2.transfer_money(
        bank::TransferMoney {
            amount: 350,
            to_account_id: Ulid::new().to_string(),
            transaction_id: Ulid::new().to_string(),
            description: "".to_owned(),
        },
        executor,
    )
    .await?;

    Ok(())
}

pub async fn subscriber_running<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let sub1 = bank::subscription().all().start(executor).await?;
    let sub2 = bank::subscription().all().start(executor).await?;

    assert!(
        !executor
            .is_subscriber_running("command".to_owned(), sub1.id)
            .await?
    );
    assert!(
        executor
            .is_subscriber_running("command".to_owned(), sub2.id)
            .await?
    );

    Ok(())
}

pub async fn subscribe<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
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
    let cmd = bank::load(executor, &john_id).await?.unwrap();
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
    bank::subscription().execute(executor).await?;

    let views = COMMAND_ROWS.read().unwrap();
    for v in views.values() {
        println!("{}", v.0.balance);
    }

    let john = views.get(&john_id);

    assert_eq!(views.len(), 1);
    assert!(john.is_some());

    let john = john.unwrap();

    assert_eq!(john.0.balance, 650);
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

struct TestEngine<'a, E: Executor> {
    pub john: String,
    pub albert: String,
    pub executor: &'a E,
}

impl<'a, E: Executor> TestEngine<'a, E> {
    async fn create(executor: &'a E) -> anyhow::Result<Self> {
        let john = evento::create()
            .event(&Created {
                name: "john".to_owned(),
            })?
            .metadata(&Metadata::default())?
            .commit(executor)
            .await?;

        let albert = evento::create()
            .event(&Created {
                name: "albert".to_owned(),
            })?
            .metadata(&Metadata::default())?
            .commit(executor)
            .await?;

        Ok(Self {
            john,
            albert,
            executor,
        })
    }

    pub async fn transfer_money(
        &self,
        from: impl Into<String>,
        to: impl Into<String>,
        amount: i64,
    ) -> anyhow::Result<()> {
        let from = self.load(from).await?;

        if !from.can_transfer(amount) {
            anyhow::bail!("cannot transfer {amount}");
        }

        let to = self.load(to).await?;

        if !to.is_active() {
            anyhow::bail!("to not active");
        }

        evento::aggregator(&from.account_id)
            .original_version(from.0.version as u16)
            .routing_key_opt(from.0.routing_key.to_owned())
            .event(&MoneyTransferred {
                amount,
                description: "".to_owned(),
                to_account_id: to.account_id.to_owned(),
                transaction_id: Ulid::new().to_string(),
            })?
            .metadata(&Metadata::default())?
            .commit(self.executor)
            .await?;

        evento::aggregator(&to.account_id)
            .original_version(to.0.version as u16)
            .routing_key_opt(to.0.routing_key)
            .event(&MoneyReceived {
                amount,
                description: "".to_owned(),
                from_account_id: from.account_id.to_owned(),
                transaction_id: Ulid::new().to_string(),
            })?
            .metadata(&Metadata::default())?
            .commit(self.executor)
            .await?;

        Ok(())
    }

    pub async fn load(&self, id: impl Into<String>) -> anyhow::Result<Command> {
        let Some(bank_account) = bank::load(self.executor, id).await? else {
            anyhow::bail!("account not found");
        };

        Ok(bank_account)
    }

    pub async fn john(&self, id: impl Into<String>) -> anyhow::Result<Command> {
        self.load(&self.john).await
    }

    pub async fn albert(&self, id: impl Into<String>) -> anyhow::Result<Command> {
        self.load(&self.albert).await
    }
}
