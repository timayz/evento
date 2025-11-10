use bincode::{Decode, Encode};
use evento::{AggregatorName, Event, EventDetails, Executor, WriteError};

pub async fn load<E: Executor>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create::<Calcul>()
        .data(&Added { value: 13 })?
        .data(&Subtracted { value: 3 })?
        .data(&Multiplied { value: 3 })?
        .data(&Divided { value: 10 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    assert_eq!(calcul.item.value, 3);

    evento::save_with(calcul)
        .metadata(&true)?
        .data(&Added { value: 9 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    assert_eq!(calcul.item.value, 12);
    Ok(())
}

pub async fn version<E: Executor>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create::<Calcul>()
        .metadata(&true)?
        .data(&Added { value: 2 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    assert_eq!(calcul.event.version, 1);

    evento::save::<Calcul>(&id)
        .metadata(&true)?
        .data(&Added { value: 9 })?
        .data(&Added { value: 3 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id).await?;
    assert_eq!(calcul.event.version, 3);

    let id = evento::create::<Calcul>()
        .metadata(&true)?
        .data(&Added { value: 2 })?
        .data(&Added { value: 32 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    assert_eq!(calcul.event.version, 2);

    evento::save_with(calcul.clone())
        .metadata(&true)?
        .data(&Added { value: 9 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id).await?;
    assert_eq!(calcul.event.version, 3);

    Ok(())
}

pub async fn routing_key<E: Executor>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create::<Calcul>()
        .metadata(&true)?
        .data(&Added { value: 2 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    assert_eq!(calcul.event.routing_key, None);

    evento::save_with(calcul.clone())
        .routing_key("routing1")
        .metadata(&true)?
        .data(&Added { value: 9 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id).await?;
    assert_eq!(calcul.event.routing_key, None);

    let id = evento::create::<Calcul>()
        .routing_key("routing1")
        .metadata(&true)?
        .data(&Added { value: 2 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    assert_eq!(calcul.event.routing_key, Some("routing1".to_owned()));

    evento::save::<Calcul>(&id)
        .metadata(&true)?
        .data(&Added { value: 9 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id).await?;
    assert_eq!(calcul.event.routing_key, Some("routing1".to_owned()));

    Ok(())
}

pub async fn invalid_original_version<E: Executor>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create::<Calcul>()
        .metadata(&true)?
        .data(&Added { value: 2 })?
        .commit(executor)
        .await?;

    let calcul = evento::load::<Calcul, _>(executor, id.to_owned()).await?;
    evento::save::<Calcul>(&id)
        .metadata(&true)?
        .data(&Added { value: 9 })?
        .commit(executor)
        .await?;

    let res = evento::save_with(calcul)
        .metadata(&true)?
        .data(&Multiplied { value: 3 })?
        .commit(executor)
        .await;

    assert_eq!(
        res.map_err(|e| e.to_string()),
        Err(WriteError::InvalidOriginalVersion.to_string())
    );

    let calcul = evento::load::<Calcul, _>(executor, id).await?;
    evento::save_with(calcul)
        .metadata(&true)?
        .data(&Subtracted { value: 39 })?
        .commit(executor)
        .await?;

    Ok(())
}

pub async fn subscriber_running<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let sub1 = evento::subscribe("sub").all().aggregator::<Calcul>();

    sub1.init(executor).await?;
    let sub2 = evento::subscribe("sub").all().aggregator::<Calcul>();

    sub2.init(executor).await?;

    assert!(!sub1.is_subscriber_running(executor).await?);
    assert!(sub2.is_subscriber_running(executor).await?);

    Ok(())
}

pub async fn subscribe<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    let events = events
        .into_iter()
        .filter(|e| e.aggregator_type == Calcul::name())
        .collect::<Vec<_>>();

    let events = evento::cursor::Reader::new(events)
        .forward(1000, None)
        .execute()?;

    let sub1 = evento::subscribe("sub1").all().aggregator::<Calcul>();

    sub1.init(executor).await?;

    let sub2 = evento::subscribe("sub2")
        .chunk_size(5)
        .all()
        .aggregator::<Calcul>();

    sub2.init(executor).await?;

    let sub1_events = sub1.read(executor).await?;
    for (index, edge) in events.edges.iter().enumerate() {
        let sub1_event = sub1_events.get(index).map(|c| &c.event);
        assert_eq!(sub1_event, Some(&edge.node));
        sub1_events.get(index).unwrap().acknowledge().await?;
    }

    let sub1_events = sub1.read(executor).await?;
    assert!(sub1_events.is_empty());

    let sub2_events = sub2.read(executor).await?;
    for (index, edge) in events.edges.iter().take(5).enumerate() {
        let sub2_event = sub2_events.get(index).map(|c| &c.event);
        assert_eq!(sub2_event, Some(&edge.node));
        sub2_events.get(index).unwrap().acknowledge().await?;
    }

    let sub2_events = sub2.read(executor).await?;
    for (index, edge) in events.edges.iter().skip(5).enumerate() {
        let sub2_event = sub2_events.get(index).map(|c| &c.event);
        assert_eq!(sub2_event, Some(&edge.node));
        sub2_events.get(index).unwrap().acknowledge().await?;
    }

    let sub2_events = sub2.read(executor).await?;
    assert!(sub2_events.is_empty());

    Ok(())
}

pub async fn subscribe_routing_key<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    let events = events
        .into_iter()
        .filter(|e| {
            e.aggregator_type == Calcul::name() && e.routing_key == Some("eu-west-3".to_owned())
        })
        .collect::<Vec<_>>();

    let events = evento::cursor::Reader::new(events)
        .forward(1000, None)
        .execute()?;

    let sub1 = evento::subscribe("sub1")
        .all()
        .routing_key("eu-west-3")
        .aggregator::<Calcul>();

    sub1.init(executor).await?;

    let sub1_events = sub1.read(executor).await?;
    for (index, edge) in events.edges.iter().enumerate() {
        let sub1_event = sub1_events.get(index).map(|c| &c.event);
        assert_eq!(sub1_event, Some(&edge.node));
        sub1_events.get(index).unwrap().acknowledge().await?;
    }

    let sub1_events = sub1.read(executor).await?;
    assert!(sub1_events.is_empty());

    Ok(())
}

pub async fn subscribe_default<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    let events = events
        .into_iter()
        .filter(|e| e.aggregator_type == Calcul::name() && e.routing_key.is_none())
        .collect::<Vec<_>>();

    let events = evento::cursor::Reader::new(events)
        .forward(1000, None)
        .execute()?;

    let sub1 = evento::subscribe("sub1").aggregator::<Calcul>();

    sub1.init(executor).await?;

    let sub1_events = sub1.read(executor).await?;
    for (index, edge) in events.edges.iter().enumerate() {
        let sub1_event = sub1_events.get(index).map(|c| &c.event);
        assert_eq!(sub1_event, Some(&edge.node));
        sub1_events.get(index).unwrap().acknowledge().await?;
    }

    let sub1_events = sub1.read(executor).await?;
    assert!(sub1_events.is_empty());

    Ok(())
}

pub async fn subscribe_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    let events = evento::cursor::Reader::new(events)
        .forward(1000, None)
        .execute()?;

    let sub1 = evento::subscribe("sub1")
        .all()
        .aggregator::<Calcul>()
        .aggregator::<MyCalcul>();

    sub1.init(executor).await?;

    let sub2 = evento::subscribe("sub2")
        .chunk_size(5)
        .all()
        .aggregator::<Calcul>()
        .aggregator::<MyCalcul>();

    sub2.init(executor).await?;

    let sub1_events = sub1.read(executor).await?;
    for (index, edge) in events.edges.iter().enumerate() {
        let sub1_event = sub1_events.get(index).map(|c| &c.event);
        assert_eq!(sub1_event, Some(&edge.node));
        sub1_events.get(index).unwrap().acknowledge().await?;
    }

    let sub1_events = sub1.read(executor).await?;
    assert!(sub1_events.is_empty());

    let sub2_events = sub2.read(executor).await?;
    for (index, edge) in events.edges.iter().take(5).enumerate() {
        let sub2_event = sub2_events.get(index).map(|c| &c.event);
        assert_eq!(sub2_event, Some(&edge.node));
        sub2_events.get(index).unwrap().acknowledge().await?;
    }

    let sub2_events = sub2.read(executor).await?;
    for (index, edge) in events.edges.iter().skip(5).enumerate() {
        let sub2_event = sub2_events.get(index).map(|c| &c.event);
        assert_eq!(sub2_event, Some(&edge.node));
        sub2_events.get(index).unwrap().acknowledge().await?;
    }

    let sub2_events = sub2.read(executor).await?;
    assert!(sub2_events.is_empty());

    Ok(())
}

pub async fn subscribe_routing_key_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    let events = events
        .into_iter()
        .filter(|e| e.routing_key == Some("eu-west-3".to_owned()))
        .collect::<Vec<_>>();

    let events = evento::cursor::Reader::new(events)
        .forward(1000, None)
        .execute()?;

    let sub1 = evento::subscribe("sub1")
        .all()
        .routing_key("eu-west-3")
        .aggregator::<Calcul>()
        .aggregator::<MyCalcul>();

    sub1.init(executor).await?;

    let sub1_events = sub1.read(executor).await?;
    for (index, edge) in events.edges.iter().enumerate() {
        let sub1_event = sub1_events.get(index).map(|c| &c.event);
        assert_eq!(sub1_event, Some(&edge.node));
        sub1_events.get(index).unwrap().acknowledge().await?;
    }

    let sub1_events = sub1.read(executor).await?;
    assert!(sub1_events.is_empty());

    Ok(())
}

pub async fn subscribe_default_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    events: Vec<Event>,
) -> anyhow::Result<()> {
    let events = events
        .into_iter()
        .filter(|e| e.routing_key.is_none())
        .collect::<Vec<_>>();

    let events = evento::cursor::Reader::new(events)
        .forward(1000, None)
        .execute()?;

    let sub1 = evento::subscribe("sub1")
        .aggregator::<Calcul>()
        .aggregator::<MyCalcul>();

    sub1.init(executor).await?;

    let sub1_events = sub1.read(executor).await?;
    for (index, edge) in events.edges.iter().enumerate() {
        let sub1_event = sub1_events.get(index).map(|c| &c.event);
        assert_eq!(sub1_event, Some(&edge.node));
        sub1_events.get(index).unwrap().acknowledge().await?;
    }

    let sub1_events = sub1.read(executor).await?;
    assert!(sub1_events.is_empty());

    Ok(())
}

#[derive(Debug, Encode, Decode, PartialEq, AggregatorName)]
struct Added {
    pub value: i16,
}

#[derive(Debug, Encode, Decode, PartialEq, AggregatorName)]
struct Subtracted {
    pub value: i16,
}

#[derive(Debug, Encode, Decode, PartialEq, AggregatorName)]
struct Multiplied {
    pub value: i16,
}

#[derive(Debug, Encode, Decode, PartialEq, AggregatorName)]
struct Divided {
    pub value: i16,
}

type CalculEvent<D> = EventDetails<D, bool>;

#[derive(Debug, Default, Encode, Decode, Clone)]
struct Calcul {
    pub value: i64,
}

#[evento::aggregator]
impl Calcul {
    async fn added(&mut self, event: CalculEvent<Added>) -> anyhow::Result<()> {
        self.value += event.data.value as i64;

        Ok(())
    }

    async fn subtracted(&mut self, event: CalculEvent<Subtracted>) -> anyhow::Result<()> {
        self.value -= event.data.value as i64;

        Ok(())
    }

    async fn multiplied(&mut self, event: CalculEvent<Multiplied>) -> anyhow::Result<()> {
        self.value *= event.data.value as i64;

        Ok(())
    }

    async fn divided(&mut self, event: CalculEvent<Divided>) -> anyhow::Result<()> {
        self.value /= event.data.value as i64;

        Ok(())
    }
}

#[derive(Debug, Default, Encode, Decode, Clone)]
struct MyCalcul {
    pub value: i64,
}

#[evento::aggregator]
impl MyCalcul {
    async fn added(&mut self, event: CalculEvent<Added>) -> anyhow::Result<()> {
        self.value += event.data.value as i64;

        Ok(())
    }

    async fn subtracted(&mut self, event: CalculEvent<Subtracted>) -> anyhow::Result<()> {
        self.value -= event.data.value as i64;

        Ok(())
    }

    async fn multiplied(&mut self, event: CalculEvent<Multiplied>) -> anyhow::Result<()> {
        self.value *= event.data.value as i64;

        Ok(())
    }

    async fn divided(&mut self, event: CalculEvent<Divided>) -> anyhow::Result<()> {
        self.value /= event.data.value as i64;

        Ok(())
    }
}
