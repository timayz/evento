//! A read over several filters is paged as the `UNION ALL` of one seek per
//! filter (#478): the pages are the ones a single `OR` of the filters gives,
//! forward and backward, and `latest_timestamp` is the greatest of the arms.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use evento_core::cursor::{Args, Value};
use evento_core::{Event, EventFilter, Executor as _, RoutingKey, TenantFilter};
use evento_turso::Turso;

/// A store whose commit clock advances one second per write, holding the
/// events of three types interleaved: `Page` (`Created`/`Edited`), `Comment`
/// (`Posted`), `Tag` (`Added`).
async fn store() -> anyhow::Result<Turso> {
    let tick = Arc::new(AtomicU64::new(1_700_000_000_000));
    let store = Turso::in_memory()
        .await?
        .with_clock(move || tick.fetch_add(1_000, Ordering::Relaxed));
    for i in 0..40u16 {
        let page = format!("p{i}");
        store
            .write(vec![
                Event {
                    id: ulid::Ulid::generate(),
                    aggregate_type: "demo/Page".into(),
                    aggregate_id: page.clone(),
                    name: "Created".into(),
                    version: 1,
                    ..Default::default()
                },
                Event {
                    id: ulid::Ulid::generate(),
                    aggregate_type: "demo/Comment".into(),
                    aggregate_id: format!("c{i}"),
                    name: "Posted".into(),
                    version: 1,
                    ..Default::default()
                },
            ])
            .await?;
        store
            .write(vec![
                Event {
                    id: ulid::Ulid::generate(),
                    aggregate_type: "demo/Page".into(),
                    aggregate_id: page,
                    name: "Edited".into(),
                    version: 2,
                    ..Default::default()
                },
                Event {
                    id: ulid::Ulid::generate(),
                    aggregate_type: "demo/Tag".into(),
                    aggregate_id: format!("t{i}"),
                    name: "Added".into(),
                    version: 1,
                    ..Default::default()
                },
            ])
            .await?;
    }
    Ok(store)
}

/// Every page of `filters` in `direction`, as event ids in page order.
async fn pages(
    store: &Turso,
    filters: Option<Arc<[EventFilter]>>,
    size: u16,
    backward: bool,
) -> anyhow::Result<Vec<Vec<String>>> {
    let mut pages = Vec::new();
    let mut cursor: Option<Value> = None;
    loop {
        let args = if backward {
            Args::backward(size, cursor.clone())
        } else {
            Args::forward(size, cursor.clone())
        };
        let page = store
            .read(
                filters.clone(),
                Some(RoutingKey::All),
                TenantFilter::All,
                args,
                None,
            )
            .await?;
        if page.edges.is_empty() {
            break;
        }
        pages.push(page.edges.iter().map(|e| e.node.id.to_string()).collect());
        let (more, next) = if backward {
            (
                page.page_info.has_previous_page,
                page.page_info.start_cursor,
            )
        } else {
            (page.page_info.has_next_page, page.page_info.end_cursor)
        };
        if !more {
            break;
        }
        cursor = next;
        assert!(pages.len() < 1000, "runaway paging");
    }
    Ok(pages)
}

#[tokio::test]
async fn several_filters_page_like_every_event() -> anyhow::Result<()> {
    let store = store().await?;
    // Three arms of three types: together they are every event.
    let arms: Arc<[EventFilter]> = vec![
        EventFilter::by_event_raw("demo/Page", "Created"),
        EventFilter::by_event_raw("demo/Page", "Edited"),
        EventFilter::by_event_raw("demo/Comment", "Posted"),
        EventFilter::by_type_raw("demo/Tag"),
    ]
    .into();
    for size in [1u16, 3, 7, 160, 500] {
        for backward in [false, true] {
            let expected = pages(&store, None, size, backward).await?;
            let actual = pages(&store, Some(arms.clone()), size, backward).await?;
            assert_eq!(actual, expected, "size {size} backward {backward}");
            assert_eq!(actual.concat().len(), 160);
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_subset_of_arms_pages_in_order_without_duplicates() -> anyhow::Result<()> {
    let store = store().await?;
    let arms: Arc<[EventFilter]> = vec![
        EventFilter::by_event_raw("demo/Page", "Edited"),
        EventFilter::by_event_raw("demo/Comment", "Posted"),
        // Never written: an arm without rows.
        EventFilter::by_event_raw("demo/Page", "Deleted"),
    ]
    .into();
    let all: Vec<String> = pages(&store, None, 500, false).await?.concat();
    let ids: Vec<String> = pages(&store, Some(arms.clone()), 5, false).await?.concat();
    assert_eq!(ids.len(), 80);
    // In store order, each once.
    let expected: Vec<String> = all.iter().filter(|id| ids.contains(id)).cloned().collect();
    assert_eq!(ids, expected);
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 80);

    // Backward pages — last page first, each in store order — give the same
    // events.
    let mut back = pages(&store, Some(arms), 5, true).await?;
    back.reverse();
    assert_eq!(back.concat(), ids);
    Ok(())
}

#[tokio::test]
async fn latest_timestamp_is_the_greatest_arm() -> anyhow::Result<()> {
    let store = store().await?;
    let latest = |filters: Vec<EventFilter>| {
        let store = &store;
        async move {
            store
                .latest_timestamp(
                    Some(filters.into()),
                    Some(RoutingKey::All),
                    TenantFilter::All,
                )
                .await
        }
    };
    let created = latest(vec![EventFilter::by_event_raw("demo/Page", "Created")]).await?;
    let tag = latest(vec![EventFilter::by_type_raw("demo/Tag")]).await?;
    assert!(tag > created, "{tag} > {created}");
    assert_eq!(
        latest(vec![
            EventFilter::by_event_raw("demo/Page", "Created"),
            EventFilter::by_type_raw("demo/Tag"),
            EventFilter::by_event_raw("demo/Page", "Deleted"),
        ])
        .await?,
        tag
    );
    assert_eq!(
        latest(vec![
            EventFilter::by_event_raw("demo/Page", "Deleted"),
            EventFilter::by_event_raw("demo/Comment", "Removed"),
        ])
        .await?,
        0
    );
    Ok(())
}
