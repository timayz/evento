use chrono::{DateTime, Utc};
use evento_query::{Cursor, CursorError, PageInfo, Query, QueryOrder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgArguments, Postgres, QueryBuilder};
use uuid::Uuid;

mod common;

use crate::common::create_pg_store;

#[derive(Clone, Serialize, Deserialize, Debug, sqlx::FromRow, Default)]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub aggregate_id: String,
    pub version: i32,
    pub data: Value,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl Cursor for Event {
    fn keys() -> Vec<&'static str> {
        vec!["created_at", "version", "id"]
    }

    fn serialize(&self) -> Vec<String> {
        vec![
            Self::serialize_utc(self.created_at),
            self.version.to_string(),
            self.id.to_string(),
        ]
    }

    fn deserialize(values: Vec<&str>) -> Result<Self, CursorError> {
        let mut values = values.iter();
        let created_at = Self::deserialize_as_utc("created_at", values.next())?;
        let version = Self::deserialize_as("version", values.next())?;
        let id = Self::deserialize_as("id", values.next())?;

        Ok(Event {
            id,
            version,
            created_at,
            ..Default::default()
        })
    }

    fn bind<'q, O>(
        self,
        query: sqlx::query::QueryAs<Postgres, O, PgArguments>,
    ) -> sqlx::query::QueryAs<Postgres, O, PgArguments>
    where
        O: for<'r> sqlx::FromRow<'r, <sqlx::Postgres as sqlx::Database>::Row>,
        O: 'q + std::marker::Send,
        O: 'q + Unpin,
        O: 'q + Cursor,
    {
        query.bind(self.created_at).bind(self.version).bind(self.id)
    }
}

#[tokio::test]
async fn query_cursor_serialization() {
    assert_eq!(
        Event::to_pg_filter_opts(&QueryOrder::Asc, true, None, None),
        "created_at < $1 OR (created_at = $1 AND (version < $2 OR (version = $2 AND id < $3)))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(&QueryOrder::Asc, false, None, None),
        "created_at > $1 OR (created_at = $1 AND (version > $2 OR (version = $2 AND id > $3)))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(
            &QueryOrder::Asc,
            false,
            Some(vec!["created_at", "version", "id"]),
            Some(2)
        ),
        "(created_at > $2 OR (created_at = $2 AND (version > $3 OR (version = $3 AND id > $4))))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(
            &QueryOrder::Asc,
            true,
            Some(vec!["created_at", "version", "id"]),
            Some(2)
        ),
        "(created_at < $2 OR (created_at = $2 AND (version < $3 OR (version = $3 AND id < $4))))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(&QueryOrder::Desc, true, None, None),
        "created_at > $1 OR (created_at = $1 AND (version > $2 OR (version = $2 AND id > $3)))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(&QueryOrder::Desc, false, None, None),
        "created_at < $1 OR (created_at = $1 AND (version < $2 OR (version = $2 AND id < $3)))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(
            &QueryOrder::Desc,
            false,
            Some(vec!["created_at", "version", "id"]),
            Some(2)
        ),
        "(created_at < $2 OR (created_at = $2 AND (version < $3 OR (version = $3 AND id < $4))))"
    );

    assert_eq!(
        Event::to_pg_filter_opts(
            &QueryOrder::Desc,
            true,
            Some(vec!["created_at", "version", "id"]),
            Some(2)
        ),
        "(created_at > $2 OR (created_at = $2 AND (version > $3 OR (version = $3 AND id > $4))))"
    );

    assert_eq!(
        Event::to_pg_order(&QueryOrder::Asc, true),
        "created_at DESC, version DESC, id DESC"
    );

    assert_eq!(
        Event::to_pg_order(&QueryOrder::Asc, false),
        "created_at ASC, version ASC, id ASC"
    );

    assert_eq!(
        Event::to_pg_order(&QueryOrder::Desc, true),
        "created_at ASC, version ASC, id ASC"
    );

    assert_eq!(
        Event::to_pg_order(&QueryOrder::Desc, false),
        "created_at DESC, version DESC, id DESC"
    );
}

#[tokio::test]
async fn query_as() {
    let (_, db) = create_pg_store("query_as", false).await;

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO evento_events (id, name, aggregate_id, version, data, metadata, created_at) ",
    );

    let events = vec![
        Event {
            id: Uuid::parse_str("fb1de7a6-996f-48c6-9973-f434852ad843").unwrap(),
            name: "event-1".to_owned(),
            version: 0,
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00.010Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("29eab018-54bc-4edb-9f0e-c63c975b1b36").unwrap(),
            name: "event-2".to_owned(),
            version: 1,
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00.010Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("6a45fd71-cc32-4eeb-823e-e8ef08ecd004").unwrap(),
            name: "event-1".to_owned(),
            version: 0,
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00.010Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-2".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("7f2a35d7-6e20-40bf-9f35-91cb7ca7e8d6").unwrap(),
            name: "event-4".to_owned(),
            version: 3,
            created_at: DateTime::parse_from_rfc3339("2020-01-02T00:00:00.956Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("0035b208-34fb-4548-ba20-cd9dcbe717fa").unwrap(),
            name: "event-3".to_owned(),
            version: 2,
            created_at: DateTime::parse_from_rfc3339("2020-01-02T00:00:00.956Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("296e887e-86dc-46a6-a289-f3a19de12a53").unwrap(),
            name: "event-5".to_owned(),
            version: 4,
            created_at: DateTime::parse_from_rfc3339("2020-01-03T00:00:00.453Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("a92b8098-2c05-4cd3-a633-10f2de26b1b9").unwrap(),
            name: "event-2".to_owned(),
            version: 1,
            created_at: DateTime::parse_from_rfc3339("2020-01-04T00:00:00.658Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-2".to_owned(),
            ..Default::default()
        },
    ];

    query_builder.push_values(events.clone(), |mut b, event| {
        b.push_bind(event.id)
            .push_bind(event.name.to_owned())
            .push_bind(event.aggregate_id.to_owned())
            .push_bind(event.version)
            .push_bind(event.data.clone())
            .push_bind(event.metadata.clone())
            .push_bind(event.created_at);
    });

    query_builder.build().execute(&db).await.unwrap();

    let res = Query::<Event>::new("select * from evento_events")
        .build(Default::default())
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: Some(res.edges[6].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 7);
    assert_eq!(res.edges[0].node.id, events[2].id);
    assert_eq!(res.edges[1].node.id, events[0].id);
    assert_eq!(res.edges[2].node.id, events[1].id);
    assert_eq!(res.edges[3].node.id, events[4].id);
    assert_eq!(res.edges[4].node.id, events[3].id);
    assert_eq!(res.edges[5].node.id, events[5].id);
    assert_eq!(res.edges[6].node.id, events[6].id);

    let res = Query::<Event>::new("select * from evento_events")
        .forward(2, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(res.edges[1].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[2].id);
    assert_eq!(res.edges[1].node.id, events[0].id);

    let res = Query::<Event>::new("select * from evento_events")
        .forward(4, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(res.edges[3].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 4);
    assert_eq!(res.edges[0].node.id, events[2].id);
    assert_eq!(res.edges[1].node.id, events[0].id);
    assert_eq!(res.edges[2].node.id, events[1].id);
    assert_eq!(res.edges[3].node.id, events[4].id);

    let res = Query::<Event>::new("select * from evento_events")
        .forward(2, Some(events[1].to_cursor()))
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(res.edges[1].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[4].id);
    assert_eq!(res.edges[1].node.id, events[3].id);

    let res = Query::<Event>::new("select * from evento_events")
        .forward(1, Some(events[6].to_cursor()))
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: None,
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 0);

    let res = Query::<Event>::new("select * from evento_events")
        .backward(2, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(res.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[5].id);
    assert_eq!(res.edges[1].node.id, events[6].id);

    let res = Query::<Event>::new("select * from evento_events")
        .backward(4, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(res.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 4);
    assert_eq!(res.edges[0].node.id, events[4].id);
    assert_eq!(res.edges[1].node.id, events[3].id);
    assert_eq!(res.edges[2].node.id, events[5].id);
    assert_eq!(res.edges[3].node.id, events[6].id);

    let res = Query::<Event>::new("select * from evento_events")
        .backward(3, Some(events[1].to_cursor()))
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: Some(res.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[2].id);
    assert_eq!(res.edges[1].node.id, events[0].id);
}

#[tokio::test]
async fn query_as_desc() {
    let (_, db) = create_pg_store("query_as_desc", false).await;

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO evento_events (id, name, aggregate_id, version, data, metadata, created_at) ",
    );

    let events = vec![
        Event {
            id: Uuid::parse_str("fb1de7a6-996f-48c6-9973-f434852ad843").unwrap(),
            name: "event-1".to_owned(),
            version: 0,
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00.010Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("29eab018-54bc-4edb-9f0e-c63c975b1b36").unwrap(),
            name: "event-2".to_owned(),
            version: 1,
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00.010Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("6a45fd71-cc32-4eeb-823e-e8ef08ecd004").unwrap(),
            name: "event-1".to_owned(),
            version: 0,
            created_at: DateTime::parse_from_rfc3339("2020-01-01T00:00:00.010Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-2".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("7f2a35d7-6e20-40bf-9f35-91cb7ca7e8d6").unwrap(),
            name: "event-4".to_owned(),
            version: 3,
            created_at: DateTime::parse_from_rfc3339("2020-01-02T00:00:00.956Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("0035b208-34fb-4548-ba20-cd9dcbe717fa").unwrap(),
            name: "event-3".to_owned(),
            version: 2,
            created_at: DateTime::parse_from_rfc3339("2020-01-02T00:00:00.956Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("296e887e-86dc-46a6-a289-f3a19de12a53").unwrap(),
            name: "event-5".to_owned(),
            version: 4,
            created_at: DateTime::parse_from_rfc3339("2020-01-03T00:00:00.453Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-1".to_owned(),
            ..Default::default()
        },
        Event {
            id: Uuid::parse_str("a92b8098-2c05-4cd3-a633-10f2de26b1b9").unwrap(),
            name: "event-2".to_owned(),
            version: 1,
            created_at: DateTime::parse_from_rfc3339("2020-01-04T00:00:00.658Z")
                .map(DateTime::<Utc>::from)
                .unwrap(),
            aggregate_id: "user-2".to_owned(),
            ..Default::default()
        },
    ];

    query_builder.push_values(events.clone(), |mut b, event| {
        b.push_bind(event.id)
            .push_bind(event.name.to_owned())
            .push_bind(event.aggregate_id.to_owned())
            .push_bind(event.version)
            .push_bind(event.data.clone())
            .push_bind(event.metadata.clone())
            .push_bind(event.created_at);
    });

    query_builder.build().execute(&db).await.unwrap();

    let res = Query::<Event>::new("select * from evento_events")
        .desc()
        .build(Default::default())
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: Some(res.edges[6].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 7);
    assert_eq!(res.edges[0].node.id, events[6].id);
    assert_eq!(res.edges[1].node.id, events[5].id);
    assert_eq!(res.edges[2].node.id, events[3].id);
    assert_eq!(res.edges[3].node.id, events[4].id);
    assert_eq!(res.edges[4].node.id, events[1].id);
    assert_eq!(res.edges[5].node.id, events[0].id);
    assert_eq!(res.edges[6].node.id, events[2].id);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .forward(2, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(res.edges[1].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[6].id);
    assert_eq!(res.edges[1].node.id, events[5].id);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .forward(4, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(res.edges[3].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 4);
    assert_eq!(res.edges[0].node.id, events[6].id);
    assert_eq!(res.edges[1].node.id, events[5].id);
    assert_eq!(res.edges[2].node.id, events[3].id);
    assert_eq!(res.edges[3].node.id, events[4].id);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .forward(2, Some(events[4].to_cursor()))
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: Some(res.edges[1].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[1].id);
    assert_eq!(res.edges[1].node.id, events[0].id);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .forward(1, Some(events[2].to_cursor()))
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: None,
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 0);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .backward(2, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(res.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[0].id);
    assert_eq!(res.edges[1].node.id, events[2].id);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .backward(4, None::<String>)
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: Some(res.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 4);
    assert_eq!(res.edges[0].node.id, events[4].id);
    assert_eq!(res.edges[1].node.id, events[1].id);
    assert_eq!(res.edges[2].node.id, events[0].id);
    assert_eq!(res.edges[3].node.id, events[2].id);

    let res = Query::<Event>::new("select * from evento_events")
        .order(QueryOrder::Desc)
        .backward(3, Some(events[3].to_cursor()))
        .fetch_all(&db)
        .await
        .unwrap();

    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: Some(res.edges[0].cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(res.edges.len(), 2);
    assert_eq!(res.edges[0].node.id, events[6].id);
    assert_eq!(res.edges[1].node.id, events[5].id);
}
