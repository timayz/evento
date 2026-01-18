use evento_core::{
    cursor::{self, Args, Cursor, Edge, Order, PageInfo, ReadResult, Reader},
    Event,
};
use rand::{seq::IndexedRandom, Rng};
use std::collections::HashMap;
use ulid::Ulid;

#[test]
fn forward() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone()).execute()?;

    assert_eq!(res.edges.len(), events.len());
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(events[3], res.edges[0].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001
    assert_eq!(events[2], res.edges[1].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[7], res.edges[2].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    assert_eq!(events[5], res.edges[3].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[9], res.edges[4].node); // 01K14CAPNNDMVZH4RCDCW3WWPS 5 1753567482
    assert_eq!(events[1], res.edges[5].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482
    assert_eq!(events[0], res.edges[6].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658
    assert_eq!(events[6], res.edges[7].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[8], res.edges[8].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[4], res.edges[9].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690

    Ok(())
}

#[test]
fn forward_custom_limit() -> anyhow::Result<()> {
    let events = get_events();
    let args = Args::forward(10, None).limit(5);
    let res = Reader::new(events.clone()).args(args).execute()?;

    assert_eq!(res.edges.len(), 5);

    assert_eq!(events[3], res.edges[0].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001
    assert_eq!(events[2], res.edges[1].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[7], res.edges[2].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    assert_eq!(events[5], res.edges[3].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[9], res.edges[4].node); // 01K14CAPNNDMVZH4RCDCW3WWPS 5 1753567482

    Ok(())
}

#[test]
fn forward_3() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone()).forward(3, None).execute()?;

    assert_eq!(res.edges.len(), 3);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[3], res.edges[0].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001
    assert_eq!(events[2], res.edges[1].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[7], res.edges[2].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    Ok(())
}

#[test]
fn forward_2_after_3() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(2, events[3].serialize_cursor().ok())
        .execute()?;

    assert_eq!(res.edges.len(), 2);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[2], res.edges[0].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[7], res.edges[1].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361

    Ok(())
}

#[test]
fn forward_2_after_9() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(2, events[9].serialize_cursor().ok())
        .execute()?;

    assert_eq!(res.edges.len(), 2);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[1], res.edges[0].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482
    assert_eq!(events[0], res.edges[1].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658

    Ok(())
}

#[test]
fn forward_3_after_8() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(3, events[8].serialize_cursor().ok())
        .execute()?;

    assert_eq!(res.edges.len(), 1);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(events[4], res.edges[0].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690

    Ok(())
}

#[test]
fn forward_desc() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone()).desc().execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), events.len());
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(events[4], res.edges[0].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690
    assert_eq!(events[8], res.edges[1].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[6], res.edges[2].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[0], res.edges[3].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658
    assert_eq!(events[1], res.edges[4].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482
    assert_eq!(events[9], res.edges[5].node); // 01K14CAPNNDMVZH4RCDCW3WWPS 5 1753567482
    assert_eq!(events[5], res.edges[6].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[7], res.edges[7].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    assert_eq!(events[2], res.edges[8].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[3], res.edges[9].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001

    Ok(())
}

#[test]
fn forward_desc_3() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(3, None)
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 3);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(events[4], res.edges[0].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690
    assert_eq!(events[8], res.edges[1].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[6], res.edges[2].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    Ok(())
}

#[test]
fn forward_desc_2_after_3() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(2, events[3].serialize_cursor().ok())
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 0);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: false,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    Ok(())
}

#[test]
fn forward_desc_2_after_9() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(2, events[9].serialize_cursor().ok())
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 2);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(events[5], res.edges[0].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[7], res.edges[1].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361

    Ok(())
}

#[test]
fn forward_desc_3_after_8() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .forward(3, events[8].serialize_cursor().ok())
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 3);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_next_page: true,
            end_cursor: res.edges.last().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    assert_eq!(events[6], res.edges[0].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[0], res.edges[1].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658
    assert_eq!(events[1], res.edges[2].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482

    Ok(())
}

#[test]
fn backward_20() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone()).backward(20, None).execute()?;

    assert_eq!(res.edges.len(), events.len());
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[3], res.edges[0].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001
    assert_eq!(events[2], res.edges[1].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[7], res.edges[2].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    assert_eq!(events[5], res.edges[3].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[9], res.edges[4].node); // 01K14CAPNNDMVZH4RCDCW3WWPS 5 1753567482
    assert_eq!(events[1], res.edges[5].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482
    assert_eq!(events[0], res.edges[6].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658
    assert_eq!(events[6], res.edges[7].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[8], res.edges[8].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[4], res.edges[9].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690

    Ok(())
}

#[test]
fn backward_custom_litmit() -> anyhow::Result<()> {
    let events = get_events();
    let args = Args::backward(20, None).limit(3);
    let res = Reader::new(events.clone()).args(args).execute()?;

    assert_eq!(res.edges.len(), 3);
    assert_eq!(events[6], res.edges[0].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[8], res.edges[1].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[4], res.edges[2].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690

    Ok(())
}

#[test]
fn backward_3() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone()).backward(3, None).execute()?;

    assert_eq!(res.edges.len(), 3);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[6], res.edges[0].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[8], res.edges[1].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[4], res.edges[2].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690

    Ok(())
}

#[test]
fn backward_2_after_4() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(2, events[4].serialize_cursor().ok())
        .execute()?;

    assert_eq!(res.edges.len(), 2);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[6], res.edges[0].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[8], res.edges[1].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680

    Ok(())
}

#[test]
fn backward_2_after_2() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(2, events[2].serialize_cursor().ok())
        .execute()?;

    assert_eq!(res.edges.len(), 1);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[3], res.edges[0].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001

    Ok(())
}

#[test]
fn backward_3_after_8() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(3, events[8].serialize_cursor().ok())
        .execute()?;

    assert_eq!(res.edges.len(), 3);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[1], res.edges[0].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482
    assert_eq!(events[0], res.edges[1].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658
    assert_eq!(events[6], res.edges[2].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670

    Ok(())
}

#[test]
fn backward_desc_20() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(20, None)
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), events.len());
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[4], res.edges[0].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690
    assert_eq!(events[8], res.edges[1].node); // 01K14CFW3GNN366VM32YEGBT13 88 1753567680
    assert_eq!(events[6], res.edges[2].node); // 01K14CFJR0GZ18SD2F4J5DXEDD 19 1753567670
    assert_eq!(events[0], res.edges[3].node); // 01K14CFBVQ9DA4240RZ0V89PQS 23 1753567658
    assert_eq!(events[1], res.edges[4].node); // 01K14CFR387QQCNT4GGGVBSAJV 8 1753567482
    assert_eq!(events[9], res.edges[5].node); // 01K14CAPNNDMVZH4RCDCW3WWPS 5 1753567482
    assert_eq!(events[5], res.edges[6].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[7], res.edges[7].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    assert_eq!(events[2], res.edges[8].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[3], res.edges[9].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001
    Ok(())
}

#[test]
fn backward_desc_3() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(3, None)
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 3);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[7], res.edges[0].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    assert_eq!(events[2], res.edges[1].node); // 01K14C49SHZGXFVAKJJTJYQS70 2 1753567361
    assert_eq!(events[3], res.edges[2].node); // 01K14C428V3TNC2K7DCFEJ4HV4 1 1753567001

    Ok(())
}

#[test]
fn backward_desc_2_after_4() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(2, events[4].serialize_cursor().ok())
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 0);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );

    Ok(())
}

#[test]
fn backward_desc_2_after_2() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(2, events[2].serialize_cursor().ok())
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 2);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: true,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[5], res.edges[0].node); // 01K14CAERY65HK16EWV7B0X9V6 5 1753567482
    assert_eq!(events[7], res.edges[1].node); // 01K14CA43DENK9KM08REE27EE7 3 1753567361
    Ok(())
}

#[test]
fn backward_desc_3_after_8() -> anyhow::Result<()> {
    let events = get_events();
    let res = Reader::new(events.clone())
        .backward(3, events[8].serialize_cursor().ok())
        .desc()
        .execute()?;

    display_expected_assert(&events, &res.edges);

    assert_eq!(res.edges.len(), 1);
    assert_eq!(
        res.page_info,
        PageInfo {
            has_previous_page: false,
            start_cursor: res.edges.first().map(|e| e.cursor.to_owned()),
            ..Default::default()
        }
    );
    assert_eq!(events[4], res.edges[0].node); // 01K14CFZJ0T1KJHBK5084C30BK 52 1753567690
    Ok(())
}

fn display_expected_assert(events: &[Event], edges: &[Edge<Event>]) {
    for (i, edge) in edges.iter().enumerate() {
        let pos = events.iter().position(|e| e.id == edge.node.id).unwrap();
        println!(
            "assert_eq!(events[{}],res.edges[{}].node); // {} {} {}",
            pos,
            i,
            edge.node.id.to_string(),
            edge.node.version,
            edge.node.timestamp
        );
    }
}

pub fn assert_read_result(
    args: Args,
    order: Order,
    data: Vec<Event>,
    result: ReadResult<Event>,
) -> anyhow::Result<()> {
    let data = cursor::Reader::new(data)
        .args(args)
        .order(order)
        .execute()?;

    assert_eq!(result.page_info, data.page_info);
    assert_eq!(result.edges, data.edges);

    Ok(())
}

pub fn get_data() -> Vec<Event> {
    let aggregator_ids = [
        Ulid::new().to_string(),
        Ulid::new().to_string(),
        Ulid::new().to_string(),
        Ulid::new().to_string(),
        Ulid::new().to_string(),
    ];

    let aggregator_types = ["evento/Calcul", "evento/MyCalcul"];

    let routing_keys = [
        Some("us-east-1".to_owned()),
        Some("eu-west-3".to_owned()),
        None,
    ];

    let timestamps: Vec<u16> = vec![rand::random(), rand::random(), rand::random()];
    let mut versions: HashMap<String, u16> = HashMap::new();
    let mut data = vec![];

    for _ in 0..10 {
        let mut rng = rand::rng();
        let aggregator_id = aggregator_ids
            .choose(&mut rng)
            .cloned()
            .unwrap_or_else(|| Ulid::new().to_string());

        let routing_key = routing_keys.choose(&mut rng).cloned().unwrap_or(None);
        let aggregator_type = aggregator_types
            .choose(&mut rng)
            .cloned()
            .unwrap_or("Calcul");
        let version = versions.entry(aggregator_id.to_owned()).or_default();
        let timestamp = if rng.random_range(0..100) < 20 {
            timestamps.choose(&mut rng).cloned()
        } else {
            None
        }
        .unwrap_or_else(|| rng.random()) as u64;

        let event = Event {
            id: Ulid::new(),
            name: "MessageSent".to_owned(),
            aggregator_id,
            aggregator_type: aggregator_type.to_owned(),
            version: *version,
            routing_key,
            timestamp: timestamp as u64,
            timestamp_subsec: 0,
            data: Default::default(),
            metadata: Default::default(),
        };

        data.push(event);

        *version += 1;
    }

    data
}

fn get_events() -> Vec<Event> {
    vec![
        create_event("01K14CFBVQ9DA4240RZ0V89PQS", 23, 1753567658),
        create_event("01K14CFR387QQCNT4GGGVBSAJV", 8, 1753567482),
        create_event("01K14C49SHZGXFVAKJJTJYQS70", 2, 1753567361),
        create_event("01K14C428V3TNC2K7DCFEJ4HV4", 1, 1753567001),
        create_event("01K14CFZJ0T1KJHBK5084C30BK", 52, 1753567690),
        create_event("01K14CAERY65HK16EWV7B0X9V6", 5, 1753567482),
        create_event("01K14CFJR0GZ18SD2F4J5DXEDD", 19, 1753567670),
        create_event("01K14CA43DENK9KM08REE27EE7", 3, 1753567361),
        create_event("01K14CFW3GNN366VM32YEGBT13", 88, 1753567680),
        create_event("01K14CAPNNDMVZH4RCDCW3WWPS", 5, 1753567482),
    ]
}

fn create_event(id: &str, version: u16, timestamp: u32) -> Event {
    Event {
        id: Ulid::from_string(id).unwrap(),
        name: "MessageSent".to_owned(),
        aggregator_id: Ulid::new().to_string(),
        aggregator_type: "Message".to_owned(),
        version,
        routing_key: None,
        timestamp: timestamp as u64,
        timestamp_subsec: 0,
        data: Default::default(),
        metadata: Default::default(),
    }
}
