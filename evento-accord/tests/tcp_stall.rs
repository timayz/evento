//! The framed-TCP writer never waits on the network unbounded. A peer that
//! stops reading (partitioned or paused, so the kernel's buffers fill and the
//! flush blocks on retransmits) is abandoned after the write timeout and
//! redialled; a peer that was unreachable is dialled again within its backoff
//! once it comes up. Before, a stalled peer pinned its writer — and shed every
//! frame meant for it — for minutes, even after the network healed.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use evento_accord::{Message, MessageSink, NodeId, TcpTransport, Timestamp, TxnId};
use evento_core::Event;
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn applied(n: u64) -> Message {
    Message::Applied {
        txn: TxnId(Timestamp {
            micros: n,
            logical: 0,
            node: NodeId(0),
        }),
    }
}

/// A frame of about `bytes`, to fill the socket buffers quickly.
fn bulky(bytes: usize) -> Message {
    Message::SyncData {
        id: 0,
        watermark: Timestamp::MIN,
        coverage: Timestamp::MIN,
        snapshot: vec![Event {
            aggregate_type: "test/Account".into(),
            aggregate_id: "a".into(),
            version: 1,
            name: "Bumped".into(),
            data: vec![7; bytes],
            ..Default::default()
        }],
        commands: Vec::new(),
    }
}

/// A peer whose first connection is accepted and then never read (a black
/// hole once the buffers fill); every later connection is read and each
/// frame reported to `frames` as `(connection index, length)`.
async fn stalling_peer(listener: TcpListener, frames: mpsc::UnboundedSender<(usize, usize)>) {
    let mut held = Vec::new();
    for index in 0.. {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        if index == 0 {
            held.push(stream);
            continue;
        }
        let frames = frames.clone();
        tokio::spawn(async move {
            let mut framed = Framed::new(
                stream,
                LengthDelimitedCodec::builder()
                    .max_frame_length(evento_accord::tcp::MAX_FRAME_LENGTH)
                    .new_codec(),
            );
            while let Some(Ok(bytes)) = framed.next().await {
                let _ = frames.send((index, bytes.len()));
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_stops_reading_is_redialled_after_the_write_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (frames_tx, mut frames) = mpsc::unbounded_channel();
    let peer = tokio::spawn(stalling_peer(listener, frames_tx));

    let transport = TcpTransport::new(NodeId(0), HashMap::from([(NodeId(1), addr)]));

    // Far more than loopback's send + receive buffers hold, so the writer's
    // flush on the unread connection blocks.
    for _ in 0..12 {
        transport.send(NodeId(1), bulky(4 << 20)).await.unwrap();
    }

    // Keep sending small frames: once the stalled write times out, the writer
    // redials and they reach the peer over a fresh connection.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut n = 0;
    let redialled = loop {
        assert!(
            Instant::now() < deadline,
            "no frame reached the peer over a fresh connection: the writer is still stalled"
        );
        n += 1;
        transport.send(NodeId(1), applied(n)).await.unwrap();
        if let Ok(Some((index, _))) =
            tokio::time::timeout(Duration::from_millis(200), frames.recv()).await
        {
            break index;
        }
    };
    assert!(redialled >= 1);
    peer.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_peer_is_reached_once_it_comes_up() {
    // Reserve a port, then free it: dials are refused until it is bound again.
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let transport = TcpTransport::new(NodeId(0), HashMap::from([(NodeId(1), addr)]));

    // Several refused dials, so the backoff has grown to its cap.
    for n in 0..20 {
        transport.send(NodeId(1), applied(n)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The peer comes up; it must be reached within about one capped backoff.
    let listener = TcpListener::bind(addr).await.unwrap();
    let (frames_tx, mut frames) = mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
        while let Some(Ok(bytes)) = framed.next().await {
            let _ = frames_tx.send(bytes.len());
        }
    });
    let up = Instant::now();
    let mut n = 100;
    loop {
        assert!(
            up.elapsed() < Duration::from_secs(3),
            "the restarted peer was not redialled within its backoff"
        );
        n += 1;
        transport.send(NodeId(1), applied(n)).await.unwrap();
        if let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(50), frames.recv()).await {
            break;
        }
    }
    peer.abort();
}
