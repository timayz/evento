//! The framed-TCP writer survives a peer restart: a write that fails on a stale
//! connection is retried once over a fresh one, so the restarted peer's first
//! round does not lose the frame (before, that frame was dropped and only the
//! *next* send reconnected).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use evento_accord::{Message, MessageSink, NodeId, TcpTransport, Timestamp, TxnId};
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

/// A raw peer: accepts one connection at a time and forwards every frame's
/// length to `frames`. `close` makes it drop the current connection (a crash /
/// restart) and go back to accepting.
async fn raw_peer(
    listener: TcpListener,
    frames: mpsc::UnboundedSender<usize>,
    mut close: mpsc::Receiver<()>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
        loop {
            tokio::select! {
                item = framed.next() => match item {
                    Some(Ok(bytes)) => { let _ = frames.send(bytes.len()); }
                    _ => break,
                },
                _ = close.recv() => break, // drop the connection
            }
        }
        drop(framed);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_on_a_stale_connection_is_retried_over_a_fresh_one() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (frames_tx, mut frames) = mpsc::unbounded_channel();
    let (close_tx, close_rx) = mpsc::channel(1);
    let peer = tokio::spawn(raw_peer(listener, frames_tx, close_rx));

    let transport = Arc::new(TcpTransport::new(
        NodeId(0),
        HashMap::from([(NodeId(1), addr)]),
    ));

    // A first frame establishes the connection.
    transport.send(NodeId(1), applied(1)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), frames.recv())
        .await
        .expect("first frame delivered")
        .unwrap();

    // The peer "restarts": it drops the connection and accepts again.
    close_tx.send(()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The writer still holds the stale connection. The very first write after a
    // graceful close can succeed at the kernel level (it is what provokes the
    // RST), so one frame may legitimately be lost; from then on every write
    // fails fast and must be retried over a fresh connection — so of the frames
    // sent now, at most one may go missing.
    const SENT: u64 = 6;
    for n in 2..2 + SENT {
        transport.send(NodeId(1), applied(n)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut received = 0u64;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(500), frames.recv()).await {
        received += 1;
    }
    assert!(
        received >= SENT - 1,
        "expected at least {} of {SENT} frames after the peer restart, got {received}",
        SENT - 1
    );
    peer.abort();
}
