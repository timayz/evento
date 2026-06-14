//! Production transport: length-delimited framed TCP behind the
//! [`MessageSink`] trait, for a real multi-process cluster.
//!
//! Each frame is a bitcode-serialised [`Frame`] (sender id + [`Message`]) over a
//! [`LengthDelimitedCodec`] stream. Outbound delivery is per-peer and lazy: the
//! first message to a peer spawns a writer task that connects on demand and
//! reconnects after a drop. Delivery is best-effort and unacknowledged — exactly
//! what the consensus layer assumes, since quorums and recovery tolerate loss.
//!
//! Membership is static: construct a [`TcpTransport`] with a fixed
//! `NodeId → SocketAddr` map. Inbound frames are decoded and forwarded to a
//! node's inbox by [`serve`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::api::MessageSink;
use crate::clock::NodeId;
use crate::message::Message;
use crate::transport::Envelope;

/// A wire frame: the originating node plus the message. The sender id travels in
/// the frame so the receiver can reply without a separate handshake.
#[derive(Serialize, Deserialize)]
struct Frame {
    from: NodeId,
    message: Message,
}

fn encode(frame: &Frame) -> anyhow::Result<Vec<u8>> {
    Ok(bitcode::serialize(frame)?)
}

fn decode(bytes: &[u8]) -> anyhow::Result<Frame> {
    Ok(bitcode::deserialize(bytes)?)
}

/// A framed-TCP [`MessageSink`] over a static membership map.
pub struct TcpTransport {
    from: NodeId,
    peers: Arc<HashMap<NodeId, SocketAddr>>,
    /// Per-peer outbound queues; each backed by a lazily-spawned writer task.
    senders: Mutex<HashMap<NodeId, mpsc::UnboundedSender<Frame>>>,
}

impl TcpTransport {
    /// Builds a transport for `from` that can reach the nodes in `peers`.
    pub fn new(from: NodeId, peers: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            from,
            peers: Arc::new(peers),
            senders: Mutex::new(HashMap::new()),
        }
    }

    /// The outbound queue for `to`, spawning its writer task on first use.
    /// `None` if `to` is not a known peer.
    fn writer_for(&self, to: NodeId) -> Option<mpsc::UnboundedSender<Frame>> {
        let mut senders = self.senders.lock().expect("senders poisoned");
        if let Some(tx) = senders.get(&to) {
            return Some(tx.clone());
        }
        let addr = *self.peers.get(&to)?;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(peer_writer(addr, rx));
        senders.insert(to, tx.clone());
        Some(tx)
    }
}

#[async_trait]
impl MessageSink for TcpTransport {
    async fn send(&self, to: NodeId, message: Message) -> anyhow::Result<()> {
        if let Some(tx) = self.writer_for(to) {
            let _ = tx.send(Frame {
                from: self.from,
                message,
            });
        }
        Ok(())
    }
}

/// Drains a peer's outbound queue to a TCP connection, connecting on demand and
/// reconnecting after a failure. Frames sent while the peer is unreachable are
/// dropped (loss is tolerated by the protocol).
async fn peer_writer(addr: SocketAddr, mut rx: mpsc::UnboundedReceiver<Frame>) {
    let mut conn: Option<Framed<TcpStream, LengthDelimitedCodec>> = None;
    while let Some(frame) = rx.recv().await {
        if conn.is_none() {
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    conn = Some(Framed::new(stream, LengthDelimitedCodec::new()));
                }
                Err(_) => continue,
            }
        }
        let bytes = match encode(&frame) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        if let Some(framed) = conn.as_mut() {
            if framed.send(Bytes::from(bytes)).await.is_err() {
                conn = None;
            }
        }
    }
}

/// Accepts inbound connections on `listener`, decoding frames and forwarding
/// each as an [`Envelope`] to `inbox`. Returns the accept-loop task handle.
pub fn serve(listener: TcpListener, inbox: mpsc::UnboundedSender<Envelope>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(read_connection(stream, inbox.clone()));
        }
    })
}

/// Reads framed messages from one inbound connection until it closes.
async fn read_connection(stream: TcpStream, inbox: mpsc::UnboundedSender<Envelope>) {
    let _ = stream.set_nodelay(true);
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(item) = framed.next().await {
        let bytes = match item {
            Ok(bytes) => bytes,
            Err(_) => break,
        };
        match decode(&bytes) {
            Ok(frame) => {
                if inbox
                    .send(Envelope {
                        from: frame.from,
                        message: frame.message,
                    })
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => continue,
        }
    }
}
