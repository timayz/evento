# Phase 4: TCP Transport Layer

## Objective

Implement the network transport layer using custom TCP + Bitcode for inter-node communication.

## Files to Create

```
evento-accord/src/
├── transport/
│   ├── mod.rs
│   ├── traits.rs       # Transport trait
│   ├── tcp.rs          # TCP implementation
│   ├── framing.rs      # Message framing
│   └── channel.rs      # In-memory (for testing)
```

## Tasks

### 4.1 Transport Trait

**File**: `src/transport/traits.rs`

```rust
use crate::protocol::Message;
use std::future::Future;

/// Node identifier
pub type NodeId = u16;

/// Network address for a node
#[derive(Clone, Debug)]
pub struct NodeAddr {
    pub id: NodeId,
    pub host: String,
    pub port: u16,
}

/// Transport layer abstraction
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send message to specific node and wait for response
    async fn send(&self, node: NodeId, msg: &Message) -> Result<Message>;

    /// Broadcast message to all nodes and collect responses
    async fn broadcast(&self, msg: &Message) -> Vec<Result<Message>>;

    /// Broadcast without waiting for responses (fire and forget)
    fn broadcast_no_wait(&self, msg: &Message);

    /// Get local node ID
    fn local_node(&self) -> NodeId;

    /// Get all known node IDs (excluding self)
    fn peers(&self) -> Vec<NodeId>;

    /// Number of nodes in cluster
    fn cluster_size(&self) -> usize;

    /// Required quorum size
    fn quorum_size(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}
```

**Acceptance Criteria**:
- [ ] Trait defined with all needed methods
- [ ] Async-friendly design
- [ ] Quorum calculation correct

---

### 4.2 Message Framing

**File**: `src/transport/framing.rs`

Simple length-prefixed framing for TCP streams.

```rust
use crate::protocol::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Frame format:
/// [length: u32 LE][payload: bitcode encoded Message]
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB

/// Write a message to the stream with framing
pub async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &Message,
) -> std::io::Result<()> {
    let payload = bitcode::encode(msg);

    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "message too large",
        ));
    }

    let len = payload.len() as u32;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;

    Ok(())
}

/// Read a message from the stream
pub async fn read_message<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Message> {
    // Read length
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "message too large",
        ));
    }

    // Read payload
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;

    // Decode
    bitcode::decode(&payload).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })
}
```

**Acceptance Criteria**:
- [ ] Length-prefixed framing works
- [ ] Large message handling
- [ ] Round-trip encode/decode
- [ ] Unit tests

---

### 4.3 TCP Transport

**File**: `src/transport/tcp.rs`

```rust
use crate::{
    protocol::Message,
    transport::{framing, NodeAddr, NodeId, Transport},
    Result, AccordError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, RwLock},
};

/// TCP-based transport implementation
pub struct TcpTransport {
    local_id: NodeId,
    peers: HashMap<NodeId, NodeAddr>,
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
    incoming_tx: mpsc::Sender<(Message, oneshot::Sender<Message>)>,
}

struct Connection {
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    pending: HashMap<u64, oneshot::Sender<Message>>,
}

impl TcpTransport {
    /// Create new TCP transport
    pub async fn new(
        local_addr: NodeAddr,
        peers: Vec<NodeAddr>,
    ) -> Result<(Self, TcpServer)> {
        let (incoming_tx, incoming_rx) = mpsc::channel(1024);

        let peers_map: HashMap<_, _> = peers
            .into_iter()
            .map(|p| (p.id, p))
            .collect();

        let transport = Self {
            local_id: local_addr.id,
            peers: peers_map,
            connections: Arc::new(RwLock::new(HashMap::new())),
            incoming_tx,
        };

        let server = TcpServer::bind(local_addr, incoming_rx).await?;

        Ok((transport, server))
    }

    /// Get or create connection to peer
    async fn get_connection(&self, node: NodeId) -> Result<&mut Connection> {
        let mut conns = self.connections.write().await;

        if !conns.contains_key(&node) {
            let addr = self.peers.get(&node)
                .ok_or(AccordError::NodeUnreachable(node))?;

            let stream = TcpStream::connect(format!("{}:{}", addr.host, addr.port))
                .await
                .map_err(|_| AccordError::NodeUnreachable(node))?;

            let (reader, writer) = stream.into_split();

            // Spawn reader task
            let incoming_tx = self.incoming_tx.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(reader);
                loop {
                    match framing::read_message(&mut reader).await {
                        Ok(msg) => {
                            // Handle response or incoming request
                        }
                        Err(_) => break,
                    }
                }
            });

            conns.insert(node, Connection {
                writer: BufWriter::new(writer),
                pending: HashMap::new(),
            });
        }

        Ok(conns.get_mut(&node).unwrap())
    }
}

#[async_trait::async_trait]
impl Transport for TcpTransport {
    async fn send(&self, node: NodeId, msg: &Message) -> Result<Message> {
        let conn = self.get_connection(node).await?;

        // Send message
        framing::write_message(&mut conn.writer, msg).await
            .map_err(|_| AccordError::NodeUnreachable(node))?;

        // Wait for response
        let (tx, rx) = oneshot::channel();
        // Register pending response...

        rx.await.map_err(|_| AccordError::NodeUnreachable(node))
    }

    async fn broadcast(&self, msg: &Message) -> Vec<Result<Message>> {
        let peers: Vec<_> = self.peers.keys().copied().collect();

        let futures: Vec<_> = peers
            .iter()
            .map(|&node| self.send(node, msg))
            .collect();

        futures::future::join_all(futures).await
    }

    fn broadcast_no_wait(&self, msg: &Message) {
        let peers: Vec<_> = self.peers.keys().copied().collect();
        let msg = msg.clone();
        let this = self.clone();

        tokio::spawn(async move {
            for node in peers {
                let _ = this.send(node, &msg).await;
            }
        });
    }

    fn local_node(&self) -> NodeId { self.local_id }

    fn peers(&self) -> Vec<NodeId> {
        self.peers.keys().copied().collect()
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
}

/// TCP server for handling incoming connections
pub struct TcpServer {
    listener: TcpListener,
    incoming_rx: mpsc::Receiver<(Message, oneshot::Sender<Message>)>,
}

impl TcpServer {
    async fn bind(
        addr: NodeAddr,
        incoming_rx: mpsc::Receiver<(Message, oneshot::Sender<Message>)>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(format!("{}:{}", addr.host, addr.port)).await?;
        Ok(Self { listener, incoming_rx })
    }

    /// Run server loop, calls handler for each message
    pub async fn run<F, Fut>(mut self, handler: F) -> Result<()>
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Message>> + Send,
    {
        loop {
            tokio::select! {
                Ok((stream, _)) = self.listener.accept() => {
                    // Handle new connection
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        Self::handle_connection(stream, handler).await;
                    });
                }
                Some((msg, reply_tx)) = self.incoming_rx.recv() => {
                    let response = handler(msg).await?;
                    let _ = reply_tx.send(response);
                }
            }
        }
    }

    async fn handle_connection<F, Fut>(stream: TcpStream, handler: F)
    where
        F: Fn(Message) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Message>> + Send,
    {
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        loop {
            match framing::read_message(&mut reader).await {
                Ok(msg) => {
                    match handler(msg).await {
                        Ok(response) => {
                            if let Err(_) = framing::write_message(&mut writer, &response).await {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Handler error: {}", e);
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}
```

**Acceptance Criteria**:
- [ ] Connection pooling works
- [ ] Request/response matching
- [ ] Concurrent connections handled
- [ ] Graceful disconnect handling
- [ ] Integration tests with multiple nodes

---

### 4.4 In-Memory Transport (Testing)

**File**: `src/transport/channel.rs`

```rust
use crate::{protocol::Message, transport::{NodeId, Transport}, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};

/// In-memory transport for testing (no actual network)
pub struct ChannelTransport {
    local_id: NodeId,
    peers: Vec<NodeId>,
    senders: Arc<RwLock<HashMap<NodeId, mpsc::Sender<(Message, oneshot::Sender<Message>)>>>>,
    receiver: mpsc::Receiver<(Message, oneshot::Sender<Message>)>,
}

impl ChannelTransport {
    /// Create a cluster of connected in-memory transports
    pub fn create_cluster(node_count: usize) -> Vec<Self> {
        let mut senders: HashMap<NodeId, mpsc::Sender<_>> = HashMap::new();
        let mut receivers: HashMap<NodeId, mpsc::Receiver<_>> = HashMap::new();

        // Create channels for each node
        for i in 0..node_count {
            let (tx, rx) = mpsc::channel(1024);
            senders.insert(i as NodeId, tx);
            receivers.insert(i as NodeId, rx);
        }

        let senders = Arc::new(RwLock::new(senders));

        // Create transports
        (0..node_count)
            .map(|i| {
                let id = i as NodeId;
                let peers: Vec<_> = (0..node_count)
                    .filter(|&j| j != i)
                    .map(|j| j as NodeId)
                    .collect();

                Self {
                    local_id: id,
                    peers,
                    senders: senders.clone(),
                    receiver: receivers.remove(&id).unwrap(),
                }
            })
            .collect()
    }

    /// Process one incoming message
    pub async fn recv(&mut self) -> Option<(Message, oneshot::Sender<Message>)> {
        self.receiver.recv().await
    }
}

#[async_trait::async_trait]
impl Transport for ChannelTransport {
    async fn send(&self, node: NodeId, msg: &Message) -> Result<Message> {
        let senders = self.senders.read().await;
        let sender = senders.get(&node)
            .ok_or(AccordError::NodeUnreachable(node))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        sender.send((msg.clone(), reply_tx)).await
            .map_err(|_| AccordError::NodeUnreachable(node))?;

        reply_rx.await.map_err(|_| AccordError::NodeUnreachable(node))
    }

    async fn broadcast(&self, msg: &Message) -> Vec<Result<Message>> {
        let futures: Vec<_> = self.peers
            .iter()
            .map(|&node| self.send(node, msg))
            .collect();

        futures::future::join_all(futures).await
    }

    fn broadcast_no_wait(&self, msg: &Message) {
        let msg = msg.clone();
        let senders = self.senders.clone();
        let peers = self.peers.clone();

        tokio::spawn(async move {
            let senders = senders.read().await;
            for node in peers {
                if let Some(sender) = senders.get(&node) {
                    let (reply_tx, _) = oneshot::channel();
                    let _ = sender.send((msg.clone(), reply_tx)).await;
                }
            }
        });
    }

    fn local_node(&self) -> NodeId { self.local_id }
    fn peers(&self) -> Vec<NodeId> { self.peers.clone() }
    fn cluster_size(&self) -> usize { self.peers.len() + 1 }
}
```

**Acceptance Criteria**:
- [ ] Create N-node cluster easily
- [ ] Messages delivered in-order per connection
- [ ] Useful for unit testing protocol
- [ ] No real network dependency

---

## Definition of Done

- [ ] Transport trait implemented
- [ ] TCP transport works between real nodes
- [ ] Channel transport works for testing
- [ ] Connection pooling and reconnection
- [ ] Integration test: 3 TCP nodes communicating
