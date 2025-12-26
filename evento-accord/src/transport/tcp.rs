//! TCP-based transport implementation.
//!
//! Provides real network communication between ACCORD nodes using TCP sockets
//! with bitcode serialization.

use crate::config::ConnectionConfig;
use crate::error::{AccordError, Result};
use crate::protocol::Message;
use crate::transport::{framing, NodeAddr, NodeId, Transport};
use crate::txn::TxnId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{timeout, Duration};

/// TCP-based transport implementation with connection management.
///
/// Features:
/// - Automatic reconnection with exponential backoff
/// - Health checks for idle connections
/// - Connection pooling (one connection per peer)
pub struct TcpTransport {
    local_id: NodeId,
    peers: HashMap<NodeId, NodeAddr>,
    connections: Arc<RwLock<HashMap<NodeId, ConnectionHandle>>>,
    request_timeout: Duration,
    connection_config: ConnectionConfig,
    health_check_running: Arc<AtomicBool>,
}

/// Handle to an active connection.
struct ConnectionHandle {
    sender: mpsc::Sender<OutgoingRequest>,
    /// Timestamp of last successful activity (ms since UNIX epoch).
    last_activity: Arc<AtomicU64>,
    /// Whether this connection is healthy.
    healthy: Arc<AtomicBool>,
}

impl ConnectionHandle {
    /// Create a new connection handle.
    fn new(sender: mpsc::Sender<OutgoingRequest>) -> Self {
        Self {
            sender,
            last_activity: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            )),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Mark the connection as having recent activity.
    fn touch(&self) {
        self.last_activity.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            Ordering::Relaxed,
        );
    }

    /// Check if the connection is idle.
    fn is_idle(&self, idle_timeout: Duration) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last = self.last_activity.load(Ordering::Relaxed);
        let idle_ms = idle_timeout.as_millis() as u64;
        // Use >= so that zero timeout means "always idle"
        now.saturating_sub(last) >= idle_ms
    }

    /// Check if the connection is usable.
    fn is_usable(&self) -> bool {
        !self.sender.is_closed() && self.healthy.load(Ordering::Relaxed)
    }

    /// Mark the connection as unhealthy.
    #[allow(dead_code)]
    fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }
}

/// Request to send through a connection.
struct OutgoingRequest {
    /// Transaction ID for matching response to request.
    txn_id: TxnId,
    message: Message,
    reply_tx: oneshot::Sender<Result<Message>>,
}

impl TcpTransport {
    /// Create a new TCP transport.
    pub fn new(local_id: NodeId, peers: Vec<NodeAddr>) -> Self {
        Self::with_config(local_id, peers, ConnectionConfig::default())
    }

    /// Create a new TCP transport with custom connection configuration.
    pub fn with_config(local_id: NodeId, peers: Vec<NodeAddr>, config: ConnectionConfig) -> Self {
        let peers_map: HashMap<_, _> = peers.into_iter().map(|p| (p.id, p)).collect();

        Self {
            local_id,
            peers: peers_map,
            connections: Arc::new(RwLock::new(HashMap::new())),
            request_timeout: Duration::from_secs(5),
            connection_config: config,
            health_check_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Start background health checks for connections.
    ///
    /// Returns a handle that can be used to stop the health check task.
    pub fn start_health_checks(&self) -> tokio::task::JoinHandle<()> {
        if self.health_check_running.swap(true, Ordering::SeqCst) {
            // Already running, return a no-op handle
            return tokio::spawn(async {});
        }

        let connections = self.connections.clone();
        let idle_timeout = self.connection_config.idle_timeout;
        let check_interval = self.connection_config.health_check_interval;
        let running = self.health_check_running.clone();

        tokio::spawn(async move {
            tracing::debug!("Starting connection health checks");
            while running.load(Ordering::SeqCst) {
                tokio::time::sleep(check_interval).await;

                let mut conns = connections.write().await;
                let mut to_remove = Vec::new();

                for (node_id, handle) in conns.iter() {
                    if !handle.is_usable() {
                        tracing::debug!("Removing unhealthy connection to node {}", node_id);
                        to_remove.push(*node_id);
                    } else if handle.is_idle(idle_timeout) {
                        tracing::debug!("Removing idle connection to node {}", node_id);
                        to_remove.push(*node_id);
                    }
                }

                for node_id in to_remove {
                    conns.remove(&node_id);
                }
            }
            tracing::debug!("Stopping connection health checks");
        })
    }

    /// Stop background health checks.
    pub fn stop_health_checks(&self) {
        self.health_check_running.store(false, Ordering::SeqCst);
    }

    /// Get or create a connection to a peer with retry logic.
    async fn get_connection(
        &self,
        node: NodeId,
    ) -> Result<(mpsc::Sender<OutgoingRequest>, Arc<AtomicU64>)> {
        // Check if we already have a usable connection
        {
            let conns = self.connections.read().await;
            if let Some(handle) = conns.get(&node) {
                if handle.is_usable() {
                    return Ok((handle.sender.clone(), handle.last_activity.clone()));
                }
            }
        }

        // Need to create a new connection with retry
        self.connect_with_retry(node).await
    }

    /// Connect to a peer with exponential backoff retry.
    async fn connect_with_retry(
        &self,
        node: NodeId,
    ) -> Result<(mpsc::Sender<OutgoingRequest>, Arc<AtomicU64>)> {
        let mut conns = self.connections.write().await;

        // Double-check (another task might have created it)
        if let Some(handle) = conns.get(&node) {
            if handle.is_usable() {
                return Ok((handle.sender.clone(), handle.last_activity.clone()));
            }
            // Remove unhealthy connection
            conns.remove(&node);
        }

        let addr = self
            .peers
            .get(&node)
            .ok_or(AccordError::NodeUnreachable(node))?;

        let mut backoff = self.connection_config.initial_backoff;

        for attempt in 0..=self.connection_config.max_retries {
            if attempt > 0 {
                tracing::debug!(
                    "Retrying connection to node {} (attempt {}/{}), backoff {:?}",
                    node,
                    attempt,
                    self.connection_config.max_retries,
                    backoff
                );
                // Release lock during backoff
                drop(conns);
                tokio::time::sleep(backoff).await;
                conns = self.connections.write().await;

                // Check again in case another task connected
                if let Some(handle) = conns.get(&node) {
                    if handle.is_usable() {
                        return Ok((handle.sender.clone(), handle.last_activity.clone()));
                    }
                }

                // Exponential backoff
                backoff = Duration::from_secs_f64(
                    (backoff.as_secs_f64() * self.connection_config.backoff_multiplier)
                        .min(self.connection_config.max_backoff.as_secs_f64()),
                );
            }

            match TcpStream::connect(addr.socket_addr()).await {
                Ok(stream) => {
                    let (sender, receiver) = mpsc::channel(256);
                    let handle = ConnectionHandle::new(sender.clone());
                    let last_activity = handle.last_activity.clone();

                    // Spawn connection handler
                    let node_id = node;
                    let healthy = handle.healthy.clone();
                    tokio::spawn(async move {
                        Self::connection_handler(stream, receiver, node_id, healthy).await;
                    });

                    conns.insert(node, handle);

                    tracing::debug!("Connected to node {} (attempt {})", node, attempt + 1);
                    return Ok((sender, last_activity));
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to connect to node {} (attempt {}): {}",
                        node,
                        attempt + 1,
                        e
                    );
                }
            }
        }

        Err(AccordError::NodeUnreachable(node))
    }

    /// Handle a single connection to a peer.
    async fn connection_handler(
        stream: TcpStream,
        mut requests: mpsc::Receiver<OutgoingRequest>,
        node_id: NodeId,
        healthy: Arc<AtomicBool>,
    ) {
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Track pending requests by transaction ID
        let pending: Arc<RwLock<HashMap<TxnId, oneshot::Sender<Result<Message>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Spawn reader task
        let pending_clone = pending.clone();
        let healthy_clone = healthy.clone();
        let reader_handle = tokio::spawn(async move {
            loop {
                match framing::read_message(&mut reader).await {
                    Ok(msg) => {
                        // Match response to request using txn_id
                        let txn_id = msg.txn_id();
                        let mut pending = pending_clone.write().await;
                        if let Some(reply_tx) = pending.remove(&txn_id) {
                            let _ = reply_tx.send(Ok(msg));
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Connection to node {} read error: {}", node_id, e);
                        healthy_clone.store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }
        });

        // Process outgoing requests
        while let Some(request) = requests.recv().await {
            // Register pending response by txn_id
            pending
                .write()
                .await
                .insert(request.txn_id, request.reply_tx);

            // Send message
            if let Err(e) = framing::write_message(&mut writer, &request.message).await {
                tracing::debug!("Connection to node {} write error: {}", node_id, e);
                healthy.store(false, Ordering::Relaxed);
                // Remove pending and notify of error
                if let Some(reply_tx) = pending.write().await.remove(&request.txn_id) {
                    let _ = reply_tx.send(Err(e));
                }
                break;
            }
        }

        reader_handle.abort();
        tracing::debug!("Connection handler for node {} terminated", node_id);
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, node: NodeId, msg: &Message) -> Result<Message> {
        let (sender, last_activity) = self.get_connection(node).await?;

        let (reply_tx, reply_rx) = oneshot::channel();
        let request = OutgoingRequest {
            txn_id: msg.txn_id(),
            message: msg.clone(),
            reply_tx,
        };

        sender
            .send(request)
            .await
            .map_err(|_| AccordError::NodeUnreachable(node))?;

        let result = timeout(self.request_timeout, reply_rx)
            .await
            .map_err(|_| AccordError::Timeout(format!("request to node {}", node)))?
            .map_err(|_| AccordError::NodeUnreachable(node))?;

        // Update last activity on successful response
        if result.is_ok() {
            last_activity.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                Ordering::Relaxed,
            );
        }

        result
    }

    async fn broadcast(&self, msg: &Message) -> Vec<Result<Message>> {
        let peers: Vec<_> = self.peers.keys().copied().collect();

        let futures: Vec<_> = peers.iter().map(|&node| self.send(node, msg)).collect();

        futures::future::join_all(futures).await
    }

    fn broadcast_no_wait(&self, msg: Message) {
        let peers: Vec<_> = self.peers.keys().copied().collect();
        let connections = self.connections.clone();
        let peers_addrs = self.peers.clone();
        let txn_id = msg.txn_id();

        tokio::spawn(async move {
            for node in peers {
                let conns = connections.read().await;
                if let Some(handle) = conns.get(&node) {
                    if handle.is_usable() {
                        let (reply_tx, _) = oneshot::channel();
                        let request = OutgoingRequest {
                            txn_id,
                            message: msg.clone(),
                            reply_tx,
                        };
                        let _ = handle.sender.send(request).await;
                        handle.touch();
                    }
                } else {
                    // Try to connect and send
                    drop(conns);
                    if let Some(addr) = peers_addrs.get(&node) {
                        if let Ok(stream) = TcpStream::connect(addr.socket_addr()).await {
                            let mut writer = BufWriter::new(stream);
                            let _ = framing::write_message(&mut writer, &msg).await;
                        }
                    }
                }
            }
        });
    }

    fn local_node(&self) -> NodeId {
        self.local_id
    }

    fn peers(&self) -> Vec<NodeId> {
        self.peers.keys().copied().collect()
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
}

/// TCP server for handling incoming connections.
pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    /// Bind to the given address.
    pub async fn bind(addr: &NodeAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr.socket_addr())
            .await
            .map_err(|e| AccordError::Internal(e.into()))?;

        Ok(Self { listener })
    }

    /// Run the server loop, handling incoming connections.
    ///
    /// The handler function is called for each incoming message and should return
    /// a response message.
    pub async fn run<F, Fut>(self, handler: F) -> Result<()>
    where
        F: Fn(Message) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<Message>> + Send,
    {
        loop {
            let (stream, _addr) = self
                .listener
                .accept()
                .await
                .map_err(|e| AccordError::Internal(e.into()))?;

            let handler = handler.clone();
            tokio::spawn(async move {
                Self::handle_connection(stream, handler).await;
            });
        }
    }

    /// Handle a single client connection.
    async fn handle_connection<F, Fut>(stream: TcpStream, handler: F)
    where
        F: Fn(Message) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<Message>> + Send,
    {
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        while let Ok(msg) = framing::read_message(&mut reader).await {
            match handler(msg).await {
                Ok(response) => {
                    if framing::write_message(&mut writer, &response)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Handler error: {}", e);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{PreAcceptRequest, PreAcceptResponse};
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};

    fn make_test_txn(time: u64) -> Transaction {
        let ts = Timestamp::new(time, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3],
            keys: vec!["key1".to_string()],
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[test]
    fn test_transport_creation() {
        let peers = vec![
            NodeAddr::new(1, "localhost", 5001),
            NodeAddr::new(2, "localhost", 5002),
        ];
        let transport = TcpTransport::new(0, peers);

        assert_eq!(transport.local_node(), 0);
        assert_eq!(transport.cluster_size(), 3);
        assert_eq!(transport.quorum_size(), 2);
    }

    #[tokio::test]
    async fn test_server_bind() {
        let addr = NodeAddr::new(0, "127.0.0.1", 0); // Port 0 = random available
        let server = TcpServer::bind(&addr).await;
        assert!(server.is_ok());
    }

    #[tokio::test]
    async fn test_client_server_roundtrip() {
        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let server_addr = NodeAddr::new(1, "127.0.0.1", port);

        // Start server
        let server = TcpServer::bind(&server_addr).await.unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = server
                .run(|msg| async move {
                    match msg {
                        Message::PreAccept(req) => Ok(Message::PreAcceptOk(PreAcceptResponse {
                            txn_id: req.txn.id,
                            deps: vec![],
                            ballot: req.ballot,
                        })),
                        _ => Err(AccordError::Internal(anyhow::anyhow!("unexpected message"))),
                    }
                })
                .await;
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create client
        let transport = TcpTransport::new(0, vec![server_addr]);

        let txn = make_test_txn(100);
        let request = Message::PreAccept(PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::initial(1),
        });

        // Send request
        let response = transport.send(1, &request).await.unwrap();

        match response {
            Message::PreAcceptOk(resp) => {
                assert_eq!(resp.txn_id, txn.id);
            }
            _ => panic!("Expected PreAcceptOk"),
        }

        server_handle.abort();
    }

    #[test]
    fn test_transport_with_config() {
        let config = ConnectionConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            health_check_interval: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(600),
            max_connections_per_peer: 1,
        };

        let peers = vec![NodeAddr::new(1, "localhost", 5001)];
        let transport = TcpTransport::with_config(0, peers, config);

        assert_eq!(transport.local_node(), 0);
        assert_eq!(transport.connection_config.max_retries, 5);
        assert_eq!(
            transport.connection_config.initial_backoff,
            Duration::from_millis(200)
        );
    }

    #[test]
    fn test_connection_handle_idle() {
        let (sender, _receiver) = mpsc::channel(1);
        let handle = ConnectionHandle::new(sender);

        // Newly created should not be idle with 5 minute timeout
        assert!(!handle.is_idle(Duration::from_secs(300)));

        // Should be idle with 0 timeout
        assert!(handle.is_idle(Duration::ZERO));
    }

    #[test]
    fn test_connection_handle_usable() {
        let (sender, receiver) = mpsc::channel(1);
        let handle = ConnectionHandle::new(sender);

        assert!(handle.is_usable());

        // Drop receiver to close channel
        drop(receiver);

        // Now should not be usable
        assert!(!handle.is_usable());
    }
}
