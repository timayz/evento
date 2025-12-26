//! Message framing for TCP transport.
//!
//! Uses a simple length-prefixed format:
//! [length: 4 bytes LE u32][payload: bitcode encoded Message]

use crate::error::{AccordError, Result};
use crate::protocol::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum message size (16 MB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Write a message to an async writer with length-prefix framing.
pub async fn write_message<W: AsyncWriteExt + Unpin>(writer: &mut W, msg: &Message) -> Result<()> {
    let payload = bitcode::encode(msg);

    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(AccordError::Internal(anyhow::anyhow!(
            "message too large: {} bytes (max {})",
            payload.len(),
            MAX_MESSAGE_SIZE
        )));
    }

    let len = payload.len() as u32;
    writer
        .write_all(&len.to_le_bytes())
        .await
        .map_err(|e| AccordError::Internal(e.into()))?;
    writer
        .write_all(&payload)
        .await
        .map_err(|e| AccordError::Internal(e.into()))?;
    writer
        .flush()
        .await
        .map_err(|e| AccordError::Internal(e.into()))?;

    Ok(())
}

/// Read a message from an async reader with length-prefix framing.
pub async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Message> {
    // Read length prefix
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| AccordError::Internal(e.into()))?;

    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_MESSAGE_SIZE {
        return Err(AccordError::Internal(anyhow::anyhow!(
            "message too large: {} bytes (max {})",
            len,
            MAX_MESSAGE_SIZE
        )));
    }

    // Read payload
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| AccordError::Internal(e.into()))?;

    // Decode message
    bitcode::decode(&payload).map_err(|e| AccordError::Internal(e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{PreAcceptRequest, PreAcceptResponse};
    use crate::timestamp::Timestamp;
    use crate::txn::{Ballot, Transaction, TxnId, TxnStatus};
    use std::io::Cursor;

    fn make_test_txn() -> Transaction {
        let ts = Timestamp::new(100, 0, 1);
        Transaction {
            id: TxnId::new(ts),
            events_data: vec![1, 2, 3, 4, 5],
            keys: vec!["key1".to_string(), "key2".to_string()],
            deps: vec![],
            status: TxnStatus::PreAccepted,
            execute_at: ts,
            ballot: Ballot::initial(1),
        }
    }

    #[tokio::test]
    async fn test_write_read_roundtrip() {
        let txn = make_test_txn();
        let msg = Message::PreAccept(PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::initial(1),
        });

        // Write to buffer
        let mut buffer = Vec::new();
        write_message(&mut buffer, &msg).await.unwrap();

        // Read back
        let mut cursor = Cursor::new(buffer);
        let decoded = read_message(&mut cursor).await.unwrap();

        match decoded {
            Message::PreAccept(req) => {
                assert_eq!(req.txn.id, txn.id);
                assert_eq!(req.txn.events_data, txn.events_data);
            }
            _ => panic!("Expected PreAccept message"),
        }
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let txn = make_test_txn();
        let msg1 = Message::PreAccept(PreAcceptRequest {
            txn: txn.clone(),
            ballot: Ballot::initial(1),
        });
        let msg2 = Message::PreAcceptOk(PreAcceptResponse {
            txn_id: txn.id,
            deps: vec![],
            ballot: Ballot::initial(1),
        });

        // Write multiple messages
        let mut buffer = Vec::new();
        write_message(&mut buffer, &msg1).await.unwrap();
        write_message(&mut buffer, &msg2).await.unwrap();

        // Read them back
        let mut cursor = Cursor::new(buffer);
        let decoded1 = read_message(&mut cursor).await.unwrap();
        let decoded2 = read_message(&mut cursor).await.unwrap();

        assert!(matches!(decoded1, Message::PreAccept(_)));
        assert!(matches!(decoded2, Message::PreAcceptOk(_)));
    }

    #[tokio::test]
    async fn test_framing_format() {
        let txn = make_test_txn();
        let msg = Message::PreAccept(PreAcceptRequest {
            txn,
            ballot: Ballot::initial(1),
        });

        let mut buffer = Vec::new();
        write_message(&mut buffer, &msg).await.unwrap();

        // First 4 bytes should be length (little endian)
        let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        // Rest should be payload
        assert_eq!(buffer.len(), 4 + len);

        // Payload should decode
        let payload = &buffer[4..];
        let decoded: Message = bitcode::decode(payload).unwrap();
        assert!(matches!(decoded, Message::PreAccept(_)));
    }

    #[tokio::test]
    async fn test_empty_payload() {
        // Simulate a message with 0 length
        let buffer = vec![0u8, 0, 0, 0]; // Length = 0
        let mut cursor = Cursor::new(buffer);

        // Should fail to decode empty payload
        let result = read_message(&mut cursor).await;
        assert!(result.is_err());
    }
}
