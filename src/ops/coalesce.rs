use std::collections::HashMap;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::send::{send_batch, send_message};
use crate::error::ApiError;

/// One pending send operation waiting for a msg_id from the coalescing loop.
pub struct PendingMessage {
    pub queue_name: String,
    pub message: serde_json::Value,
    pub headers: Option<serde_json::Value>,
    pub delay: i32,
    pub sync_commit: bool,
    pub tx: oneshot::Sender<Result<i64, ApiError>>,
}

/// Cloneable handle to the coalescing background task.
#[derive(Clone)]
pub struct Coalescer(mpsc::Sender<PendingMessage>);

impl Coalescer {
    /// Submit a message for coalesced sending. Awaits the assigned msg_id.
    pub async fn send(
        &self,
        queue_name: String,
        message: serde_json::Value,
        headers: Option<serde_json::Value>,
        delay: i32,
        sync_commit: bool,
    ) -> Result<i64, ApiError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(PendingMessage {
                queue_name,
                message,
                headers,
                delay,
                sync_commit,
                tx,
            })
            .await
            .map_err(|_| ApiError::Internal(anyhow::anyhow!("coalescer channel closed")))?;
        rx.await
            .map_err(|_| ApiError::Internal(anyhow::anyhow!("coalescer responder dropped")))?
    }
}

/// Start the coalescing background task and return a handle to it.
///
/// `linger_ms` controls the collection window. Zero disables batching (each
/// message is flushed immediately after the first one arrives, which still
/// allows multiple in-flight messages to be grouped if they arrived before the
/// flush began).
pub fn start(pool: PgPool, linger_ms: u64) -> (Coalescer, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<PendingMessage>(16_384);
    let handle = tokio::spawn(run(pool, rx, linger_ms));
    (Coalescer(tx), handle)
}

// ---------------------------------------------------------------------------
// Background task
// ---------------------------------------------------------------------------

async fn run(pool: PgPool, mut rx: mpsc::Receiver<PendingMessage>, linger_ms: u64) {
    let linger = Duration::from_millis(linger_ms);

    loop {
        // Block until at least one message arrives.
        let first = match rx.recv().await {
            Some(m) => m,
            None => return, // all Coalescer handles dropped — shut down
        };

        // Collect additional messages that arrive within the linger window.
        let deadline = tokio::time::Instant::now() + linger;
        let mut pending: Vec<PendingMessage> = vec![first];

        while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            pending.push(msg);
        }

        flush(&pool, pending).await;
    }
}

async fn flush(pool: &PgPool, pending: Vec<PendingMessage>) {
    // Group by (queue_name, delay, sync_commit).
    // A batch is only async-commit if ALL members opted in.
    // Messages with different (queue, delay) tuples need separate batch calls.
    type GroupKey = (String, i32);
    let mut groups: HashMap<GroupKey, BatchGroup> = HashMap::new();

    for msg in pending {
        let key = (msg.queue_name.clone(), msg.delay);
        let group = groups.entry(key).or_insert_with(|| BatchGroup {
            queue_name: msg.queue_name.clone(),
            delay: msg.delay,
            sync_commit: true, // gets ANDed down: only false if every member says so
            messages: Vec::new(),
            headers: Vec::new(),
            responders: Vec::new(),
        });
        // sync_commit=false only if ALL messages in the group request it.
        group.sync_commit = group.sync_commit && msg.sync_commit;
        group.messages.push(msg.message);
        group.headers.push(msg.headers);
        group.responders.push(msg.tx);
    }

    for (_, group) in groups {
        group.flush(pool).await;
    }
}

struct BatchGroup {
    queue_name: String,
    delay: i32,
    sync_commit: bool,
    messages: Vec<serde_json::Value>,
    headers: Vec<Option<serde_json::Value>>,
    responders: Vec<oneshot::Sender<Result<i64, ApiError>>>,
}

impl BatchGroup {
    async fn flush(mut self, pool: &PgPool) {
        if self.messages.len() == 1 {
            // Single message — avoid batch overhead.
            let message = self.messages.pop().expect("len == 1");
            let header = self.headers.pop().expect("len == 1");
            let tx = self.responders.pop().expect("len == 1");
            let result = send_message(
                pool,
                &self.queue_name,
                message,
                header,
                self.delay,
                self.sync_commit,
            )
            .await
            .map(|r| r.msg_id);
            let _ = tx.send(result);
            return;
        }

        // Normalise headers: always pass a full array so each message keeps its
        // own headers. serde_json::Value::Null signals "no headers" per message.
        let has_any_headers = self.headers.iter().any(|h| h.is_some());
        let headers_vec: Option<Vec<serde_json::Value>> = if has_any_headers {
            Some(
                self.headers
                    .into_iter()
                    .map(|h| h.unwrap_or(serde_json::Value::Null))
                    .collect(),
            )
        } else {
            None
        };

        match send_batch(
            pool,
            &self.queue_name,
            self.messages,
            headers_vec,
            self.delay,
            self.sync_commit,
        )
        .await
        {
            Ok(result) => {
                for (tx, id) in self.responders.into_iter().zip(result.msg_ids) {
                    let _ = tx.send(Ok(id));
                }
            }
            Err(e) => {
                // Fan the error out to all waiters. ApiError is not Clone, so we
                // reconstruct per-waiter from extracted state. QueueNotFound is the
                // one variant with a distinct 4xx status; everything else is 500.
                let queue_not_found = if let ApiError::QueueNotFound(ref n) = e {
                    Some(n.clone())
                } else {
                    None
                };
                let msg = e.to_string();
                for tx in self.responders {
                    let err = match queue_not_found.clone() {
                        Some(name) => ApiError::QueueNotFound(name),
                        None => ApiError::Internal(anyhow::anyhow!("{}", msg)),
                    };
                    let _ = tx.send(Err(err));
                }
            }
        }
    }
}
