//! CAScadq client and claimed task handle.

use std::time::Duration;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

use crate::config::ClientConfig;
use crate::error::CascadqError;

type Result<T> = std::result::Result<T, CascadqError>;

// ---------------------------------------------------------------------------
// Request/response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct CreateQueueRequest {
    name: String,
    payload_schema: Value,
}

#[derive(Serialize)]
struct PushRequest {
    payload: Value,
    push_key: String,
}

#[derive(Serialize)]
struct ClaimRequest {
    claim_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_seconds: Option<f64>,
}

#[derive(Serialize)]
struct HeartbeatRequest {
    task_id: String,
}

#[derive(Serialize)]
struct FinishRequest {
    task_id: String,
    sequence: u64,
}

#[derive(Deserialize)]
struct ClaimResponse {
    task_id: String,
    sequence: u64,
    payload: Value,
}

#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    error: String,
    #[serde(default)]
    code: String,
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

/// Which domain error to raise for a given HTTP status code.
enum ErrorContext {
    Default,
    QueueCreate,
    Task,
}

fn map_error(status: StatusCode, body: &str, ctx: &ErrorContext) -> CascadqError {
    // Try to parse the JSON error body for a "code" override.
    let (detail, code) = match serde_json::from_str::<ErrorBody>(body) {
        Ok(b) => (b.error, b.code),
        Err(_) => (body.to_owned(), String::new()),
    };

    // Code-level overrides (disambiguate 503 variants)
    match code.as_str() {
        "broker_fenced" => return CascadqError::BrokerFenced(detail),
        "flush_exhausted" => return CascadqError::FlushExhausted(detail),
        _ => {}
    }

    // Status-code based mapping
    match (status.as_u16(), ctx) {
        (404, ErrorContext::Task) => CascadqError::TaskNotFound(detail),
        (404, _) => CascadqError::QueueNotFound(detail),
        (409, ErrorContext::QueueCreate) => CascadqError::QueueAlreadyExists(detail),
        (409, ErrorContext::Task) => CascadqError::TaskNotClaimed(detail),
        (409, _) => CascadqError::UnexpectedStatus {
            status: 409,
            body: detail,
        },
        (422, _) => CascadqError::PayloadValidation(detail),
        (503, _) => CascadqError::BrokerFenced(detail),
        _ => CascadqError::UnexpectedStatus {
            status: status.as_u16(),
            body: detail,
        },
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Async client for the CAScadq broker HTTP API.
///
/// Translates HTTP errors into domain errors and retries transient (5xx)
/// failures with exponential backoff and jitter.
pub struct CascadqClient {
    http: reqwest::Client,
    config: ClientConfig,
}

impl CascadqClient {
    /// Creates a new client with the given configuration.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    /// Creates a new queue.
    pub async fn create_queue(&self, name: &str, payload_schema: Option<Value>) -> Result<()> {
        let body = CreateQueueRequest {
            name: name.to_owned(),
            payload_schema: payload_schema.unwrap_or(Value::Object(Default::default())),
        };
        self.request(
            reqwest::Method::POST,
            "/queues",
            Some(&body),
            &[201],
            ErrorContext::QueueCreate,
        )
        .await?;
        Ok(())
    }

    /// Deletes a queue.
    pub async fn delete_queue(&self, name: &str) -> Result<()> {
        self.request(
            reqwest::Method::DELETE,
            &format!("/queues/{name}"),
            None::<&()>,
            &[204],
            ErrorContext::Default,
        )
        .await?;
        Ok(())
    }

    /// Pushes a task to a queue. Generates a random push_key for idempotency.
    pub async fn push(&self, queue_name: &str, payload: Value) -> Result<()> {
        let body = PushRequest {
            payload,
            push_key: Uuid::new_v4().simple().to_string(),
        };
        self.request(
            reqwest::Method::POST,
            &format!("/queues/{queue_name}/push"),
            Some(&body),
            &[204],
            ErrorContext::Default,
        )
        .await?;
        Ok(())
    }

    /// Claims the next pending task, blocking up to `timeout`.
    ///
    /// Returns `None` if the timeout expires with no task available.
    /// The returned [`ClaimedTask`] sends heartbeats in the background
    /// until [`ClaimedTask::finish`] is called or the task is dropped.
    pub async fn claim(
        &self,
        queue_name: &str,
        timeout: Option<Duration>,
    ) -> Result<Option<ClaimedTask>> {
        let body = ClaimRequest {
            claim_key: Uuid::new_v4().simple().to_string(),
            timeout_seconds: timeout.map(|d| d.as_secs_f64()),
        };
        let resp = self
            .request(
                reqwest::Method::POST,
                &format!("/queues/{queue_name}/claim"),
                Some(&body),
                &[200, 204],
                ErrorContext::Default,
            )
            .await?;

        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }

        let data: ClaimResponse = resp.json().await?;

        let heartbeat_url = format!("{}/queues/{queue_name}/heartbeat", self.config.base_url);
        let heartbeat_interval = Duration::from_secs_f64(self.config.heartbeat_interval_seconds);
        let task_id = data.task_id.clone();
        let http = self.http.clone();

        let heartbeat_handle = tokio::spawn(heartbeat_loop(
            http,
            heartbeat_url,
            task_id,
            heartbeat_interval,
        ));

        let finish_url = format!("{}/queues/{queue_name}/finish", self.config.base_url);

        Ok(Some(ClaimedTask {
            task_id: data.task_id,
            sequence: data.sequence,
            payload: data.payload,
            http: self.http.clone(),
            finish_url,
            heartbeat_handle: Some(heartbeat_handle),
            finished: false,
        }))
    }

    /// Sends an HTTP request with retry for transient failures.
    async fn request<B: Serialize>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
        expected: &[u16],
        ctx: ErrorContext,
    ) -> Result<reqwest::Response> {
        let url = format!("{}{path}", self.config.base_url);
        let mut last_error: Option<CascadqError> = None;
        let mut last_body = String::new();
        let mut last_status = StatusCode::INTERNAL_SERVER_ERROR;

        for attempt in 0..=self.config.max_retries {
            let mut req = self.http.request(method.clone(), &url);
            if let Some(b) = body {
                req = req.json(b);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(CascadqError::Http(e));
                    if attempt < self.config.max_retries {
                        self.backoff(attempt).await;
                    }
                    continue;
                }
            };

            let status = resp.status();
            if expected.contains(&status.as_u16()) {
                return Ok(resp);
            }

            if status.is_server_error() {
                last_body = resp.text().await.unwrap_or_default();
                last_status = status;
                last_error = None;
                if attempt < self.config.max_retries {
                    self.backoff(attempt).await;
                }
                continue;
            }

            // 4xx — translate to domain error immediately
            let text = resp.text().await.unwrap_or_default();
            return Err(map_error(status, &text, &ctx));
        }

        // Retries exhausted
        if let Some(e) = last_error {
            return Err(e);
        }
        Err(map_error(last_status, &last_body, &ctx))
    }

    async fn backoff(&self, attempt: u32) {
        let delay = (self.config.retry_base_delay_seconds * 2.0f64.powi(attempt as i32))
            .min(self.config.retry_max_delay_seconds);
        // Simple jitter: 0–50% of delay
        let jitter = delay * (rand_frac() * 0.5);
        tokio::time::sleep(Duration::from_secs_f64(delay + jitter)).await;
    }
}

impl Default for CascadqClient {
    fn default() -> Self {
        Self::new(ClientConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Claimed task
// ---------------------------------------------------------------------------

/// Handle for a claimed task. Sends heartbeats in the background.
///
/// Call [`finish`](Self::finish) to mark the task as completed and stop
/// heartbeats. If dropped without finishing, heartbeats are cancelled
/// and the server will eventually requeue the task via lease expiry.
pub struct ClaimedTask {
    pub task_id: String,
    pub sequence: u64,
    pub payload: Value,
    http: reqwest::Client,
    finish_url: String,
    heartbeat_handle: Option<JoinHandle<()>>,
    finished: bool,
}

impl ClaimedTask {
    /// Marks the task as completed.
    ///
    /// Sends the finish request to the broker, then stops heartbeats.
    /// Consumes `self` so the task cannot be used after finishing.
    pub async fn finish(mut self) -> Result<()> {
        self.finish_inner().await
    }

    async fn finish_inner(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        let body = FinishRequest {
            task_id: self.task_id.clone(),
            sequence: self.sequence,
        };
        let resp = self.http.post(&self.finish_url).json(&body).send().await?;

        let status = resp.status();
        if status == StatusCode::NO_CONTENT {
            self.finished = true;
            self.stop_heartbeat();
            return Ok(());
        }

        let text = resp.text().await.unwrap_or_default();
        Err(map_error(status, &text, &ErrorContext::Task))
    }

    fn stop_heartbeat(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for ClaimedTask {
    fn drop(&mut self) {
        self.stop_heartbeat();
    }
}

// ---------------------------------------------------------------------------
// Background heartbeat loop
// ---------------------------------------------------------------------------

async fn heartbeat_loop(http: reqwest::Client, url: String, task_id: String, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;

        let body = HeartbeatRequest {
            task_id: task_id.clone(),
        };
        match http.post(&url).json(&body).send().await {
            Ok(resp) if resp.status() == StatusCode::NO_CONTENT => {}
            Ok(resp) => {
                let status = resp.status().as_u16();
                // 404 (task not found) or 409 (task not claimed) — stop heartbeating
                if status == 404 || status == 409 {
                    warn!(
                        task_id = %task_id,
                        status,
                        "heartbeat rejected, stopping"
                    );
                    return;
                }
                warn!(
                    task_id = %task_id,
                    status,
                    "heartbeat got unexpected status"
                );
            }
            Err(e) => {
                warn!(
                    task_id = %task_id,
                    error = %e,
                    "heartbeat failed"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simple pseudo-random fraction [0, 1) for jitter, avoiding a rand dependency.
fn rand_frac() -> f64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    // Use subsecond nanos to produce a rough [0, 1) value
    (t.subsec_nanos() as f64) / 1_000_000_000.0
}
