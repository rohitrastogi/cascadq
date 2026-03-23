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

/// URL-encode a path segment to handle special characters in queue names.
fn encode_path(segment: &str) -> String {
    // Percent-encode everything except unreserved chars (RFC 3986)
    segment
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                String::from(b as char)
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
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
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs_f64(config.request_timeout_seconds))
            .build()
            .expect("failed to build HTTP client");
        Self { http, config }
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
        let encoded = encode_path(name);
        self.request(
            reqwest::Method::DELETE,
            &format!("/queues/{encoded}"),
            None::<&()>,
            &[204],
            ErrorContext::Default,
        )
        .await?;
        Ok(())
    }

    /// Pushes a task to a queue. Generates a random push_key for idempotency.
    pub async fn push(&self, queue_name: &str, payload: Value) -> Result<()> {
        let encoded = encode_path(queue_name);
        let body = PushRequest {
            payload,
            push_key: Uuid::new_v4().simple().to_string(),
        };
        self.request(
            reqwest::Method::POST,
            &format!("/queues/{encoded}/push"),
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
        let encoded = encode_path(queue_name);
        let body = ClaimRequest {
            claim_key: Uuid::new_v4().simple().to_string(),
            timeout_seconds: timeout.map(|d| d.as_secs_f64()),
        };
        let resp = self
            .request(
                reqwest::Method::POST,
                &format!("/queues/{encoded}/claim"),
                Some(&body),
                &[200, 204],
                ErrorContext::Default,
            )
            .await?;

        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }

        let data: ClaimResponse = resp.json().await?;

        let heartbeat_url = format!("{}/queues/{encoded}/heartbeat", self.config.base_url);
        let heartbeat_interval = Duration::from_secs_f64(self.config.heartbeat_interval_seconds);
        let finish_url = format!("{}/queues/{encoded}/finish", self.config.base_url);
        let http = self.http.clone();

        let heartbeat_handle = tokio::spawn(heartbeat_loop(
            http.clone(),
            heartbeat_url,
            data.task_id.clone(),
            heartbeat_interval,
        ));

        Ok(Some(ClaimedTask {
            task_id: data.task_id,
            sequence: data.sequence,
            payload: data.payload,
            http,
            finish_url,
            heartbeat_handle: Some(heartbeat_handle),
            max_retries: self.config.max_retries,
            retry_base_delay: self.config.retry_base_delay_seconds,
            retry_max_delay: self.config.retry_max_delay_seconds,
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
                        backoff(
                            attempt,
                            self.config.retry_base_delay_seconds,
                            self.config.retry_max_delay_seconds,
                        )
                        .await;
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
                    backoff(
                        attempt,
                        self.config.retry_base_delay_seconds,
                        self.config.retry_max_delay_seconds,
                    )
                    .await;
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
    task_id: String,
    sequence: u64,
    payload: Value,
    http: reqwest::Client,
    finish_url: String,
    heartbeat_handle: Option<JoinHandle<()>>,
    max_retries: u32,
    retry_base_delay: f64,
    retry_max_delay: f64,
}

impl ClaimedTask {
    /// Returns the task identifier.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns the task's sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the task payload.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// Marks the task as completed.
    ///
    /// Retries transient failures with exponential backoff (same policy as
    /// the client's other RPCs). Stops heartbeats before returning.
    /// Consumes `self` so the task cannot be used after finishing.
    pub async fn finish(mut self) -> Result<()> {
        // Stop heartbeats first — the finish call itself may take a few
        // retries, and we don't need heartbeats once we've decided to finish.
        self.stop_heartbeat();

        let body = FinishRequest {
            task_id: self.task_id.clone(),
            sequence: self.sequence,
        };

        let mut last_error: Option<CascadqError> = None;
        let mut last_body = String::new();
        let mut last_status = StatusCode::INTERNAL_SERVER_ERROR;

        for attempt in 0..=self.max_retries {
            let resp = match self.http.post(&self.finish_url).json(&body).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(CascadqError::Http(e));
                    if attempt < self.max_retries {
                        backoff(attempt, self.retry_base_delay, self.retry_max_delay).await;
                    }
                    continue;
                }
            };

            let status = resp.status();
            if status == StatusCode::NO_CONTENT {
                return Ok(());
            }

            if status.is_server_error() {
                last_body = resp.text().await.unwrap_or_default();
                last_status = status;
                last_error = None;
                if attempt < self.max_retries {
                    backoff(attempt, self.retry_base_delay, self.retry_max_delay).await;
                }
                continue;
            }

            // 4xx — translate to domain error immediately
            let text = resp.text().await.unwrap_or_default();
            return Err(map_error(status, &text, &ErrorContext::Task));
        }

        if let Some(e) = last_error {
            return Err(e);
        }
        Err(map_error(last_status, &last_body, &ErrorContext::Task))
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

/// Pre-serializes the heartbeat body once and reuses it across iterations
/// to avoid cloning the task_id string on every heartbeat.
async fn heartbeat_loop(http: reqwest::Client, url: String, task_id: String, interval: Duration) {
    let body = serde_json::to_vec(&HeartbeatRequest {
        task_id: task_id.clone(),
    })
    .expect("HeartbeatRequest serialization cannot fail");

    loop {
        tokio::time::sleep(interval).await;

        let result = http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone())
            .send()
            .await;

        match result {
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

/// Exponential backoff with jitter, shared by client and claimed task retry.
async fn backoff(attempt: u32, base_delay: f64, max_delay: f64) {
    let delay = (base_delay * 2.0f64.powi(attempt as i32)).min(max_delay);
    let jitter = delay * (rand_frac() * 0.5);
    tokio::time::sleep(Duration::from_secs_f64(delay + jitter)).await;
}

/// Simple pseudo-random fraction [0, 1) for jitter, avoiding a rand dependency.
fn rand_frac() -> f64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (t.subsec_nanos() as f64) / 1_000_000_000.0
}
