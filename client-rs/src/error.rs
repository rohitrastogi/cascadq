/// Client-side error types for CAScadq, mirroring the server's error codes.
#[derive(Debug, thiserror::Error)]
pub enum CascadqError {
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    #[error("queue already exists: {0}")]
    QueueAlreadyExists(String),

    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("task not claimed: {0}")]
    TaskNotClaimed(String),

    #[error("broker fenced: {0}")]
    BrokerFenced(String),

    #[error("flush exhausted: {0}")]
    FlushExhausted(String),

    #[error("payload validation error: {0}")]
    PayloadValidation(String),

    #[error("unexpected HTTP status {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}
