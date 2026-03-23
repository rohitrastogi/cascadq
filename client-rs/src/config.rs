/// Client-side configuration for CAScadq.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub heartbeat_interval_seconds: f64,
    pub max_retries: u32,
    pub retry_base_delay_seconds: f64,
    pub retry_max_delay_seconds: f64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8000".to_owned(),
            heartbeat_interval_seconds: 10.0,
            max_retries: 3,
            retry_base_delay_seconds: 0.5,
            retry_max_delay_seconds: 30.0,
        }
    }
}
