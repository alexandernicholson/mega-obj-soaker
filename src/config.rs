use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub min_processes: usize,
    pub max_processes: usize,
    pub max_speed: f64,
    pub optimization_interval: f64,
    pub max_retries: u32,
    pub retry_delay: f64,
    pub verify_ssl: bool,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            min_processes: env::var("MIN_PROCESSES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            max_processes: env::var("MAX_PROCESSES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16),
            max_speed: env::var("MAX_SPEED")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(99_999_999_999_999_999_999.0),
            optimization_interval: env::var("OPTIMIZATION_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10.0),
            max_retries: env::var("MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            retry_delay: env::var("RETRY_DELAY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5.0),
            verify_ssl: env::var("S3_VERIFY_SSL")
                .ok()
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(true),
        }
    }
}
