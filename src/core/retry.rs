use std::time::Duration;
use tokio::time::sleep;

/// Exponential backoff with jitter.
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, initial_ms: u64, max_ms: u64) -> Self {
        Self {
            max_retries,
            initial_backoff: Duration::from_millis(initial_ms),
            max_backoff: Duration::from_millis(max_ms),
        }
    }

    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let base = self.initial_backoff.as_millis() as u64;
        let exp = base.saturating_mul(1u64 << (attempt.min(16)));
        let capped = exp.min(self.max_backoff.as_millis() as u64);
        // Simple jitter +/- 20%
        let jitter = (capped as f64 * 0.2 * (rand_factor() - 0.5)) as i64;
        let ms = (capped as i64 + jitter).max(0) as u64;
        Duration::from_millis(ms)
    }

    pub async fn wait(&self, attempt: u32) {
        let d = self.backoff_duration(attempt);
        if !d.is_zero() {
            sleep(d).await;
        }
    }
}

fn rand_factor() -> f64 {
    // Lightweight pseudo-random without extra deps for jitter
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut h = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut h);
    (h.finish() % 10000) as f64 / 10000.0
}
