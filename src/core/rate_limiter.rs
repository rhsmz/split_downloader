use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use nonzero_ext::nonzero;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

/// Token-bucket rate limiter wrapper (bytes per second).
#[derive(Clone)]
pub struct BandwidthLimiter {
    inner: Option<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>>,
    limit_bps: u64,
}

impl BandwidthLimiter {
    pub fn new(limit_bps: u64) -> Self {
        if limit_bps == 0 {
            return Self {
                inner: None,
                limit_bps: 0,
            };
        }
        // Approximate: 1 cell = 1024 bytes
        let cells_per_sec = (limit_bps / 1024).max(1) as u32;
        let quota = Quota::per_second(NonZeroU32::new(cells_per_sec).unwrap_or(nonzero!(1u32)));
        let limiter = RateLimiter::direct(quota);
        Self {
            inner: Some(Arc::new(limiter)),
            limit_bps,
        }
    }

    pub fn limit_bps(&self) -> u64 {
        self.limit_bps
    }

    /// Wait until `bytes` can be consumed (approximate).
    pub async fn consume(&self, bytes: u64) {
        let Some(ref limiter) = self.inner else {
            return;
        };
        let cells = ((bytes + 1023) / 1024).max(1) as u32;
        let n = NonZeroU32::new(cells).unwrap_or(nonzero!(1u32));
        loop {
            match limiter.check_n(n) {
                Ok(_) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
}
