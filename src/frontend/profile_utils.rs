use std::time::{Duration, Instant};

/// Simple timer intended for profiling
/// Lazily inits. Returns `Duration`s or raw string (in ms)
/// Use Option as a safeguard because apparently previous Rust versions panicked
/// when current time was earlier than self.

pub struct Timer {
    start_time: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self { start_time: Instant::now() }
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn formatted_elapsed(&self) -> String {
        self.elapsed().as_millis().to_string()
    }
}
