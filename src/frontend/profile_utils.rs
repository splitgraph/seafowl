use std::time::{Duration, Instant};

// Simple timer intended for profiling
// Lazily inits. Returns `Duration`s or raw string (in ms)
// Use Option as a safeguard because apparently previous Rust versions panicked
// when current time was earlier than self.

pub struct Timer {
    start_time: Option<Instant>,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            start_time: None
        }
    }

    pub fn start_timer(&mut self) {
        self.start_time = Some(Instant::now())
    }

    pub fn elapsed(&self) -> Option<Duration> {
        self.start_time.map(|start| start.elapsed())
    }

    pub fn formatted_elapsed(&self) -> String {
        self.elapsed().map(|duration| {
            let millis = duration.as_millis();
            format!("{}", millis)
        })
        .unwrap_or_else(|| String::new())
    }
}
