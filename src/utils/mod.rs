use std::time::{SystemTime, UNIX_EPOCH};

pub mod buffer;
pub mod settings;

pub fn now_seconds() -> u64 {
    let duration_since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards!"); // Handles rare system clock shifts

    duration_since_epoch.as_secs()
}