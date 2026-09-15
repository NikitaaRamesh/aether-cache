use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;

/// A cache record containing immutable, shared byte buffers and entry metadata.
///
/// `Bytes` allows the key and value storage to be shared across threads without
/// copying their payloads. Moving a `Bytes` handle into this entry transfers the
/// handle; cloning one only increments the reference count of the shared buffer.
#[repr(C)]
pub struct CacheEntry {
    key: Bytes,
    value: Bytes,
    expires_at: Option<u64>,
    access_counter: AtomicU32,
}

impl CacheEntry {
    /// Creates an entry whose optional TTL and expiration are measured in
    /// milliseconds.
    ///
    /// If the system clock is unavailable before the Unix epoch, a supplied TTL
    /// is treated as non-expiring rather than risking immediate data eviction.
    pub fn new(key: Bytes, value: Bytes, ttl_ms: Option<u64>) -> Self {
        let expires_at =
            ttl_ms.and_then(|ttl| current_time_ms().map(|now| now.saturating_add(ttl)));

        Self {
            key,
            value,
            expires_at,
            access_counter: AtomicU32::new(0),
        }
    }

    /// Returns the immutable value payload without cloning its backing bytes.
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Records an access without taking the containing shard's write lock.
    pub fn record_access(&self) {
        let _ = self
            .access_counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_add(1)
            });
    }

    /// Returns whether this entry has reached its absolute expiration time.
    pub fn is_expired(&self) -> bool {
        match (self.expires_at, current_time_ms()) {
            (Some(expires_at), Some(now)) => now >= expires_at,
            _ => false,
        }
    }
}

fn current_time_ms() -> Option<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    Some(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}
