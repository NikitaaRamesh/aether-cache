use std::{
    sync::atomic::AtomicU32,
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
        let expires_at = ttl_ms.and_then(|ttl| {
            let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
            let now_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);

            Some(now_ms.saturating_add(ttl))
        });

        Self {
            key,
            value,
            expires_at,
            access_counter: AtomicU32::new(0),
        }
    }
}
