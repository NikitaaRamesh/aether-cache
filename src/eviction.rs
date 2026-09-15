use std::collections::VecDeque;

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use parking_lot::Mutex;

/// A bounded, lock-free buffer of cache keys observed by concurrent readers.
///
/// Reads enqueue lightweight [`Bytes`] handles instead of taking the eviction
/// policy's write lock for every access. A maintenance path can drain a batch
/// and update eviction metadata under one lock acquisition, amortizing lock
/// contention across many reads. When the buffer is full, [`Self::push_key`]
/// returns ownership of the unqueued key so the caller can trigger that drain.
pub struct ReadBuffer {
    pub(crate) queue: ArrayQueue<Bytes>,
}

impl ReadBuffer {
    /// Creates an empty read buffer with a fixed, non-zero `capacity`.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero, as required by [`ArrayQueue`].
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
        }
    }

    /// Attempts to record a key without blocking or acquiring a mutex.
    ///
    /// Returns `Err(key)` when the bounded queue is full. No payload copy is
    /// performed: ownership of the [`Bytes`] handle is either transferred into
    /// the queue or returned directly to the caller.
    #[inline]
    pub fn push_key(&self, key: Bytes) -> Result<(), Bytes> {
        self.queue.push(key)
    }
}

/// Defines hooks used by cache eviction implementations.
pub trait EvictionPolicy {
    /// Records that `key` was accessed.
    fn on_read(&self, key: Bytes);

    /// Promotes `key` in policies that maintain an explicit access order.
    fn promote(&self, key: Bytes);

    /// Removes entries while holding the target shard's write guard.
    fn evict(
        &self,
        map: &mut parking_lot::RwLockWriteGuard<
            '_,
            hashbrown::HashMap<Bytes, crate::entry::CacheEntry>,
        >,
    );
}

/// An approximate least-frequently-used policy based on bounded sampling.
pub struct SampledLfu {
    target_capacity: usize,
}

impl SampledLfu {
    /// Creates a policy that trims a shard to `target_capacity` entries.
    pub fn new(target_capacity: usize) -> Self {
        Self { target_capacity }
    }
}

impl EvictionPolicy for SampledLfu {
    fn on_read(&self, _key: Bytes) {
        // LFU frequency is already tracked by CacheEntry's atomic counter.
    }

    fn promote(&self, _key: Bytes) {
        // LFU does not maintain chronological access order.
    }

    fn evict(
        &self,
        map: &mut parking_lot::RwLockWriteGuard<
            '_,
            hashbrown::HashMap<Bytes, crate::entry::CacheEntry>,
        >,
    ) {
        if map.len() <= self.target_capacity {
            return;
        }

        while map.len() > self.target_capacity {
            // hashbrown's randomized iteration makes `.take(5)` a constant-size
            // approximate sample, mirroring Redis `maxmemory-policy allkeys-lfu`.
            let victim = map
                .iter()
                .take(5)
                .min_by_key(|(_, entry)| entry.access_count())
                .map(|(key, _)| key.clone());

            let Some(victim) = victim else {
                break;
            };
            map.remove(&victim);
        }
    }
}

/// A strict least-recently-used policy with explicit promotion ordering.
///
/// Promotions are intended to be applied in batches drained from [`ReadBuffer`].
/// The internal `parking_lot` lock provides the interior mutability required by
/// [`EvictionPolicy::evict`], whose shared-reference API supports concurrent
/// policy implementations without using a standard-library mutex.
pub struct StrictLru {
    target_capacity: usize,
    access_order: Mutex<VecDeque<Bytes>>,
}

impl StrictLru {
    /// Creates an empty LRU policy that trims maps to `target_capacity` entries.
    pub fn new(target_capacity: usize) -> Self {
        Self {
            target_capacity,
            access_order: Mutex::new(VecDeque::new()),
        }
    }

    /// Appends `key` as the most recently accessed entry.
    pub fn promote(&self, key: Bytes) {
        self.access_order.lock().push_back(key);
    }
}

impl EvictionPolicy for StrictLru {
    fn on_read(&self, _key: Bytes) {
        // ReadBuffer batches promotions so cache reads do not contend here.
    }

    fn promote(&self, key: Bytes) {
        StrictLru::promote(self, key);
    }

    fn evict(
        &self,
        map: &mut parking_lot::RwLockWriteGuard<
            '_,
            hashbrown::HashMap<Bytes, crate::entry::CacheEntry>,
        >,
    ) {
        let mut access_order = self.access_order.lock();

        while map.len() > self.target_capacity {
            let Some(victim) = access_order.pop_front() else {
                break;
            };

            if map.contains_key(&victim) {
                map.remove(&victim);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::ReadBuffer;

    #[test]
    fn accepts_a_key_when_capacity_is_available() {
        let buffer = ReadBuffer::new(1);
        let key = Bytes::from_static(b"key");

        assert_eq!(buffer.push_key(key.clone()), Ok(()));
        assert_eq!(buffer.queue.pop(), Some(key));
    }

    #[test]
    fn returns_the_key_when_the_buffer_is_full() {
        let buffer = ReadBuffer::new(1);
        let queued = Bytes::from_static(b"queued");
        let rejected = Bytes::from_static(b"rejected");

        assert_eq!(buffer.push_key(queued), Ok(()));
        assert_eq!(buffer.push_key(rejected.clone()), Err(rejected));
    }
}

#[cfg(test)]
mod sampled_lfu_tests {
    use bytes::Bytes;
    use hashbrown::HashMap;
    use parking_lot::RwLock;

    use super::{EvictionPolicy, SampledLfu};
    use crate::entry::CacheEntry;

    fn entry(key: &'static [u8]) -> CacheEntry {
        let key = Bytes::from_static(key);
        CacheEntry::new(key, Bytes::from_static(b"value"), None)
    }

    #[test]
    fn evicts_the_least_frequently_used_entry_from_five_candidates() {
        let lock = RwLock::new(HashMap::new());
        let cold_key = Bytes::from_static(b"cold");

        {
            let mut map = lock.write();
            map.insert(cold_key.clone(), entry(b"cold"));
            for key in [b"hot-1", b"hot-2", b"hot-3", b"hot-4"] {
                let key = Bytes::from_static(key);
                let hot_entry = CacheEntry::new(key.clone(), Bytes::from_static(b"value"), None);
                hot_entry.record_access();
                map.insert(key, hot_entry);
            }

            SampledLfu::new(4).evict(&mut map);
        }

        let map = lock.read();
        assert_eq!(map.len(), 4);
        assert!(!map.contains_key(&cold_key));
    }

    #[test]
    fn evicts_until_the_target_capacity_is_reached() {
        let lock = RwLock::new(HashMap::new());
        let mut map = lock.write();
        for index in 0..12 {
            let key = Bytes::from(index.to_string());
            map.insert(key.clone(), CacheEntry::new(key, Bytes::new(), None));
        }

        SampledLfu::new(3).evict(&mut map);

        assert_eq!(map.len(), 3);
    }
}

#[cfg(test)]
mod strict_lru_tests {
    use bytes::Bytes;
    use hashbrown::HashMap;
    use parking_lot::RwLock;

    use super::{EvictionPolicy, StrictLru};
    use crate::entry::CacheEntry;

    fn insert(map: &mut HashMap<Bytes, CacheEntry>, key: Bytes) {
        map.insert(key.clone(), CacheEntry::new(key, Bytes::new(), None));
    }

    #[test]
    fn evicts_promoted_keys_in_chronological_order() {
        let lock = RwLock::new(HashMap::new());
        let policy = StrictLru::new(1);
        let oldest = Bytes::from_static(b"oldest");
        let middle = Bytes::from_static(b"middle");
        let newest = Bytes::from_static(b"newest");

        policy.promote(oldest.clone());
        policy.promote(middle.clone());
        policy.promote(newest.clone());

        let mut map = lock.write();
        insert(&mut map, oldest.clone());
        insert(&mut map, middle.clone());
        insert(&mut map, newest.clone());
        policy.evict(&mut map);

        assert_eq!(map.len(), 1);
        assert!(!map.contains_key(&oldest));
        assert!(!map.contains_key(&middle));
        assert!(map.contains_key(&newest));
    }

    #[test]
    fn skips_stale_access_order_entries() {
        let lock = RwLock::new(HashMap::new());
        let policy = StrictLru::new(1);
        let stale = Bytes::from_static(b"stale");
        let victim = Bytes::from_static(b"victim");
        let survivor = Bytes::from_static(b"survivor");

        policy.promote(stale);
        policy.promote(victim.clone());

        let mut map = lock.write();
        insert(&mut map, victim.clone());
        insert(&mut map, survivor.clone());
        policy.evict(&mut map);

        assert_eq!(map.len(), 1);
        assert!(!map.contains_key(&victim));
        assert!(map.contains_key(&survivor));
    }
}
