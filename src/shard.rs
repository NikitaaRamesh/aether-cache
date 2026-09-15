use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
};

use hashbrown::HashMap;
use parking_lot::RwLock;

pub use crate::eviction::SampledLfu;
use crate::eviction::{EvictionPolicy, ReadBuffer};

/// The fixed shard count. It must remain a power of two for bitmask routing.
pub const SHARD_COUNT: usize = 64;

/// An independently locked cache partition.
///
/// Cache-line alignment prevents adjacent shard locks from sharing a cache line
/// and invalidating each other's CPU caches under concurrent writes.
#[repr(align(64))]
pub struct CacheShard<K, V, P> {
    pub map: RwLock<HashMap<K, V>>,
    pub policy: P,
    pub read_buffer: ReadBuffer,
}

impl<K, V, P> CacheShard<K, V, P> {
    fn new(policy: P) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            policy,
            read_buffer: ReadBuffer::new(128),
        }
    }
}

/// A fixed-size collection of independently locked cache partitions.
///
/// Unlike a Python dictionary protected by one lock, unrelated keys can be read
/// or written concurrently when they route to different shards.
pub struct ShardedCache<K, V, P = SampledLfu, S = RandomState> {
    shards: Box<[CacheShard<K, V, P>]>,
    hash_builder: S,
}

impl<K, V, P, S> ShardedCache<K, V, P, S>
where
    P: EvictionPolicy,
    S: BuildHasher,
{
    /// Creates an empty cache with a power-of-two shard count and hash builder.
    ///
    /// # Panics
    ///
    /// Panics if `shard_count` is zero or is not a power of two.
    pub fn new(
        shard_count: usize,
        total_capacity: usize,
        policy_factory: impl Fn(usize) -> P,
        hash_builder: S,
    ) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard count must be a non-zero power of two"
        );
        let capacity_per_shard = total_capacity / shard_count;

        let mut shards = Vec::with_capacity(shard_count);
        shards.resize_with(shard_count, || {
            CacheShard::new(policy_factory(capacity_per_shard))
        });

        Self {
            shards: shards.into_boxed_slice(),
            hash_builder,
        }
    }

    /// Hashes `key` and returns its shard using power-of-two bitmask routing.
    pub fn get_shard<Q>(&self, key: &Q) -> &CacheShard<K, V, P>
    where
        Q: Hash + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        let index = (hash as usize) & (self.shards.len() - 1);

        &self.shards[index]
    }
}

impl<K, V> Default for ShardedCache<K, V> {
    fn default() -> Self {
        Self::new(SHARD_COUNT, 100_000, SampledLfu::new, RandomState::new())
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::align_of, thread};

    use super::{CacheShard, SHARD_COUNT, SampledLfu, ShardedCache};

    #[test]
    fn shards_are_cache_line_aligned() {
        assert!(align_of::<CacheShard<u64, u64, SampledLfu>>() >= 64);
    }

    #[test]
    fn creates_the_fixed_number_of_shards() {
        let cache = ShardedCache::<u64, u64>::default();

        assert_eq!(cache.shards.len(), SHARD_COUNT);
    }

    #[test]
    fn equal_keys_route_to_the_same_shard() {
        let cache = ShardedCache::<String, u64>::default();

        assert!(std::ptr::eq(
            cache.get_shard("consistent-key"),
            cache.get_shard("consistent-key"),
        ));
    }

    #[test]
    fn concurrent_inserts_preserve_every_entry() {
        const THREAD_COUNT: usize = 16;
        const INSERTS_PER_THREAD: usize = 10_000;

        let cache = ShardedCache::<String, String>::default();

        thread::scope(|scope| {
            let mut handles = Vec::with_capacity(THREAD_COUNT);

            for thread_id in 0..THREAD_COUNT {
                let cache = &cache;
                handles.push(scope.spawn(move || {
                    for item_id in 0..INSERTS_PER_THREAD {
                        let key = format!("{thread_id}:{item_id}");
                        let value = format!("value:{thread_id}:{item_id}");

                        cache.get_shard(&key).map.write().insert(key, value);
                    }
                }));
            }

            for handle in handles {
                handle.join().expect("cache worker thread panicked");
            }
        });

        let item_count: usize = cache
            .shards
            .iter()
            .map(|shard| shard.map.read().len())
            .sum();

        assert_eq!(item_count, THREAD_COUNT * INSERTS_PER_THREAD);
    }
}
