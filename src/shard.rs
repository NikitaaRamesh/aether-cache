use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
};

use hashbrown::HashMap;
use parking_lot::RwLock;

/// The fixed shard count. It must remain a power of two for bitmask routing.
pub const SHARD_COUNT: usize = 64;

/// An independently locked cache partition.
///
/// Cache-line alignment prevents adjacent shard locks from sharing a cache line
/// and invalidating each other's CPU caches under concurrent writes.
#[repr(align(64))]
pub struct CacheShard<K, V> {
    pub map: RwLock<HashMap<K, V>>,
}

impl<K, V> CacheShard<K, V> {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }
}

/// A fixed-size collection of independently locked cache partitions.
///
/// Unlike a Python dictionary protected by one lock, unrelated keys can be read
/// or written concurrently when they route to different shards.
pub struct ShardedCache<K, V, S = RandomState> {
    shards: Box<[CacheShard<K, V>]>,
    hash_builder: S,
}

impl<K, V> ShardedCache<K, V, RandomState> {
    /// Creates an empty cache containing [`SHARD_COUNT`] shards.
    pub fn new() -> Self {
        Self::with_hasher(SHARD_COUNT, RandomState::new())
    }
}

impl<K, V, S> ShardedCache<K, V, S>
where
    S: BuildHasher,
{
    /// Creates an empty cache with a power-of-two shard count and hash builder.
    ///
    /// # Panics
    ///
    /// Panics if `shard_count` is zero or is not a power of two.
    pub fn with_hasher(shard_count: usize, hash_builder: S) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "shard count must be a non-zero power of two"
        );

        let mut shards = Vec::with_capacity(shard_count);
        shards.resize_with(shard_count, CacheShard::new);

        Self {
            shards: shards.into_boxed_slice(),
            hash_builder,
        }
    }

    /// Hashes `key` and returns its shard using power-of-two bitmask routing.
    pub fn get_shard<Q>(&self, key: &Q) -> &CacheShard<K, V>
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::align_of, thread};

    use super::{CacheShard, SHARD_COUNT, ShardedCache};

    #[test]
    fn shards_are_cache_line_aligned() {
        assert_eq!(align_of::<CacheShard<u64, u64>>(), 64);
    }

    #[test]
    fn creates_the_fixed_number_of_shards() {
        let cache = ShardedCache::<u64, u64>::new();

        assert_eq!(cache.shards.len(), SHARD_COUNT);
    }

    #[test]
    fn equal_keys_route_to_the_same_shard() {
        let cache = ShardedCache::<String, u64>::new();

        assert!(std::ptr::eq(
            cache.get_shard("consistent-key"),
            cache.get_shard("consistent-key"),
        ));
    }

    #[test]
    fn concurrent_inserts_preserve_every_entry() {
        const THREAD_COUNT: usize = 16;
        const INSERTS_PER_THREAD: usize = 10_000;

        let cache = ShardedCache::<String, String>::new();

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
