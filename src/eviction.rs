use bytes::Bytes;
use crossbeam_queue::ArrayQueue;

/// A bounded, lock-free buffer of cache keys observed by concurrent readers.
///
/// Reads enqueue lightweight [`Bytes`] handles instead of taking the eviction
/// policy's write lock for every access. A maintenance path can drain a batch
/// and update eviction metadata under one lock acquisition, amortizing lock
/// contention across many reads. When the buffer is full, [`Self::push_key`]
/// returns ownership of the unqueued key so the caller can trigger that drain.
pub struct ReadBuffer {
    queue: ArrayQueue<Bytes>,
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
