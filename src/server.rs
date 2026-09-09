use std::{io, sync::Arc};

use bytes::BytesMut;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};

use crate::shard::ShardedCache;

const INITIAL_READ_CAPACITY: usize = 8 * 1024;

/// An asynchronous TCP front end for a shared sharded cache.
pub struct CacheServer<K, V> {
    cache: Arc<ShardedCache<K, V>>,
    port: u16,
}

impl<K, V> CacheServer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Creates a server that owns a shared cache handle and listens on `port`.
    pub fn new(cache: Arc<ShardedCache<K, V>>, port: u16) -> Self {
        Self { cache, port }
    }

    /// Accepts TCP connections and dispatches each one onto the Tokio runtime.
    ///
    /// This method runs until the listener fails or its task is cancelled.
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(("0.0.0.0", self.port)).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let cache = Arc::clone(&self.cache);

            tokio::spawn(async move {
                // Retain a cache handle for the lifetime of this connection.
                // Protocol dispatch will use it once command parsing is added.
                let _cache = cache;

                if let Err(error) = read_connection(stream).await {
                    eprintln!("cache connection terminated with an I/O error: {error}");
                }
            });
        }
    }
}

async fn read_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut buffer = BytesMut::with_capacity(INITIAL_READ_CAPACITY);

    loop {
        // `BytesMut` exposes spare capacity directly to Tokio, avoiding an
        // intermediate read allocation. Future parsers can split complete
        // frames and freeze them into immutable `Bytes` handles without copying
        // payload data—unlike repeatedly concatenating Python `bytes` objects.
        let bytes_read = stream.read_buf(&mut buffer).await?;

        if bytes_read == 0 {
            return Ok(());
        }

        println!("received {bytes_read} bytes");

        // This scaffold consumes every received byte. A protocol parser will
        // instead advance only complete frames and retain any partial frame.
        buffer.clear();
    }
}
