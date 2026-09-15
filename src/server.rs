use std::{hash::BuildHasher, io, sync::Arc};

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{entry::CacheEntry, frame::Frame, parse::parse_frame, shard::ShardedCache};

const INITIAL_READ_CAPACITY: usize = 8 * 1024;

/// An asynchronous TCP front end for a shared sharded cache.
pub struct CacheServer<S> {
    cache: Arc<ShardedCache<Bytes, CacheEntry, S>>,
    port: u16,
}

impl<S> CacheServer<S>
where
    S: BuildHasher + Send + Sync + 'static,
{
    /// Creates a server that owns a shared cache handle and listens on `port`.
    pub fn new(cache: Arc<ShardedCache<Bytes, CacheEntry, S>>, port: u16) -> Self {
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
                if let Err(error) = read_connection(stream, cache).await {
                    eprintln!("cache connection terminated with an I/O error: {error}");
                }
            });
        }
    }
}

async fn read_connection<S>(
    mut stream: TcpStream,
    cache: Arc<ShardedCache<Bytes, CacheEntry, S>>,
) -> io::Result<()>
where
    S: BuildHasher,
{
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

        while let Some(frame) = parse_frame(&mut buffer).map_err(invalid_data)? {
            execute_frame(&mut stream, &cache, frame).await?;
        }
    }
}

async fn execute_frame<S>(
    stream: &mut TcpStream,
    cache: &ShardedCache<Bytes, CacheEntry, S>,
    frame: Frame,
) -> io::Result<()>
where
    S: BuildHasher,
{
    let Frame::Array(elements) = frame else {
        return write_unknown_command(stream).await;
    };
    let mut elements = elements.into_iter();
    let Some(Frame::BulkString(command)) = elements.next() else {
        return write_unknown_command(stream).await;
    };

    if command.eq_ignore_ascii_case(b"SET") {
        let (Some(Frame::BulkString(key)), Some(Frame::BulkString(value)), None) =
            (elements.next(), elements.next(), elements.next())
        else {
            return write_unknown_command(stream).await;
        };
        let entry = CacheEntry::new(key.clone(), value, None);

        {
            let shard = cache.get_shard(&key);
            shard.map.write().insert(key, entry);
        }

        stream.write_all(b"+OK\r\n").await
    } else if command.eq_ignore_ascii_case(b"GET") {
        let (Some(Frame::BulkString(key)), None) = (elements.next(), elements.next()) else {
            return write_unknown_command(stream).await;
        };

        let payload = {
            let shard = cache.get_shard(&key);
            let map = shard.map.read();
            map.get(&key).and_then(|entry| {
                if entry.is_expired() {
                    None
                } else {
                    entry.record_access();
                    Some(entry.value().clone())
                }
            })
        };

        if let Some(payload) = payload {
            let header = format!("${}\r\n", payload.len());
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(&payload).await?;
            stream.write_all(b"\r\n").await
        } else {
            stream.write_all(b"$-1\r\n").await
        }
    } else {
        write_unknown_command(stream).await
    }
}

async fn write_unknown_command(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b"-ERR unknown command\r\n").await
}

fn invalid_data(error: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use hashbrown::DefaultHashBuilder;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::read_connection;
    use crate::{entry::CacheEntry, shard::ShardedCache};

    #[tokio::test]
    async fn executes_pipelined_set_and_get() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let cache = Arc::new(ShardedCache::<Bytes, CacheEntry, DefaultHashBuilder>::new(
            64,
            DefaultHashBuilder::default(),
        ));
        let server_cache = Arc::clone(&cache);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            read_connection(stream, server_cache).await.unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();

        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            )
            .await
            .unwrap();

        let mut response = [0; 16];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"+OK\r\n$5\r\nvalue\r\n");

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }
}
