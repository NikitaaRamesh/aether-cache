use std::{io, sync::Arc};

use aether_cache::{
    entry::CacheEntry,
    server::CacheServer,
    shard::{SHARD_COUNT, ShardedCache},
};
use bytes::Bytes;
use hashbrown::DefaultHashBuilder;

const SERVER_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> io::Result<()> {
    let cache = Arc::new(
        ShardedCache::<Bytes, CacheEntry, DefaultHashBuilder>::with_hasher(
            SHARD_COUNT,
            DefaultHashBuilder::default(),
        ),
    );
    let server = CacheServer::new(Arc::clone(&cache), SERVER_PORT);

    println!("aether-cache server started on port {SERVER_PORT}");

    if let Err(error) = server.run().await {
        eprintln!("aether-cache server stopped: {error}");
        return Err(error);
    }

    Ok(())
}
