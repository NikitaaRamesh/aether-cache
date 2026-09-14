use bytes::Bytes;

/// A value encoded by the Redis Serialization Protocol (RESP).
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    /// Binary payload backed by shared storage.
    ///
    /// `Bytes` lets a parser split and freeze data from the TCP read buffer
    /// without copying the payload, preserving zero-copy stream semantics.
    BulkString(Bytes),
    Array(Vec<Frame>),
    Null,
}
