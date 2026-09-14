use bytes::{Buf, BytesMut};

use crate::frame::Frame;

const MAX_NESTING_DEPTH: usize = 128;

/// Parses one RESP frame from the front of `buffer`.
///
/// Complete frames are consumed. Incomplete frames return `Ok(None)` without
/// modifying `buffer`, allowing the TCP read loop to append more data.
pub fn parse_frame(buffer: &mut BytesMut) -> Result<Option<Frame>, String> {
    let Some(frame_len) = validate_frame(buffer, 0, 0)? else {
        return Ok(None);
    };

    let initial_len = buffer.len();
    let frame = consume_frame(buffer, 0)?;
    debug_assert_eq!(initial_len - buffer.len(), frame_len);

    Ok(Some(frame))
}

fn validate_frame(input: &[u8], offset: usize, depth: usize) -> Result<Option<usize>, String> {
    if depth > MAX_NESTING_DEPTH {
        return Err("RESP array nesting limit exceeded".to_owned());
    }

    let Some(&prefix) = input.get(offset) else {
        return Ok(None);
    };

    let Some((length, payload_offset)) = parse_length(input, offset)? else {
        return Ok(None);
    };

    match prefix {
        b'$' => validate_bulk_string(input, offset, payload_offset, length),
        b'*' => validate_array(input, offset, payload_offset, length, depth),
        _ => Err(format!("unsupported RESP type byte: 0x{prefix:02x}")),
    }
}

fn validate_bulk_string(
    input: &[u8],
    frame_offset: usize,
    payload_offset: usize,
    length: i64,
) -> Result<Option<usize>, String> {
    if length == -1 {
        return Ok(Some(payload_offset - frame_offset));
    }

    let length = non_negative_length(length, "bulk string")?;
    let Some(payload_end) = payload_offset.checked_add(length) else {
        return Err("bulk string length overflow".to_owned());
    };
    let Some(frame_end) = payload_end.checked_add(2) else {
        return Err("bulk string length overflow".to_owned());
    };

    if input.len() < frame_end {
        return Ok(None);
    }
    if input[payload_end..frame_end] != *b"\r\n" {
        return Err("bulk string payload is not terminated by CRLF".to_owned());
    }

    Ok(Some(frame_end - frame_offset))
}

fn validate_array(
    input: &[u8],
    frame_offset: usize,
    mut element_offset: usize,
    length: i64,
    depth: usize,
) -> Result<Option<usize>, String> {
    if length == -1 {
        return Ok(Some(element_offset - frame_offset));
    }

    let length = non_negative_length(length, "array")?;
    for _ in 0..length {
        let Some(element_len) = validate_frame(input, element_offset, depth + 1)? else {
            return Ok(None);
        };
        element_offset = element_offset
            .checked_add(element_len)
            .ok_or_else(|| "array length overflow".to_owned())?;
    }

    Ok(Some(element_offset - frame_offset))
}

fn parse_length(input: &[u8], offset: usize) -> Result<Option<(i64, usize)>, String> {
    let header_start = offset
        .checked_add(1)
        .ok_or_else(|| "frame offset overflow".to_owned())?;
    let Some(relative_end) = find_crlf(&input[header_start..]) else {
        return Ok(None);
    };
    let header_end = header_start + relative_end;
    let text = std::str::from_utf8(&input[header_start..header_end])
        .map_err(|_| "RESP length is not valid ASCII".to_owned())?;
    let length = text
        .parse::<i64>()
        .map_err(|_| "RESP length is not a valid integer".to_owned())?;

    Ok(Some((length, header_end + 2)))
}

fn non_negative_length(length: i64, kind: &str) -> Result<usize, String> {
    usize::try_from(length).map_err(|_| format!("invalid {kind} length: {length}"))
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|window| window == b"\r\n")
}

fn consume_frame(buffer: &mut BytesMut, depth: usize) -> Result<Frame, String> {
    if depth > MAX_NESTING_DEPTH {
        return Err("RESP array nesting limit exceeded".to_owned());
    }

    let prefix = buffer[0];
    buffer.advance(1);
    let header_end = find_crlf(buffer).ok_or_else(|| "missing RESP length CRLF".to_owned())?;
    let length = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| "RESP length is not valid ASCII".to_owned())?
        .parse::<i64>()
        .map_err(|_| "RESP length is not a valid integer".to_owned())?;
    buffer.advance(header_end + 2);

    match prefix {
        b'$' if length == -1 => Ok(Frame::Null),
        b'$' => {
            let length = non_negative_length(length, "bulk string")?;
            let payload = buffer.split_to(length).freeze();
            buffer.advance(2);
            Ok(Frame::BulkString(payload))
        }
        b'*' if length == -1 => Ok(Frame::Null),
        b'*' => {
            let length = non_negative_length(length, "array")?;
            let mut elements = Vec::with_capacity(length);
            for _ in 0..length {
                elements.push(consume_frame(buffer, depth + 1)?);
            }
            Ok(Frame::Array(elements))
        }
        _ => Err(format!("unsupported RESP type byte: 0x{prefix:02x}")),
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::parse_frame;
    use crate::frame::Frame;

    #[test]
    fn parses_bulk_string_without_copying_payload() {
        let mut buffer = BytesMut::from(&b"$5\r\nhello\r\ntrailing"[..]);
        let payload_address = unsafe { buffer.as_ptr().add(4) };

        let frame = parse_frame(&mut buffer).unwrap();

        let Frame::BulkString(payload) = frame.unwrap() else {
            panic!("expected bulk string");
        };
        assert_eq!(payload, Bytes::from_static(b"hello"));
        assert_eq!(payload.as_ptr(), payload_address);
        assert_eq!(buffer, &b"trailing"[..]);
    }

    #[test]
    fn incomplete_bulk_string_does_not_modify_buffer() {
        let mut buffer = BytesMut::from(&b"$5\r\nhel"[..]);
        let original = buffer.clone();

        assert_eq!(parse_frame(&mut buffer), Ok(None));
        assert_eq!(buffer, original);
    }

    #[test]
    fn parses_nested_array() {
        let mut buffer = BytesMut::from(&b"*3\r\n$3\r\nGET\r\n$3\r\nkey\r\n$-1\r\n"[..]);

        assert_eq!(
            parse_frame(&mut buffer),
            Ok(Some(Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(Bytes::from_static(b"key")),
                Frame::Null,
            ])))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn incomplete_array_does_not_modify_buffer() {
        let mut buffer = BytesMut::from(&b"*2\r\n$3\r\none\r\n$3\r\ntw"[..]);
        let original = buffer.clone();

        assert_eq!(parse_frame(&mut buffer), Ok(None));
        assert_eq!(buffer, original);
    }

    #[test]
    fn rejects_missing_bulk_payload_crlf_without_modifying_buffer() {
        let mut buffer = BytesMut::from(&b"$3\r\nabcXX"[..]);
        let original = buffer.clone();

        assert!(parse_frame(&mut buffer).is_err());
        assert_eq!(buffer, original);
    }
}
