use super::fanout_error::INVALID_MESSAGE_ERROR;
use crate::common::encoding::{
    try_read_byte_slice, try_read_signed_varint, try_read_u64_le, try_read_uvarint,
    write_byte_slice, write_signed_varint, write_u64_le, write_uvarint,
};
use crate::fanout::{FanoutError, FanoutResult};

pub const FANOUT_MESSAGE_VERSION: u16 = 1;
pub const FANOUT_MESSAGE_MARKER: u32 = 0xBADCAB;

/// Envelope feature bits this node supports (`required_features` in the
/// message header). A message demanding bits outside this mask is rejected at
/// intake with [`FanoutError::unsupported_features`] instead of being silently
/// mis-processed. No bits are defined yet: allocate one only for a future
/// envelope/payload change the receiver cannot ignore (e.g. payload
/// compression) — see docs/fanout-compatibility-handshake.md.
pub(super) const SUPPORTED_MESSAGE_FEATURES: u16 = 0;

/// True when `required_features` demands a feature outside
/// [`SUPPORTED_MESSAGE_FEATURES`].
pub(super) fn has_unsupported_features(required_features: u16) -> bool {
    required_features & !SUPPORTED_MESSAGE_FEATURES != 0
}

const HEADER_SIZE: usize = size_of::<u32>();
const MAX_USERNAME_LEN: usize = 1024;
/// Registered fanout handler names are short internal identifiers (e.g. "mget",
/// "queryindex"); this cap bounds the allocation for the handler field while
/// leaving generous headroom.
const MAX_HANDLER_LEN: usize = 128;

/// Reads a length-prefixed UTF-8 string from `buf`, enforcing `max_len` on the
/// raw byte length BEFORE allocating, so an oversized length from a malicious or
/// buggy peer cannot force a large allocation that is only rejected afterwards.
///
/// [`try_read_byte_slice`] merely borrows from the already-received buffer (and
/// errors if the claimed length exceeds the remaining data), so the length cap is
/// the only gate before the caller allocates. Returns a borrowed `&str` and leaves
/// the empty-vs-present decision (and any allocation) to the caller.
fn read_bounded_str<'a>(buf: &mut &'a [u8], max_len: usize) -> FanoutResult<&'a str> {
    let bytes =
        try_read_byte_slice(buf).map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?;
    if bytes.len() > max_len {
        return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
    }
    std::str::from_utf8(bytes).map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))
}

/// Header for messages exchanged between cluster nodes.
#[derive(Debug, Clone)]
pub(super) struct FanoutMessageHeader {
    pub version: u16,
    /// Unique ID for this request, used to match responses.
    pub request_id: u64,
    /// The database to use for this request.
    pub db: i32,
    /// The name of the handler to process this message.
    pub handler: String,
    /// ACL user propagated by requester for shard-side enforcement.
    /// `None` means no user context should be enforced.
    pub user: Option<String>,
    /// Hash of the sender's cluster map (hash of all shard fingerprints) at the
    /// time the request was generated. `0` means "no fingerprint", which
    /// disables the receiver-side topology check.
    pub cluster_fingerprint: u64,
    /// Envelope feature bits the receiver MUST understand to process this
    /// message; validated at intake against [`SUPPORTED_MESSAGE_FEATURES`].
    /// `0` (all senders today, and every pre-repurposing peer — this slot was
    /// formerly `reserved`, written as 0 and ignored) means no requirements.
    pub required_features: u16,
}

impl FanoutMessageHeader {
    #[cfg(test)]
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        write_message_header(
            buf,
            self.version,
            self.request_id,
            self.db,
            &self.handler,
            self.user.as_deref(),
            self.cluster_fingerprint,
            self.required_features,
        );
    }

    /// Deserializes a MessageHeader from the beginning of the buffer.
    /// Returns the deserialized header and the payload buffer.
    pub fn deserialize(buf: &[u8]) -> FanoutResult<(Self, &[u8])> {
        // Read and validate the marker
        let mut buf = skip_marker(buf)?;

        let version =
            read_u16_le(&mut buf).map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?;

        if version != FANOUT_MESSAGE_VERSION {
            return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
        }

        // Decode request_id as uvarint
        let request_id = read_uvarint(&mut buf)?;

        // `db` is written from an `i32`, so anything outside that range is a
        // malformed or hostile encoding. Reject it rather than truncating: an
        // `as i32` cast would silently fold e.g. 2^32 down to a *valid-looking*
        // db 0 and run the request against the wrong keyspace.
        let db_raw = try_read_signed_varint(&mut buf)
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?;
        let db =
            i32::try_from(db_raw).map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?;

        // Read the handler name, capping the raw length before allocating.
        let handler = read_bounded_str(&mut buf, MAX_HANDLER_LEN)?.to_owned();

        // Read the ACL user, capping the raw length before allocating (empty = no user).
        let user_str = read_bounded_str(&mut buf, MAX_USERNAME_LEN)?;
        let user = if user_str.is_empty() {
            None
        } else {
            Some(user_str.to_owned())
        };

        // Cluster-map fingerprint, fixed 8-byte little-endian.
        let cluster_fingerprint = try_read_u64_le(&mut buf)
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?;

        // Read required_features as a little-endian u16
        let required_features = read_u16_le(&mut buf)?;

        Ok((
            FanoutMessageHeader {
                version,
                request_id,
                handler,
                user,
                db,
                cluster_fingerprint,
                required_features,
            },
            buf,
        ))
    }
}

/// Write a message header directly to `buf` without constructing a [`FanoutMessageHeader`].
/// This avoids an unnecessary `String` allocation for the handler name in the outgoing
/// (serialization) path, where the handler is always available as a borrowed `&str`.
///
/// The wire layout here must stay in lockstep with [`FanoutMessageHeader::deserialize`].
#[allow(clippy::too_many_arguments)]
fn write_message_header(
    buf: &mut Vec<u8>,
    version: u16,
    request_id: u64,
    db: i32,
    handler: &str,
    user: Option<&str>,
    cluster_fingerprint: u64,
    required_features: u16,
) {
    // Start with the marker
    write_marker(buf);

    // version is stored as a little-endian u16
    write_u16_le(buf, version);

    // Encode request_id as uvarint
    write_uvarint(buf, request_id);

    // Encode db as signed varint
    write_signed_varint(buf, db as i64);

    // Encode handler as a string
    write_byte_slice(buf, handler.as_bytes());

    // Encode user as a string (empty = no user).
    write_byte_slice(buf, user.unwrap_or_default().as_bytes());

    // Cluster-map fingerprint, fixed 8-byte little-endian.
    write_u64_le(buf, cluster_fingerprint);

    // Envelope feature bits the receiver must understand (0 = none).
    write_u16_le(buf, required_features);
}

fn read_uvarint(input: &mut &[u8]) -> FanoutResult<u64> {
    try_read_uvarint(input).map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))
}

fn write_u16_le(slice: &mut Vec<u8>, v: u16) {
    let bytes = v.to_le_bytes();
    slice.extend_from_slice(bytes.as_ref());
}

fn read_u16_le(input: &mut &[u8]) -> FanoutResult<u16> {
    if input.len() < 2 {
        return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
    }
    let (int_bytes, rest) = input.split_at(2);
    *input = rest;
    let val = u16::from_le_bytes(
        int_bytes
            .try_into()
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?,
    );
    Ok(val)
}

fn write_marker(slice: &mut Vec<u8>) {
    let bytes = FANOUT_MESSAGE_MARKER.to_le_bytes();
    slice.extend_from_slice(bytes.as_ref());
}

fn skip_marker(input: &[u8]) -> FanoutResult<&[u8]> {
    if input.len() < HEADER_SIZE {
        return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
    }
    let (int_bytes, rest) = input.split_at(HEADER_SIZE);
    let marker = u32::from_le_bytes(
        int_bytes
            .try_into()
            .map_err(|_| FanoutError::serialization(INVALID_MESSAGE_ERROR))?,
    );
    if marker != FANOUT_MESSAGE_MARKER {
        return Err(FanoutError::serialization(INVALID_MESSAGE_ERROR));
    }

    Ok(rest)
}

/// Represents a decoded fanout message with its header and payload.
pub(super) struct FanoutMessage<'a> {
    pub buf: &'a [u8],
    /// The version of the fanout message protocol.
    pub version: u16,
    /// Unique ID for this request, used to match responses.
    pub request_id: u64,
    /// The name of the handler to process this message.
    pub handler: String,
    /// ACL user propagated by requester for shard-side enforcement.
    pub user: Option<String>,
    /// The database to use for this request.
    pub db: i32,
    /// Sender's cluster-map fingerprint (0 if unavailable / v1 sender).
    pub cluster_fingerprint: u64,
    /// Envelope feature bits the receiver must understand; checked against
    /// [`SUPPORTED_MESSAGE_FEATURES`] at intake (0 = no requirements).
    pub required_features: u16,
}

impl<'a> FanoutMessage<'a> {
    /// Creates a new FanoutMessage by parsing the provided buffer.
    /// Returns an error if the buffer is invalid or cannot be parsed.
    ///
    /// # Arguments
    /// - `buf`: The byte slice containing the serialized request message.
    ///
    /// # Returns
    /// A Result containing the FanoutMessage or a FanoutError.
    ///
    /// ## Note
    /// An empty payload is possible because of protobuf encoding of default values.
    /// A prost struct with all fields at their default values (e.g., all numeric fields are 0,
    /// booleans are false, strings are "", etc.) serializes into an empty byte buffer.
    /// There is no data to write because everything is implicit.
    pub fn new(buf: &'a [u8]) -> FanoutResult<Self> {
        let (header, buf) = FanoutMessageHeader::deserialize(buf)?;
        let FanoutMessageHeader {
            request_id,
            db,
            handler,
            user,
            cluster_fingerprint,
            required_features,
            ..
        } = header;

        Ok(Self {
            buf,
            request_id,
            handler,
            user,
            db,
            cluster_fingerprint,
            required_features,
            version: header.version,
        })
    }
}

pub(super) fn serialize_request_message(
    dest: &mut Vec<u8>,
    request_id: u64,
    db: i32,
    handler: &str,
    user: Option<&str>,
    cluster_fingerprint: u64,
    serialized_request: &[u8],
) {
    write_message_header(
        dest,
        FANOUT_MESSAGE_VERSION,
        request_id,
        db,
        handler,
        user,
        cluster_fingerprint,
        0,
    );
    dest.extend_from_slice(serialized_request);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::ErrorKind;
    use proptest::prelude::*;

    /// The envelope feature gate: with no bits defined, any demanded bit is
    /// unsupported; 0 (all current senders and pre-repurposing peers, which
    /// wrote this slot as `reserved: 0`) passes. `serialize_request_message`
    /// must keep sending 0 until a feature bit is actually defined.
    #[test]
    fn test_required_features_gate() {
        assert!(!has_unsupported_features(0));
        assert!(!has_unsupported_features(SUPPORTED_MESSAGE_FEATURES));
        assert!(has_unsupported_features(1));
        assert!(has_unsupported_features(0x8000));
        assert!(has_unsupported_features(u16::MAX));

        let mut buf = Vec::new();
        serialize_request_message(&mut buf, 7, 0, "mrange", None, 0, &[]);
        let msg = FanoutMessage::new(&buf).unwrap();
        assert_eq!(msg.required_features, 0);
        assert!(!has_unsupported_features(msg.required_features));
    }

    /// A nonzero required_features round-trips the wire slot (formerly
    /// `reserved`) and is visible to intake via `FanoutMessage`.
    #[test]
    fn test_required_features_roundtrip_nonzero() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: 99,
            db: 2,
            handler: "mrange".to_string(),
            user: None,
            cluster_fingerprint: 0,
            required_features: 0b1010,
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let msg = FanoutMessage::new(&buf).unwrap();
        assert_eq!(msg.required_features, 0b1010);
        assert!(has_unsupported_features(msg.required_features));
    }

    #[test]
    fn test_cluster_message_header_serialize_deserialize_basic() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: 12345,
            db: 0,
            handler: "test_handler".to_string(),
            user: None,
            cluster_fingerprint: 0xABCD_1234_5678_9F00,
            required_features: 0,
        };

        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let (deserialized_header, remaining_buf) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(deserialized_header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(deserialized_header.request_id, 12345);
        assert_eq!(deserialized_header.db, 0);
        assert_eq!(deserialized_header.handler, "test_handler");
        assert_eq!(deserialized_header.user, None);
        assert_eq!(deserialized_header.required_features, 0);
        assert_eq!(
            deserialized_header.cluster_fingerprint,
            0xABCD_1234_5678_9F00
        );
        assert_eq!(remaining_buf.len(), 0);
    }

    #[test]
    fn test_cluster_message_header_serialize_deserialize_with_negative_db() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: u64::MAX,
            db: -15,
            handler: "negative_db_handler".to_string(),
            user: Some("alice".to_string()),
            cluster_fingerprint: u64::MAX,
            required_features: 42,
        };

        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let (deserialized_header, remaining_buf) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(deserialized_header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(deserialized_header.request_id, u64::MAX);
        assert_eq!(deserialized_header.db, -15);
        assert_eq!(deserialized_header.handler, "negative_db_handler");
        assert_eq!(deserialized_header.user.as_deref(), Some("alice"));
        assert_eq!(deserialized_header.cluster_fingerprint, u64::MAX);
        assert_eq!(deserialized_header.required_features, 42);
        assert_eq!(remaining_buf.len(), 0);
    }

    #[test]
    fn test_cluster_message_header_deserialize_with_remaining_data() {
        let header = FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: 999,
            db: 5,
            handler: "handler_with_extra".to_string(),
            user: Some("bob".to_string()),
            cluster_fingerprint: 7,
            required_features: 1,
        };

        let mut buf = Vec::new();
        header.serialize(&mut buf);
        buf.extend_from_slice(b"extra_data");

        let (deserialized_header, remaining_buf) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(deserialized_header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(deserialized_header.request_id, 999);
        assert_eq!(deserialized_header.db, 5);
        assert_eq!(deserialized_header.required_features, 1);
        assert_eq!(deserialized_header.handler, "handler_with_extra");
        assert_eq!(deserialized_header.user.as_deref(), Some("bob"));
        assert_eq!(deserialized_header.cluster_fingerprint, 7);
        assert_eq!(remaining_buf, b"extra_data");
    }

    #[test]
    fn test_cluster_message_header_deserialize_empty_buffer() {
        let buf = &[];
        let result = FanoutMessageHeader::deserialize(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_message_header_deserialize_buffer_too_small() {
        let buf = &[0x01, 0x02, 0x03]; // Too small for a complete header
        let result = FanoutMessageHeader::deserialize(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_message_header_deserialize_invalid_marker() {
        let mut buf = Vec::new();

        // Write invalid marker
        let invalid_marker = 0xDEADBEEF_u32;
        buf.extend_from_slice(&invalid_marker.to_le_bytes());

        // Add some dummy data for version, request_id, db, required_features
        buf.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05]);

        let result = FanoutMessageHeader::deserialize(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_version() {
        let mut buf = Vec::new();

        // Write correct marker but incomplete data
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        // No version data

        let result = FanoutMessageHeader::deserialize(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_request_id() {
        let mut buf = Vec::new();

        // Write the correct marker and version
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.push(1); // version as varint
        // Missing request_id and other fields

        let result = FanoutMessageHeader::deserialize(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    fn assert_serialization_error(result: FanoutResult<(FanoutMessageHeader, &[u8])>) {
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_db() {
        let mut buf = Vec::new();

        // Write the correct marker, version, and request_id
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.push(1); // version as varint
        buf.push(42); // request_id as varint
        // Missing db and required_features fields

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_cluster_message_header_deserialize_incomplete_reserved() {
        let mut buf = Vec::new();

        // Write the correct marker, version, request_id, and db
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.push(1); // version as varint
        buf.push(42); // request_id as varint
        buf.push(0); // db as signed varint (0 encodes as 0)
        // Missing handler, user, cluster_fingerprint, and required_features field

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_write_marker() {
        let mut buf = Vec::new();
        write_marker(&mut buf);

        assert_eq!(buf.len(), 4);
        let marker = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(marker, FANOUT_MESSAGE_MARKER);
    }

    #[test]
    fn test_skip_marker_valid() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&FANOUT_MESSAGE_MARKER.to_le_bytes());
        buf.extend_from_slice(b"remaining_data");

        let result = skip_marker(&buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"remaining_data");
    }

    #[test]
    fn test_skip_marker_invalid() {
        let mut buf = Vec::new();
        let invalid_marker = 0xDEADBEEF_u32;
        buf.extend_from_slice(&invalid_marker.to_le_bytes());
        buf.extend_from_slice(b"remaining_data");

        let result = skip_marker(&buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_skip_marker_buffer_too_small() {
        let buf = &[0x01, 0x02]; // Only 2 bytes, need 4 for u32 marker

        let result = skip_marker(buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_skip_marker_empty_buffer() {
        let buf = &[];

        let result = skip_marker(buf);
        assert!(result.is_err());
        if let Err(res) = result {
            assert_eq!(res.kind, ErrorKind::Serialization);
            assert_eq!(res.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_serialize_request_message() {
        let mut buf = Vec::new();
        let request_data = b"test_request_data";

        serialize_request_message(
            &mut buf,
            123,
            5,
            "handler",
            Some("alice"),
            0xDEAD_BEEF,
            request_data,
        );

        // Verify we can deserialize the header
        let (header, remaining) = FanoutMessageHeader::deserialize(&buf).unwrap();

        assert_eq!(header.version, FANOUT_MESSAGE_VERSION);
        assert_eq!(header.request_id, 123);
        assert_eq!(header.db, 5);
        assert_eq!(header.required_features, 0);
        assert_eq!(header.handler, "handler");
        assert_eq!(header.user.as_deref(), Some("alice"));
        assert_eq!(header.cluster_fingerprint, 0xDEAD_BEEF);
        assert_eq!(remaining, request_data);
    }

    #[test]
    fn test_request_message_new_valid() {
        let mut buf = Vec::new();
        let request_data = b"test_payload";

        serialize_request_message(&mut buf, 456, -3, "test_handler", None, 99, request_data);

        let request_message = FanoutMessage::new(&buf).unwrap();

        assert_eq!(request_message.request_id, 456);
        assert_eq!(request_message.db, -3);
        assert_eq!(request_message.handler, "test_handler");
        assert_eq!(request_message.user, None);
        assert_eq!(request_message.cluster_fingerprint, 99);
        assert_eq!(request_message.buf, request_data);
    }

    #[test]
    fn test_cluster_message_header_deserialize_rejects_version_mismatch() {
        let mut buf = Vec::new();
        write_marker(&mut buf);
        write_u16_le(&mut buf, FANOUT_MESSAGE_VERSION - 1);
        write_uvarint(&mut buf, 1);
        write_signed_varint(&mut buf, 0);
        write_byte_slice(&mut buf, b"handler");
        write_byte_slice(&mut buf, b"");
        write_u64_le(&mut buf, 0);
        write_u16_le(&mut buf, 0);

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_cluster_message_header_deserialize_rejects_oversized_user() {
        let mut buf = Vec::new();
        write_marker(&mut buf);
        write_u16_le(&mut buf, FANOUT_MESSAGE_VERSION);
        write_uvarint(&mut buf, 1);
        write_signed_varint(&mut buf, 0);
        write_byte_slice(&mut buf, b"handler");
        let oversized_user = vec![b'a'; MAX_USERNAME_LEN + 1];
        write_byte_slice(&mut buf, &oversized_user);
        write_u64_le(&mut buf, 0);
        write_u16_le(&mut buf, 0);

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_cluster_message_header_deserialize_rejects_oversized_handler() {
        let mut buf = Vec::new();
        write_marker(&mut buf);
        write_u16_le(&mut buf, FANOUT_MESSAGE_VERSION);
        write_uvarint(&mut buf, 1);
        write_signed_varint(&mut buf, 0);
        let oversized_handler = vec![b'a'; MAX_HANDLER_LEN + 1];
        write_byte_slice(&mut buf, &oversized_handler);
        write_byte_slice(&mut buf, b"");
        write_u64_le(&mut buf, 0);
        write_u16_le(&mut buf, 0);

        let result = FanoutMessageHeader::deserialize(&buf);
        assert_serialization_error(result);
    }

    #[test]
    fn test_request_message_new_invalid_header() {
        let buf = b"invalid_data";

        let result = FanoutMessage::new(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_message_header_roundtrip_property_based() {
        // Property-based test: any valid ClusterMessageHeader should roundtrip,
        // including various handler strings and fingerprints.
        let test_cases = vec![
            (0, i32::MIN, 0, "", 0u64),
            (1, -1, 1, "handler1", 1),
            (u64::MAX, i32::MAX, u16::MAX, "h", u64::MAX),
            (1234567890, 0, 999, "property_based_test", 0xABCD),
            (555, -42, 200, "with_special_字符", 0x0102_0304_0506_0708),
            (3, 4, 5, "another_handler", 42),
        ];

        for (request_id, db, required_features, handler, cluster_fingerprint) in test_cases {
            let original_header = FanoutMessageHeader {
                version: FANOUT_MESSAGE_VERSION,
                request_id,
                db,
                handler: handler.to_string(),
                user: Some("user".to_string()),
                required_features,
                cluster_fingerprint,
            };

            let mut buf = Vec::new();
            original_header.serialize(&mut buf);

            let (deserialized_header, remaining_buf) =
                FanoutMessageHeader::deserialize(&buf).unwrap();

            assert_eq!(deserialized_header.version, original_header.version);
            assert_eq!(deserialized_header.request_id, original_header.request_id);
            assert_eq!(deserialized_header.db, original_header.db);
            assert_eq!(
                deserialized_header.required_features,
                original_header.required_features
            );
            assert_eq!(deserialized_header.handler, original_header.handler);
            assert_eq!(deserialized_header.user, original_header.user);
            assert_eq!(
                deserialized_header.cluster_fingerprint,
                original_header.cluster_fingerprint
            );
            assert_eq!(remaining_buf.len(), 0);
        }
    }

    // -----------------------------------------------------------------------
    // Adversarial input
    //
    // Everything below feeds the parser bytes a correct sender cannot produce.
    // The header is decoded straight off the cluster bus inside
    // `on_request_received`, an `extern "C"` entry point, so a panic here does
    // not fail a request — it unwinds into C and aborts the node. The bar for
    // every case is therefore: return `Err`, never panic, and never allocate
    // proportionally to an attacker-chosen number.
    // -----------------------------------------------------------------------

    /// A header builder that takes every field as a raw wire value, so tests can
    /// express bytes no well-behaved sender would emit: over-long strings,
    /// non-UTF-8, a `db` outside `i32`, a version we do not speak.
    struct RawHeader<'a> {
        version: u16,
        request_id: u64,
        db: i64,
        handler: &'a [u8],
        user: &'a [u8],
        cluster_fingerprint: u64,
        required_features: u16,
    }

    impl Default for RawHeader<'_> {
        fn default() -> Self {
            Self {
                version: FANOUT_MESSAGE_VERSION,
                request_id: 1,
                db: 0,
                handler: b"handler",
                user: b"",
                cluster_fingerprint: 0,
                required_features: 0,
            }
        }
    }

    impl RawHeader<'_> {
        /// Mirrors `write_message_header` field for field.
        fn encode(&self) -> Vec<u8> {
            let mut buf = Vec::new();
            write_marker(&mut buf);
            write_u16_le(&mut buf, self.version);
            write_uvarint(&mut buf, self.request_id);
            write_signed_varint(&mut buf, self.db);
            write_byte_slice(&mut buf, self.handler);
            write_byte_slice(&mut buf, self.user);
            write_u64_le(&mut buf, self.cluster_fingerprint);
            write_u16_le(&mut buf, self.required_features);
            buf
        }
    }

    /// Non-UTF-8 handler bytes must be rejected, not lossily converted. The
    /// handler is looked up in the registry and logged on miss.
    #[test]
    fn test_deserialize_rejects_non_utf8_handler() {
        // Lone continuation byte, a truncated 2-byte sequence, and a surrogate
        // half — the three shapes `from_utf8` must catch.
        for bad in [&[0x80u8][..], &[0xC3][..], &[0xED, 0xA0, 0x80][..]] {
            let buf = RawHeader {
                handler: bad,
                ..Default::default()
            }
            .encode();
            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
        }
    }

    /// Non-UTF-8 user bytes must be rejected before the name reaches
    /// `authenticate_user`.
    #[test]
    fn test_deserialize_rejects_non_utf8_user() {
        for bad in [&[0xFFu8][..], &[0xF0, 0x9F][..]] {
            let buf = RawHeader {
                user: bad,
                ..Default::default()
            }
            .encode();
            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
        }
    }

    /// The length caps are `>`, not `>=`: a name of exactly the maximum length
    /// is legitimate and must still decode. Guards the oversized-* tests against
    /// passing for an off-by-one reason.
    #[test]
    fn test_deserialize_accepts_fields_exactly_at_length_cap() {
        let handler = vec![b'h'; MAX_HANDLER_LEN];
        let user = vec![b'u'; MAX_USERNAME_LEN];
        let buf = RawHeader {
            handler: &handler,
            user: &user,
            ..Default::default()
        }
        .encode();

        let (header, remaining) = FanoutMessageHeader::deserialize(&buf).unwrap();
        assert_eq!(header.handler.len(), MAX_HANDLER_LEN);
        assert_eq!(header.user.as_deref().map(str::len), Some(MAX_USERNAME_LEN));
        assert!(remaining.is_empty());
    }

    /// A length prefix that overstates the bytes actually present must fail on
    /// the remaining-data check. This is the shape that would otherwise read
    /// past the end of the received payload.
    #[test]
    fn test_deserialize_rejects_length_prefix_exceeding_buffer() {
        let mut buf = Vec::new();
        write_marker(&mut buf);
        write_u16_le(&mut buf, FANOUT_MESSAGE_VERSION);
        write_uvarint(&mut buf, 1);
        write_signed_varint(&mut buf, 0);
        write_uvarint(&mut buf, 64); // handler claims 64 bytes...
        buf.extend_from_slice(b"short"); // ...only 5 follow

        assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
    }

    /// An absurd length prefix must be rejected by the remaining-data check
    /// before anything is sized from it. If the parser ever pre-allocated on the
    /// claimed length, these values would OOM or panic on capacity overflow
    /// rather than return an error.
    #[test]
    fn test_deserialize_rejects_absurd_length_prefix() {
        for claimed in [u64::MAX, u64::MAX / 2, 1 << 40, usize::MAX as u64] {
            let mut buf = Vec::new();
            write_marker(&mut buf);
            write_u16_le(&mut buf, FANOUT_MESSAGE_VERSION);
            write_uvarint(&mut buf, 1);
            write_signed_varint(&mut buf, 0);
            write_uvarint(&mut buf, claimed);

            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
        }
    }

    /// Every strict prefix of a valid message must be rejected. This covers the
    /// truncation points the hand-written `incomplete_*` tests miss — mid-string
    /// bodies, a partial 8-byte fingerprint, and a 1-byte `required_features`.
    #[test]
    fn test_deserialize_rejects_every_truncated_prefix() {
        let full = RawHeader {
            request_id: u64::MAX, // 10-byte varint, so truncation lands inside it too
            db: -7,
            handler: b"mrange",
            user: b"alice",
            cluster_fingerprint: u64::MAX,
            ..Default::default()
        }
        .encode();

        for len in 0..full.len() {
            assert!(
                FanoutMessageHeader::deserialize(&full[..len]).is_err(),
                "a {len}-byte prefix of a {}-byte header parsed as valid",
                full.len()
            );
        }
        assert!(FanoutMessageHeader::deserialize(&full).is_ok());
    }

    /// An unterminated varint must stop on the decoder's shift guard instead of
    /// consuming the rest of the buffer or looping.
    #[test]
    fn test_deserialize_rejects_unterminated_varint() {
        for run in [10usize, 64, 4096] {
            let mut buf = Vec::new();
            write_marker(&mut buf);
            write_u16_le(&mut buf, FANOUT_MESSAGE_VERSION);
            // request_id: continuation bit set on every byte.
            buf.extend_from_slice(&vec![0x80u8; run]);
            buf.push(0x01);

            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
        }
    }

    /// `db` is written from an `i32`; a value outside that range is malformed and
    /// must be rejected rather than truncated into a different, valid-looking
    /// database. `on_request_received` hands this value straight to `SelectDb`.
    #[test]
    fn test_deserialize_rejects_db_outside_i32_range() {
        for db in [
            i32::MAX as i64 + 1,
            i32::MIN as i64 - 1,
            1 << 32, // truncates to 0 under an `as i32` cast
            i64::MAX,
            i64::MIN,
        ] {
            let buf = RawHeader {
                db,
                ..Default::default()
            }
            .encode();
            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
        }

        // The extremes of the representable range still decode.
        for db in [i32::MIN, i32::MAX] {
            let buf = RawHeader {
                db: db as i64,
                ..Default::default()
            }
            .encode();
            let (header, _) = FanoutMessageHeader::deserialize(&buf).unwrap();
            assert_eq!(header.db, db);
        }
    }

    /// Only the exact current version is accepted — older and newer alike.
    #[test]
    fn test_deserialize_rejects_all_other_versions() {
        for version in [0, FANOUT_MESSAGE_VERSION + 1, 0x00FF, u16::MAX] {
            let buf = RawHeader {
                version,
                ..Default::default()
            }
            .encode();
            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
        }
    }

    /// An empty user field decodes to `None`, which `with_fanout_user` treats as
    /// "run without ACL enforcement". Pinning this makes the collapse explicit:
    /// a peer cannot smuggle a distinct zero-length ACL identity through, and
    /// conversely an empty name is never passed to `authenticate_user`.
    #[test]
    fn test_deserialize_empty_user_collapses_to_none() {
        let from_wire = RawHeader {
            user: b"",
            ..Default::default()
        }
        .encode();
        let (header, _) = FanoutMessageHeader::deserialize(&from_wire).unwrap();
        assert_eq!(header.user, None);

        // Serializing `Some("")` produces the identical bytes.
        let mut round_trip = Vec::new();
        FanoutMessageHeader {
            version: FANOUT_MESSAGE_VERSION,
            request_id: 1,
            db: 0,
            handler: "handler".to_string(),
            user: Some(String::new()),
            cluster_fingerprint: 0,
            required_features: 0,
        }
        .serialize(&mut round_trip);
        assert_eq!(round_trip, from_wire);
    }

    /// Framing is length-prefixed, so interior NUL bytes must survive intact
    /// rather than truncating the name the way a C string would. A silently
    /// truncated "alice\0evil" must not become "alice".
    #[test]
    fn test_deserialize_preserves_interior_nul_in_user() {
        let buf = RawHeader {
            user: b"alice\0evil",
            ..Default::default()
        }
        .encode();

        let (header, _) = FanoutMessageHeader::deserialize(&buf).unwrap();
        assert_eq!(header.user.as_deref(), Some("alice\0evil"));
    }

    /// An unsupported `required_features` bit must survive parsing so intake can
    /// see it and reject the message. Dropping or masking it here would let the
    /// request through as if it demanded nothing.
    #[test]
    fn test_unsupported_features_survive_parsing_for_intake() {
        for required_features in [1u16, 0x8000, u16::MAX] {
            let buf = RawHeader {
                required_features,
                ..Default::default()
            }
            .encode();

            let msg = FanoutMessage::new(&buf).unwrap();
            assert_eq!(msg.required_features, required_features);
            assert!(has_unsupported_features(msg.required_features));
        }
    }

    proptest! {
        /// Totality: arbitrary bytes off the cluster bus must decode to `Ok` or
        /// `Err`, never panic. `deserialize` runs under an `extern "C"` callback,
        /// where an unwind aborts the process.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
            let _ = FanoutMessageHeader::deserialize(&bytes);
            let _ = FanoutMessage::new(&bytes);
        }

        /// The same, but starting from a valid marker so the generator gets past
        /// the cheap `skip_marker` rejection and actually exercises field decoding.
        #[test]
        fn arbitrary_bytes_after_valid_marker_never_panic(
            tail in prop::collection::vec(any::<u8>(), 0..1024),
        ) {
            let mut buf = FANOUT_MESSAGE_MARKER.to_le_bytes().to_vec();
            buf.extend_from_slice(&tail);
            let _ = FanoutMessageHeader::deserialize(&buf);
        }

        /// Single-byte corruption of a well-formed message: whatever survives
        /// parsing must still satisfy the parser's own invariants. Catches a cap
        /// or version check that a flipped length/discriminant can slip past.
        #[test]
        fn corrupted_message_never_violates_invariants(
            index in any::<prop::sample::Index>(),
            replacement in any::<u8>(),
        ) {
            let mut buf = RawHeader {
                request_id: 4242,
                db: 3,
                handler: b"mrange",
                user: b"alice",
                cluster_fingerprint: 0xFEED_FACE,
                ..Default::default()
            }
            .encode();

            let i = index.index(buf.len());
            buf[i] = replacement;

            match FanoutMessageHeader::deserialize(&buf) {
                Ok((header, _)) => {
                    prop_assert_eq!(header.version, FANOUT_MESSAGE_VERSION);
                    prop_assert!(header.handler.len() <= MAX_HANDLER_LEN);
                    prop_assert!(header.user.as_deref().map_or(0, str::len) <= MAX_USERNAME_LEN);
                    prop_assert!(header.user.as_deref() != Some(""));
                }
                Err(e) => prop_assert_eq!(e.kind, ErrorKind::Serialization),
            }
        }

        /// Round-trip fidelity across the full range of each field, so the
        /// rejection tests above cannot be passing by rejecting too much.
        /// `user` is generated non-empty because empty and `None` are the same
        /// on the wire (see `test_deserialize_empty_user_collapses_to_none`).
        #[test]
        fn valid_headers_round_trip(
            request_id in any::<u64>(),
            db in any::<i32>(),
            // Char counts, not byte counts: capped so that even all-4-byte
            // characters stay inside MAX_HANDLER_LEN / MAX_USERNAME_LEN.
            handler in "\\PC{0,32}",
            user in prop::option::of("\\PC{1,64}"),
            cluster_fingerprint in any::<u64>(),
            required_features in any::<u16>(),
        ) {
            let original = FanoutMessageHeader {
                version: FANOUT_MESSAGE_VERSION,
                request_id,
                db,
                handler,
                user,
                cluster_fingerprint,
                required_features,
            };

            let mut buf = Vec::new();
            original.serialize(&mut buf);
            let (decoded, remaining) = FanoutMessageHeader::deserialize(&buf).unwrap();

            prop_assert_eq!(decoded.request_id, original.request_id);
            prop_assert_eq!(decoded.db, original.db);
            prop_assert_eq!(decoded.handler, original.handler);
            prop_assert_eq!(decoded.user, original.user);
            prop_assert_eq!(decoded.cluster_fingerprint, original.cluster_fingerprint);
            prop_assert_eq!(decoded.required_features, original.required_features);
            prop_assert!(remaining.is_empty());
        }
    }
}
