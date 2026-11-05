use super::fanout_error::INVALID_MESSAGE_ERROR;
use crate::commands::fanout_codec::EnvelopeHeader;
use crate::fanout::{FanoutError, FanoutResult};
use prost::Message;
use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};

pub const FANOUT_MESSAGE_MARKER: u32 = 0xBADCAB;

/// Envelope feature bits this node supports (`required_features` in the
/// message header). A message demanding bits outside this mask is rejected at
/// intake with [`FanoutError::unsupported_features`] instead of being silently
/// mis-processed. No bits are defined yet: allocate one only for a future
/// envelope/payload change the receiver cannot ignore (e.g. payload
/// compression) — see docs/fanout-compatibility-handshake.md.
pub(super) const SUPPORTED_MESSAGE_FEATURES: u64 = 0;

/// True when `required_features` demands a feature outside
/// [`SUPPORTED_MESSAGE_FEATURES`].
pub(super) fn has_unsupported_features(required_features: u64) -> bool {
    required_features & !SUPPORTED_MESSAGE_FEATURES != 0
}

const MARKER_SIZE: usize = size_of::<u32>();
const MAX_USERNAME_LEN: usize = 1024;
/// Registered fanout handler names are short internal identifiers (e.g. "mget",
/// "queryindex"); this cap leaves generous headroom.
const MAX_HANDLER_LEN: usize = 128;
/// Upper bound on the encoded header, enforced before decoding so it bounds what
/// a malicious or buggy peer can make the decoder allocate. Well above the field
/// caps, to leave room for fields a newer peer may add.
const MAX_HEADER_LEN: usize = 4096;

fn invalid_message() -> FanoutError {
    FanoutError::serialization(INVALID_MESSAGE_ERROR)
}

/// Header for messages exchanged between cluster nodes: an [`EnvelopeHeader`]
/// that has passed the checks the wire format cannot express.
#[derive(Debug, Clone)]
pub(super) struct FanoutMessageHeader {
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
    /// `0` means no requirements.
    pub required_features: u64,
}

impl TryFrom<EnvelopeHeader> for FanoutMessageHeader {
    type Error = FanoutError;

    fn try_from(wire: EnvelopeHeader) -> FanoutResult<Self> {
        // Reject rather than truncate: an `as i32` cast would silently fold e.g.
        // 2^32 down to a *valid-looking* db 0 and run the request against the
        // wrong keyspace.
        let db = i32::try_from(wire.db).map_err(|_| invalid_message())?;
        if wire.handler.len() > MAX_HANDLER_LEN || wire.user.len() > MAX_USERNAME_LEN {
            return Err(invalid_message());
        }
        Ok(Self {
            request_id: wire.request_id,
            db,
            handler: wire.handler,
            user: (!wire.user.is_empty()).then_some(wire.user),
            cluster_fingerprint: wire.cluster_fingerprint,
            required_features: wire.required_features,
        })
    }
}

impl FanoutMessageHeader {
    #[cfg(test)]
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        write_message_header(
            buf,
            &EnvelopeHeader {
                request_id: self.request_id,
                db: i64::from(self.db),
                handler: self.handler.clone(),
                user: self.user.clone().unwrap_or_default(),
                cluster_fingerprint: self.cluster_fingerprint,
                required_features: self.required_features,
            },
        );
    }

    /// Deserializes a MessageHeader from the beginning of the buffer.
    /// Returns the deserialized header and the payload buffer.
    pub fn deserialize(buf: &[u8]) -> FanoutResult<(Self, &[u8])> {
        let mut buf = skip_marker(buf)?;

        let len = decode_varint(&mut buf).map_err(|_| invalid_message())?;
        // A real header always carries a request id, so an empty one is malformed.
        if len == 0 || len > MAX_HEADER_LEN as u64 || len > buf.len() as u64 {
            return Err(invalid_message());
        }

        let (encoded, payload) = buf.split_at(len as usize);
        let wire = EnvelopeHeader::decode(encoded).map_err(|_| invalid_message())?;
        Ok((wire.try_into()?, payload))
    }
}

/// Writes the marker and the length-prefixed header. The payload follows
/// directly, outside the protobuf, so it is not copied through it.
fn write_message_header(dest: &mut Vec<u8>, header: &EnvelopeHeader) {
    let len = header.encoded_len();
    dest.reserve(MARKER_SIZE + encoded_len_varint(len as u64) + len);
    write_marker(dest);
    encode_varint(len as u64, dest);
    header.encode_raw(dest);
}

fn write_marker(slice: &mut Vec<u8>) {
    let bytes = FANOUT_MESSAGE_MARKER.to_le_bytes();
    slice.extend_from_slice(bytes.as_ref());
}

fn skip_marker(input: &[u8]) -> FanoutResult<&[u8]> {
    let Some((int_bytes, rest)) = input.split_first_chunk::<MARKER_SIZE>() else {
        return Err(invalid_message());
    };
    if u32::from_le_bytes(*int_bytes) != FANOUT_MESSAGE_MARKER {
        return Err(invalid_message());
    }
    Ok(rest)
}

/// A decoded fanout message: its header and the payload it borrows.
pub(super) struct FanoutMessage<'a> {
    pub header: FanoutMessageHeader,
    pub buf: &'a [u8],
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
        Ok(Self { header, buf })
    }
}

pub(super) fn serialize_request_message(
    dest: &mut Vec<u8>,
    request_id: u64,
    db: i32,
    handler: &str,
    user: Option<String>,
    cluster_fingerprint: u64,
    serialized_request: &[u8],
) {
    write_message_header(
        dest,
        &EnvelopeHeader {
            request_id,
            db: i64::from(db),
            handler: handler.to_owned(),
            user: user.unwrap_or_default(),
            cluster_fingerprint,
            required_features: 0,
        },
    );
    dest.extend_from_slice(serialized_request);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::ErrorKind;
    use proptest::prelude::*;
    use prost::encoding::{WireType, encode_key, fixed64, sint64, uint64};

    fn assert_serialization_error(result: FanoutResult<(FanoutMessageHeader, &[u8])>) {
        let err = result.expect_err("expected a rejected message");
        assert_eq!(err.kind, ErrorKind::Serialization);
        assert_eq!(err.message, INVALID_MESSAGE_ERROR);
    }

    fn write_len_delimited(buf: &mut Vec<u8>, tag: u32, bytes: &[u8]) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(bytes.len() as u64, buf);
        buf.extend_from_slice(bytes);
    }

    /// Frames already-encoded header bytes the way a sender would.
    fn frame(encoded_header: &[u8]) -> Vec<u8> {
        let mut buf = FANOUT_MESSAGE_MARKER.to_le_bytes().to_vec();
        encode_varint(encoded_header.len() as u64, &mut buf);
        buf.extend_from_slice(encoded_header);
        buf
    }

    /// A header builder that takes every field as a raw wire value, so tests can
    /// express bytes no well-behaved sender would emit: over-long strings,
    /// non-UTF-8, a `db` outside `i32`. Omits default values, as prost does.
    struct RawHeader<'a> {
        request_id: u64,
        db: i64,
        handler: &'a [u8],
        user: &'a [u8],
        cluster_fingerprint: u64,
        required_features: u64,
    }

    impl Default for RawHeader<'_> {
        fn default() -> Self {
            Self {
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
        fn encode(&self) -> Vec<u8> {
            let mut h = Vec::new();
            if self.request_id != 0 {
                uint64::encode(1, &self.request_id, &mut h);
            }
            if self.db != 0 {
                sint64::encode(2, &self.db, &mut h);
            }
            if !self.handler.is_empty() {
                write_len_delimited(&mut h, 3, self.handler);
            }
            if !self.user.is_empty() {
                write_len_delimited(&mut h, 4, self.user);
            }
            if self.cluster_fingerprint != 0 {
                fixed64::encode(5, &self.cluster_fingerprint, &mut h);
            }
            if self.required_features != 0 {
                uint64::encode(6, &self.required_features, &mut h);
            }
            frame(&h)
        }
    }

    // -----------------------------------------------------------------------
    // Round trips
    // -----------------------------------------------------------------------

    #[test]
    fn test_header_round_trip_basic() {
        let header = FanoutMessageHeader {
            request_id: 12345,
            db: 0,
            handler: "test_handler".to_string(),
            user: None,
            cluster_fingerprint: 0xABCD_1234_5678_9F00,
            required_features: 0,
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let (decoded, remaining) = FanoutMessageHeader::deserialize(&buf).unwrap();
        assert_eq!(decoded.request_id, 12345);
        assert_eq!(decoded.db, 0);
        assert_eq!(decoded.handler, "test_handler");
        assert_eq!(decoded.user, None);
        assert_eq!(decoded.required_features, 0);
        assert_eq!(decoded.cluster_fingerprint, 0xABCD_1234_5678_9F00);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_header_round_trip_extremes() {
        let header = FanoutMessageHeader {
            request_id: u64::MAX,
            db: -15,
            handler: "with_special_字符".to_string(),
            user: Some("alice".to_string()),
            cluster_fingerprint: u64::MAX,
            required_features: u64::MAX,
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf);

        let (decoded, remaining) = FanoutMessageHeader::deserialize(&buf).unwrap();
        assert_eq!(decoded.request_id, u64::MAX);
        assert_eq!(decoded.db, -15);
        assert_eq!(decoded.handler, "with_special_字符");
        assert_eq!(decoded.user.as_deref(), Some("alice"));
        assert_eq!(decoded.cluster_fingerprint, u64::MAX);
        assert_eq!(decoded.required_features, u64::MAX);
        assert!(remaining.is_empty());
    }

    /// Bytes after the header are the payload, returned untouched.
    #[test]
    fn test_payload_follows_header() {
        let mut buf = Vec::new();
        serialize_request_message(
            &mut buf,
            123,
            5,
            "handler",
            Some("alice".to_string()),
            0xDEAD_BEEF,
            b"test_request_data",
        );

        let message = FanoutMessage::new(&buf).unwrap();
        assert_eq!(message.header.request_id, 123);
        assert_eq!(message.header.db, 5);
        assert_eq!(message.header.required_features, 0);
        assert_eq!(message.header.handler, "handler");
        assert_eq!(message.header.user.as_deref(), Some("alice"));
        assert_eq!(message.header.cluster_fingerprint, 0xDEAD_BEEF);
        assert_eq!(message.buf, b"test_request_data");
    }

    /// Error replies are sent with an empty handler; that must stay legal.
    #[test]
    fn test_empty_handler_is_accepted() {
        let mut buf = Vec::new();
        serialize_request_message(&mut buf, 9, 0, "", None, 0, b"err");

        let message = FanoutMessage::new(&buf).unwrap();
        assert_eq!(message.header.request_id, 9);
        assert_eq!(message.header.handler, "");
        assert_eq!(message.buf, b"err");
    }

    /// Pins the wire layout. Field numbers are the compatibility contract, so
    /// renumbering one must fail here rather than only between mixed-version
    /// nodes.
    #[test]
    fn test_wire_layout_is_stable() {
        let mut buf = Vec::new();
        serialize_request_message(
            &mut buf,
            7,
            -1,
            "mget",
            Some("bob".to_string()),
            0x0102_0304_0506_0708,
            b"P",
        );

        let mut expected = vec![0xAB, 0xDC, 0xBA, 0x00]; // marker, little-endian
        expected.push(24); // header length
        expected.extend_from_slice(&[0x08, 7]); // 1: request_id
        expected.extend_from_slice(&[0x10, 1]); // 2: db, zigzag(-1)
        expected.extend_from_slice(&[0x1A, 4]); // 3: handler
        expected.extend_from_slice(b"mget");
        expected.extend_from_slice(&[0x22, 3]); // 4: user
        expected.extend_from_slice(b"bob");
        expected.push(0x29); // 5: cluster_fingerprint, fixed64 little-endian
        expected.extend_from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]);
        expected.push(b'P'); // payload
        assert_eq!(buf, expected);
    }

    // -----------------------------------------------------------------------
    // Forward compatibility
    // -----------------------------------------------------------------------

    /// A newer peer's fields are skipped, in every wire type, and the payload
    /// boundary still comes from the length prefix.
    #[test]
    fn test_unknown_header_fields_are_ignored() {
        let mut h = Vec::new();
        uint64::encode(1, &7, &mut h);
        write_len_delimited(&mut h, 3, b"mrange");
        uint64::encode(50, &1, &mut h);
        fixed64::encode(51, &1, &mut h);
        write_len_delimited(&mut h, 52, b"from the future");

        let mut buf = frame(&h);
        buf.extend_from_slice(b"PAYLOAD");

        let message = FanoutMessage::new(&buf).unwrap();
        assert_eq!(message.header.request_id, 7);
        assert_eq!(message.header.handler, "mrange");
        assert_eq!(message.buf, b"PAYLOAD");
    }

    /// Fields an older peer does not send decode to values meaning "nothing to
    /// enforce or check".
    #[test]
    fn test_absent_fields_take_their_defaults() {
        let mut h = Vec::new();
        uint64::encode(1, &7, &mut h);
        let buf = frame(&h);

        let message = FanoutMessage::new(&buf).unwrap();
        assert_eq!(message.header.db, 0);
        assert_eq!(message.header.handler, "");
        assert_eq!(message.header.user, None);
        assert_eq!(message.header.cluster_fingerprint, 0);
        assert_eq!(message.header.required_features, 0);
    }

    /// The envelope feature gate: with no bits defined, any demanded bit is
    /// unsupported, and `serialize_request_message` keeps sending 0 until a
    /// feature bit is actually defined.
    #[test]
    fn test_required_features_gate() {
        assert!(!has_unsupported_features(0));
        assert!(!has_unsupported_features(SUPPORTED_MESSAGE_FEATURES));
        assert!(has_unsupported_features(1));
        assert!(has_unsupported_features(1 << 32));
        assert!(has_unsupported_features(u64::MAX));

        let mut buf = Vec::new();
        serialize_request_message(&mut buf, 7, 0, "mrange", None, 0, &[]);
        let message = FanoutMessage::new(&buf).unwrap();
        assert_eq!(message.header.required_features, 0);
    }

    /// An unsupported bit must survive parsing so intake can see it and reject
    /// the message. Bits above 32 guard the field's width: a 32-bit field would
    /// truncate them away and let the request through as if it demanded nothing.
    #[test]
    fn test_unsupported_features_survive_parsing_for_intake() {
        for required_features in [1u64, 0x8000, 1 << 32, 1 << 63, u64::MAX] {
            let buf = RawHeader {
                required_features,
                ..Default::default()
            }
            .encode();

            let message = FanoutMessage::new(&buf).unwrap();
            assert_eq!(message.header.required_features, required_features);
            assert!(has_unsupported_features(message.header.required_features));
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

    #[test]
    fn test_skip_marker() {
        let mut buf = Vec::new();
        write_marker(&mut buf);
        assert_eq!(buf.len(), MARKER_SIZE);
        buf.extend_from_slice(b"remaining_data");
        assert_eq!(skip_marker(&buf).unwrap(), b"remaining_data");

        let mut invalid = 0xDEADBEEF_u32.to_le_bytes().to_vec();
        invalid.extend_from_slice(b"remaining_data");
        for bad in [&invalid[..], &[0x01, 0x02], &[]] {
            let err = skip_marker(bad).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Serialization);
            assert_eq!(err.message, INVALID_MESSAGE_ERROR);
        }
    }

    #[test]
    fn test_deserialize_rejects_invalid_marker() {
        let mut buf = RawHeader::default().encode();
        buf[0] ^= 0xFF;
        assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
    }

    #[test]
    fn test_request_message_new_invalid_header() {
        assert!(FanoutMessage::new(b"invalid_data").is_err());
    }

    /// A zero-length header would otherwise decode to all defaults.
    #[test]
    fn test_deserialize_rejects_empty_header() {
        assert_serialization_error(FanoutMessageHeader::deserialize(&frame(&[])));
    }

    /// A message in the previous hand-rolled layout (marker, u16 version, then
    /// positional fields) is rejected rather than misread.
    #[test]
    fn test_deserialize_rejects_previous_format() {
        let mut buf = FANOUT_MESSAGE_MARKER.to_le_bytes().to_vec();
        buf.extend_from_slice(&1u16.to_le_bytes()); // version
        buf.push(42); // request_id
        buf.push(0); // db
        buf.push(6); // handler
        buf.extend_from_slice(b"mrange");
        buf.push(0); // user
        buf.extend_from_slice(&[0; 8]); // cluster_fingerprint
        buf.extend_from_slice(&[0; 2]); // required_features

        assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
    }

    #[test]
    fn test_deserialize_rejects_oversized_user() {
        let user = vec![b'a'; MAX_USERNAME_LEN + 1];
        let buf = RawHeader {
            user: &user,
            ..Default::default()
        }
        .encode();
        assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
    }

    #[test]
    fn test_deserialize_rejects_oversized_handler() {
        let handler = vec![b'a'; MAX_HANDLER_LEN + 1];
        let buf = RawHeader {
            handler: &handler,
            ..Default::default()
        }
        .encode();
        assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
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

    /// The cap on the whole header applies to unknown fields too, which the
    /// per-field caps cannot see: exactly at the cap decodes, one byte over does
    /// not.
    #[test]
    fn test_header_length_cap() {
        let header_of_len = |len: usize| {
            let mut h = Vec::new();
            uint64::encode(1, &7, &mut h);
            // Tag 50 takes 2 bytes and the length prefix 2 more at this size.
            let filler = vec![0; len - h.len() - 4];
            write_len_delimited(&mut h, 50, &filler);
            assert_eq!(h.len(), len);
            frame(&h)
        };

        assert!(FanoutMessageHeader::deserialize(&header_of_len(MAX_HEADER_LEN)).is_ok());
        assert_serialization_error(FanoutMessageHeader::deserialize(&header_of_len(
            MAX_HEADER_LEN + 1,
        )));
    }

    /// Non-UTF-8 handler bytes must be rejected, not lossily converted. The
    /// handler is looked up in the registry and logged on miss.
    #[test]
    fn test_deserialize_rejects_non_utf8_handler() {
        // Lone continuation byte, a truncated 2-byte sequence, and a surrogate
        // half — the three shapes UTF-8 validation must catch.
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

    /// A header length that overstates the bytes actually present must fail on
    /// the remaining-data check.
    #[test]
    fn test_deserialize_rejects_header_length_exceeding_buffer() {
        let mut buf = FANOUT_MESSAGE_MARKER.to_le_bytes().to_vec();
        encode_varint(64, &mut buf); // claims 64 bytes...
        buf.extend_from_slice(b"short"); // ...only 5 follow

        assert_serialization_error(FanoutMessageHeader::deserialize(&buf));
    }

    /// An absurd length, at either framing level, must be rejected before
    /// anything is sized from it. If the parser ever pre-allocated on the claimed
    /// length, these values would OOM or panic on capacity overflow rather than
    /// return an error.
    #[test]
    fn test_deserialize_rejects_absurd_lengths() {
        for claimed in [u64::MAX, u64::MAX / 2, 1 << 40, usize::MAX as u64] {
            let mut buf = FANOUT_MESSAGE_MARKER.to_le_bytes().to_vec();
            encode_varint(claimed, &mut buf);
            assert_serialization_error(FanoutMessageHeader::deserialize(&buf));

            let mut h = Vec::new();
            encode_key(3, WireType::LengthDelimited, &mut h);
            encode_varint(claimed, &mut h);
            assert_serialization_error(FanoutMessageHeader::deserialize(&frame(&h)));
        }
    }

    /// Every strict prefix of a valid message must be rejected, wherever the cut
    /// lands: the marker, the length, or the header. The header length is what
    /// catches a cut inside it, so a bad prefix cannot decode as a shorter but
    /// well-formed protobuf.
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

    /// An unterminated varint must fail instead of consuming the rest of the
    /// buffer or looping, in the framing length and in a header field.
    #[test]
    fn test_deserialize_rejects_unterminated_varint() {
        for run in [10usize, 64, 4000] {
            let mut in_length = FANOUT_MESSAGE_MARKER.to_le_bytes().to_vec();
            in_length.extend_from_slice(&vec![0x80u8; run]);
            in_length.push(0x01);
            assert_serialization_error(FanoutMessageHeader::deserialize(&in_length));

            let mut h = vec![0x08]; // 1: request_id, as a varint
            h.extend_from_slice(&vec![0x80u8; run]);
            h.push(0x01);
            assert_serialization_error(FanoutMessageHeader::deserialize(&frame(&h)));
        }
    }

    /// Unknown groups nest, so skipping them recurses. Deep nesting must hit the
    /// decoder's recursion limit rather than overflow the stack.
    #[test]
    fn test_deserialize_rejects_deeply_nested_unknown_groups() {
        let mut h = Vec::new();
        for _ in 0..1000 {
            encode_key(99, WireType::StartGroup, &mut h);
        }
        assert_serialization_error(FanoutMessageHeader::deserialize(&frame(&h)));
    }

    /// A known field with the wrong wire type is corruption, not an unknown field.
    #[test]
    fn test_deserialize_rejects_wrong_wire_type() {
        let mut h = Vec::new();
        write_len_delimited(&mut h, 1, b"not a varint");
        assert_serialization_error(FanoutMessageHeader::deserialize(&frame(&h)));
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

    /// An empty user field decodes to `None`, which `FanoutContext` treats as
    /// "run without ACL enforcement". Pinning this makes the collapse explicit:
    /// a peer cannot smuggle a distinct zero-length ACL identity through, and
    /// conversely an empty name is never resolved via `ModuleUser::from_name`.
    #[test]
    fn test_deserialize_empty_user_collapses_to_none() {
        let from_wire = RawHeader::default().encode();
        let (header, _) = FanoutMessageHeader::deserialize(&from_wire).unwrap();
        assert_eq!(header.user, None);

        // An explicitly encoded empty string decodes the same way.
        let mut h = Vec::new();
        uint64::encode(1, &1, &mut h);
        write_len_delimited(&mut h, 4, b"");
        let (header, _) = FanoutMessageHeader::deserialize(&frame(&h)).unwrap();
        assert_eq!(header.user, None);

        // Serializing `Some("")` produces the identical bytes.
        let mut round_trip = Vec::new();
        FanoutMessageHeader {
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

    proptest! {
        /// Totality: arbitrary bytes off the cluster bus must decode to `Ok` or
        /// `Err`, never panic. `deserialize` runs under an `extern "C"` callback,
        /// where an unwind aborts the process.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
            let _ = FanoutMessageHeader::deserialize(&bytes);
            let _ = FanoutMessage::new(&bytes);
        }

        /// The same, but framed so the generator gets past the marker and length
        /// checks and actually exercises field decoding.
        #[test]
        fn arbitrary_header_bytes_never_panic(
            header in prop::collection::vec(any::<u8>(), 0..1024),
        ) {
            let _ = FanoutMessageHeader::deserialize(&frame(&header));
        }

        /// Single-byte corruption of a well-formed message: whatever survives
        /// parsing must still satisfy the parser's own invariants. Catches a cap
        /// that a flipped length can slip past.
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
        /// `request_id` is non-zero so the header can never encode to nothing.
        #[test]
        fn valid_headers_round_trip(
            request_id in 1..=u64::MAX,
            db in any::<i32>(),
            // Char counts, not byte counts: capped so that even all-4-byte
            // characters stay inside MAX_HANDLER_LEN / MAX_USERNAME_LEN.
            handler in "\\PC{0,32}",
            user in prop::option::of("\\PC{1,64}"),
            cluster_fingerprint in any::<u64>(),
            required_features in any::<u64>(),
        ) {
            let original = FanoutMessageHeader {
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
