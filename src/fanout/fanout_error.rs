use crate::common::encoding::{try_read_byte_slice, write_byte_slice};
use crate::error_consts;
use valkey_module::{ValkeyError, ValkeyResult};

/// Fanout error. Designed mostly for compactness since it's sent over the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutError {
    pub kind: ErrorKind,

    /// Extra context about error
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum ErrorKind {
    /// Invalid cluster message
    InvalidMessage = 0,

    /// A cluster node was unreachable.
    NodeUnreachable = 1,

    /// One or more of the nodes in the cluster did not respond to the request in time.
    Timeout = 2,

    /// Unknown message type.
    UnknownMessageType = 3,

    Permissions = 4,

    /// The user does not have access to one or more keys required to fulfill the request.
    KeyPermissions = 5,

    /// Error during serialization or deserialization of the request or response.
    Serialization = 6,

    BadRequestId = 7,

    Internal = 8,

    /// The cluster topology changed between the sending of a request and its
    /// receipt: the requester's cluster-map fingerprint did not match the
    /// receiver's. The aggregate result would be inconsistent, so the fanout
    /// fails fast, and the requester invalidates its cluster map.
    ClusterMapMismatch = 9,

    /// The message header demanded envelope features (required_features bits)
    /// this node does not support. Rejecting explicitly lets a newer peer fail
    /// fast (or degrade) instead of this node silently mis-processing the
    /// message. See docs/fanout-compatibility-handshake.md.
    UnsupportedFeatures = 10,

    Custom = 255,
}

pub(super) const PERMISSIONS_ERROR: &str = "Permission denied";
pub(super) const KEY_PERMISSIONS_ERROR: &str = "User does not have access to one or more keys";
pub(super) const UNKNOWN_MESSAGE_TYPE_ERROR: &str = "Unknown message type.";
pub(super) const SERIALIZATION_ERROR: &str = "Serialization error";
pub(super) const BAD_REQUEST_ID_ERROR: &str = "Bad request id";
pub(super) const TIMEOUT_ERROR: &str = "A multi-shard command failed because at least one shard did not reply within the given timeframe.";
pub(super) const NODE_UNREACHABLE_ERROR: &str = "Cluster node unreachable";
pub(super) const NO_CLUSTER_NODES_AVAILABLE: &str = "No cluster nodes available";
pub(super) const INTERNAL_ERROR: &str = "Internal error";
pub(super) const INVALID_MESSAGE_ERROR: &str = "Invalid cluster message";
pub(super) const CLUSTER_MAP_MISMATCH_ERROR: &str =
    "A multi-shard command failed because the cluster topology has changed";
pub(super) const UNSUPPORTED_FEATURES_ERROR: &str = "A multi-shard command failed because a peer requires fanout features this node does not support";

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidMessage => INVALID_MESSAGE_ERROR,
            Self::Permissions => PERMISSIONS_ERROR,
            Self::KeyPermissions => KEY_PERMISSIONS_ERROR,
            Self::UnknownMessageType => UNKNOWN_MESSAGE_TYPE_ERROR,
            Self::Serialization => SERIALIZATION_ERROR,
            Self::BadRequestId => BAD_REQUEST_ID_ERROR,
            Self::Timeout => TIMEOUT_ERROR,
            Self::NodeUnreachable => NODE_UNREACHABLE_ERROR,
            Self::Internal => INTERNAL_ERROR,
            Self::ClusterMapMismatch => CLUSTER_MAP_MISMATCH_ERROR,
            Self::UnsupportedFeatures => UNSUPPORTED_FEATURES_ERROR,
            Self::Custom => "Custom error",
        }
    }
}

pub type FanoutResult<T = ()> = Result<T, FanoutError>;

impl FanoutError {
    pub fn timeout() -> FanoutError {
        ErrorKind::Timeout.into()
    }

    pub fn serialization<S: Into<String>>(description: S) -> Self {
        Self {
            message: description.into(),
            kind: ErrorKind::Serialization,
        }
    }

    pub fn key_permissions(description: String) -> Self {
        Self {
            message: description,
            kind: ErrorKind::KeyPermissions,
        }
    }

    pub fn invalid_message() -> Self {
        ErrorKind::InvalidMessage.into()
    }

    pub fn cluster_map_mismatch() -> Self {
        ErrorKind::ClusterMapMismatch.into()
    }

    pub fn unsupported_features() -> Self {
        ErrorKind::UnsupportedFeatures.into()
    }

    pub fn custom<S: Into<String>>(description: S) -> Self {
        Self {
            message: description.into(),
            kind: ErrorKind::Custom,
        }
    }

    /// Whether this error means the peer does not know the operation at all,
    /// rather than that the operation was attempted and failed.
    ///
    /// A node that predates a fanout operation rejects the envelope instead of
    /// answering it. An operation that exists only as an optimization — the
    /// PromQL push-downs — uses this to tell a rolling upgrade apart from a real
    /// failure, and falls back to the unoptimized path rather than failing the
    /// query.
    pub fn is_unsupported_operation(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::InvalidMessage
                | ErrorKind::UnknownMessageType
                | ErrorKind::UnsupportedFeatures
        )
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(self.kind as u8);
        write_byte_slice(buf, self.message.as_str().as_ref());
    }

    pub fn deserialize(buf: &[u8]) -> ValkeyResult<(Self, &[u8])> {
        if buf.len() < 2 {
            return Err(ValkeyError::Str("Buffer too small to contain Error"));
        }

        let kind = ErrorKind::try_from(buf[0])?;
        let mut buf = &buf[1..];

        let extra = try_read_byte_slice(&mut buf)
            .map_err(|_| ValkeyError::Str("Failed to read Error extra"))?;

        let extra = if !extra.is_empty() {
            String::from_utf8(extra.to_vec())
                .map_err(|_| ValkeyError::Str("Invalid UTF-8 in Error extra"))?
        } else {
            String::new()
        };

        Ok((
            Self {
                kind,
                message: extra,
            },
            buf,
        ))
    }
}

impl TryFrom<u8> for ErrorKind {
    type Error = ValkeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ErrorKind::InvalidMessage),
            1 => Ok(ErrorKind::NodeUnreachable),
            2 => Ok(ErrorKind::Timeout),
            3 => Ok(ErrorKind::UnknownMessageType),
            4 => Ok(ErrorKind::Permissions),
            5 => Ok(ErrorKind::KeyPermissions),
            6 => Ok(ErrorKind::Serialization),
            7 => Ok(ErrorKind::BadRequestId),
            8 => Ok(ErrorKind::Internal),
            9 => Ok(ErrorKind::ClusterMapMismatch),
            10 => Ok(ErrorKind::UnsupportedFeatures),
            255 => Ok(ErrorKind::Custom),
            _ => {
                let msg = format!("Invalid error kind: {value}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

impl From<ErrorKind> for FanoutError {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: String::new(),
        }
    }
}

impl core::fmt::Display for ErrorKind {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        write!(fmt, "{}", self.as_str())
    }
}

impl core::fmt::Display for FanoutError {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        if self.message.is_empty() {
            write!(fmt, "{}", self.kind.as_str())
        } else {
            write!(fmt, "{}", self.message)
        }
    }
}

impl core::error::Error for FanoutError {
    fn description(&self) -> &str {
        if self.message.is_empty() {
            return self.kind.as_str();
        }
        &self.message
    }
    fn cause(&self) -> Option<&dyn core::error::Error> {
        None
    }
}

/// Converts an error string into a structured Error object.
/// Maps known error strings to specific ErrorKind variants,
/// or falls back to a general Failed error with the original message.
fn convert_from_string(err: &str) -> FanoutError {
    if err.is_empty() {
        return ErrorKind::Internal.into();
    }

    // `valkey_module`'s blanket `From<E: Error> for ValkeyError` prefixes "ERR ".
    // A shard-side error round-trips FanoutError -> ValkeyError -> FanoutError, so
    // strip that prefix before classifying; otherwise a known error string (e.g. a
    // key-permission denial) would be misclassified as a generic `Custom` error.
    let err = err.strip_prefix("ERR ").unwrap_or(err);

    match err {
        INVALID_MESSAGE_ERROR => ErrorKind::InvalidMessage.into(),
        KEY_PERMISSIONS_ERROR => ErrorKind::KeyPermissions.into(),
        PERMISSIONS_ERROR => ErrorKind::Permissions.into(),
        INTERNAL_ERROR => ErrorKind::Internal.into(),
        CLUSTER_MAP_MISMATCH_ERROR => ErrorKind::ClusterMapMismatch.into(),
        UNSUPPORTED_FEATURES_ERROR => ErrorKind::UnsupportedFeatures.into(),
        NODE_UNREACHABLE_ERROR => ErrorKind::NodeUnreachable.into(),
        UNKNOWN_MESSAGE_TYPE_ERROR => ErrorKind::UnknownMessageType.into(),
        SERIALIZATION_ERROR => FanoutError::serialization(String::new()),
        BAD_REQUEST_ID_ERROR => ErrorKind::BadRequestId.into(),
        TIMEOUT_ERROR => ErrorKind::Timeout.into(),
        error_consts::COMMAND_DESERIALIZATION_ERROR => ErrorKind::Serialization.into(),
        error_consts::NO_CLUSTER_NODES_AVAILABLE => ErrorKind::NodeUnreachable.into(),
        error_consts::KEY_READ_PERMISSION_ERROR => ErrorKind::KeyPermissions.into(),
        // A read-permission denial for one of the matched keys. Keep the detailed
        // message so the client sees *why* the multi-shard command failed closed
        // instead of a generic aggregate error.
        error_consts::PERMISSION_DENIED => FanoutError::key_permissions(err.to_string()),
        NO_CLUSTER_NODES_AVAILABLE => ErrorKind::NodeUnreachable.into(),
        _ => FanoutError::custom(err.to_string()),
    }
}

impl From<&str> for FanoutError {
    fn from(value: &str) -> Self {
        convert_from_string(value)
    }
}

impl From<String> for FanoutError {
    fn from(value: String) -> Self {
        convert_from_string(&value)
    }
}

impl From<ValkeyError> for FanoutError {
    fn from(value: ValkeyError) -> Self {
        match value {
            ValkeyError::Str(msg) => convert_from_string(msg),
            ValkeyError::String(err) => convert_from_string(&err),
            _ => FanoutError {
                kind: ErrorKind::Internal,
                message: value.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_consts;
    use valkey_module::ValkeyError;

    #[test]
    fn test_fanout_error_constructors() {
        let error = FanoutError::custom("test failure".to_string());
        assert_eq!(error.kind, ErrorKind::Custom);
        assert_eq!(error.message, "test failure");

        let error = FanoutError::serialization("serialization issue".to_string());
        assert_eq!(error.kind, ErrorKind::Serialization);
        assert_eq!(error.message, "serialization issue");

        let error = FanoutError::key_permissions("key access denied".to_string());
        assert_eq!(error.kind, ErrorKind::KeyPermissions);
        assert_eq!(error.message, "key access denied");
    }

    #[test]
    fn test_fanout_error_display() {
        // Error with a message
        let error = FanoutError::custom("custom message".to_string());
        assert_eq!(format!("{error}"), "custom message");

        // Error without a message
        let error = FanoutError {
            kind: ErrorKind::Internal,
            message: String::new(),
        };
        assert_eq!(format!("{error}"), "Internal error");
    }

    #[test]
    fn test_fanout_error_error_trait() {
        // Test with an empty message
        let error = FanoutError::timeout();
        assert_eq!(error.to_string(), TIMEOUT_ERROR);
    }

    #[test]
    fn test_serialize_deserialize_with_message() {
        let original = FanoutError::custom("test error message".to_string());
        let mut buf = Vec::new();
        original.serialize(&mut buf);

        let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
        assert_eq!(original, deserialized);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_serialize_deserialize_empty_message() {
        let original = FanoutError {
            kind: ErrorKind::Timeout,
            message: String::new(),
        };
        let mut buf = Vec::new();
        original.serialize(&mut buf);

        let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
        assert_eq!(original, deserialized);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_serialize_deserialize_all_error_kinds() {
        let error_kinds = [
            ErrorKind::InvalidMessage,
            ErrorKind::NodeUnreachable,
            ErrorKind::Timeout,
            ErrorKind::UnknownMessageType,
            ErrorKind::Permissions,
            ErrorKind::KeyPermissions,
            ErrorKind::Serialization,
            ErrorKind::BadRequestId,
            ErrorKind::Internal,
            ErrorKind::ClusterMapMismatch,
            ErrorKind::UnsupportedFeatures,
            ErrorKind::Custom,
        ];

        for kind in &error_kinds {
            let original = FanoutError {
                kind: *kind,
                message: format!("test message for {kind:?}"),
            };
            let mut buf = Vec::new();
            original.serialize(&mut buf);

            let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
            assert_eq!(original, deserialized);
            assert!(remaining.is_empty());
        }
    }

    #[test]
    fn test_deserialize_with_remaining_data() {
        let original = FanoutError::custom("test".to_string());
        let mut buf = Vec::new();
        original.serialize(&mut buf);
        buf.extend_from_slice(b"extra data");

        let (deserialized, remaining) = FanoutError::deserialize(&buf).unwrap();
        assert_eq!(original, deserialized);
        assert_eq!(remaining, b"extra data");
    }

    #[test]
    fn test_deserialize_errors() {
        // Buffer too small
        let result = FanoutError::deserialize(&[]);
        assert!(result.is_err());

        let result = FanoutError::deserialize(&[0]);
        assert!(result.is_err());

        // Invalid error kind
        let mut buf = vec![151]; // Invalid error kind
        buf.push(0); // Empty message length
        let result = FanoutError::deserialize(&buf);
        assert!(result.is_err());

        // Invalid UTF-8 in a message
        let mut buf = vec![0]; // Valid error kind
        buf.push(3); // Message length = 3
        buf.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // Invalid UTF-8
        let result = FanoutError::deserialize(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_buffer_too_small_for_message() {
        let mut buf = vec![0]; // Valid error kind (Failed)
        buf.push(10); // Message length = 10, but we won't provide 10 bytes
        buf.extend_from_slice(b"short"); // Only 5 bytes

        let result = FanoutError::deserialize(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_str() {
        // Test known error constants
        let error = FanoutError::from(error_consts::COMMAND_DESERIALIZATION_ERROR);
        assert_eq!(error.kind, ErrorKind::Serialization);
        assert_eq!(error.message, "");

        let error = FanoutError::from(error_consts::NO_CLUSTER_NODES_AVAILABLE);
        assert_eq!(error.kind, ErrorKind::NodeUnreachable);
        assert_eq!(error.message, "");

        let error = FanoutError::from(error_consts::KEY_READ_PERMISSION_ERROR);
        assert_eq!(error.kind, ErrorKind::KeyPermissions);
        assert_eq!(error.message, "");

        // Test unknown error
        let error = FanoutError::from("unknown error message");
        assert_eq!(error.kind, ErrorKind::Custom);
        assert_eq!(error.message, "unknown error message");

        // Test empty string
        let error = FanoutError::from("");
        assert_eq!(error.kind, ErrorKind::Internal);
        assert_eq!(error.message, "");
    }

    #[test]
    fn test_from_valkey_error() {
        let valkey_error = ValkeyError::Str("test error");
        let fanout_error = FanoutError::from(valkey_error);
        assert_eq!(fanout_error.kind, ErrorKind::Custom);
        assert_eq!(fanout_error.message, "test error");

        let valkey_error = ValkeyError::String("another test".to_string());
        let fanout_error = FanoutError::from(valkey_error);
        assert_eq!(fanout_error.kind, ErrorKind::Custom);
        assert_eq!(fanout_error.message, "another test");

        // Test with a known error constant
        let valkey_error = ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR);
        let fanout_error: FanoutError = valkey_error.into();
        assert_eq!(fanout_error.kind, ErrorKind::Serialization);
        assert_eq!(fanout_error.message, "");
    }

    #[test]
    fn test_convert_from_string_edge_cases() {
        // Test specific error constants
        assert_eq!(
            convert_from_string(PERMISSIONS_ERROR).kind,
            ErrorKind::Permissions
        );
        assert_eq!(
            convert_from_string(UNKNOWN_MESSAGE_TYPE_ERROR).kind,
            ErrorKind::UnknownMessageType
        );
        assert_eq!(
            convert_from_string(SERIALIZATION_ERROR).kind,
            ErrorKind::Serialization
        );
        assert_eq!(
            convert_from_string(BAD_REQUEST_ID_ERROR).kind,
            ErrorKind::BadRequestId
        );
        assert_eq!(convert_from_string(TIMEOUT_ERROR).kind, ErrorKind::Timeout);
        assert_eq!(
            convert_from_string(NODE_UNREACHABLE_ERROR).kind,
            ErrorKind::NodeUnreachable
        );
        assert_eq!(
            convert_from_string(INTERNAL_ERROR).kind,
            ErrorKind::Internal
        );
    }

    #[test]
    fn test_error_kind_repr_u8() {
        // Test that the repr(u8) values match what we expect
        assert_eq!(ErrorKind::InvalidMessage as u8, 0);
        assert_eq!(ErrorKind::NodeUnreachable as u8, 1);
        assert_eq!(ErrorKind::Timeout as u8, 2);
        assert_eq!(ErrorKind::UnknownMessageType as u8, 3);
        assert_eq!(ErrorKind::Permissions as u8, 4);
        assert_eq!(ErrorKind::KeyPermissions as u8, 5);
        assert_eq!(ErrorKind::Serialization as u8, 6);
        assert_eq!(ErrorKind::BadRequestId as u8, 7);
        assert_eq!(ErrorKind::Internal as u8, 8);
        assert_eq!(ErrorKind::ClusterMapMismatch as u8, 9);
        assert_eq!(ErrorKind::UnsupportedFeatures as u8, 10);
        assert_eq!(ErrorKind::Custom as u8, 255);
    }
}
