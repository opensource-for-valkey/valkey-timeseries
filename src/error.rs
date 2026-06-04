use crate::common::Sample;
use crate::error_consts;
use thiserror::Error;
use valkey_module::ValkeyError;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Enum for various errors in Tsdb.
pub enum TsdbError {
    #[error("Chunk at full capacity. Max capacity {0}.")]
    CapacityFull(usize),

    #[error("Invalid configuration. {0}")]
    InvalidConfiguration(String),

    #[error("Decoding error. {0}")]
    DecodingError(String),

    #[error("Encoding error. {0}")]
    EncodingError(String),

    #[error("Serialization error. {0}")]
    CannotSerialize(String),

    #[error("Cannot deserialize. {0}")]
    CannotDeserialize(String),

    #[error("Compression error. {0}")]
    CannotCompress(String),

    #[error("Decompression error. {0}")]
    CannotDecompress(String),

    #[error("{0}")] // need a better error
    DuplicateSample(String),

    #[error("Invalid metric: {0}")]
    InvalidMetric(String),

    #[error("Invalid compressed method. {0}")]
    InvalidCompression(String),

    #[error("Error removing range")]
    RemoveRangeError,

    #[error("Invalid number. {0}")]
    InvalidNumber(String),

    #[error("Invalid series selector. {0}")]
    InvalidSeriesSelector(String),

    #[error("Sample timestamp exceeds retention period")]
    SampleTooOld,

    #[error("Error adding sample. {0:?}")]
    CannotAddSample(Sample),

    #[error("{0}")]
    General(String),

    #[error("TSDB: error encoding chunk")]
    ChunkEncoding,

    #[error("TSDB: error decoding chunk")]
    ChunkDecoding,

    #[error("TSDB: error splitting chunk")]
    ChunkSplitError,

    #[error("End of stream")]
    EndOfStream,

    #[error("TSDB permissions error: {0}")]
    PermissionsError(String),

    #[error("TSDB forecast error: {0}")]
    ForecastError(String),
}

pub type TsdbResult<T = ()> = Result<T, TsdbError>;

impl From<&str> for TsdbError {
    fn from(s: &str) -> Self {
        TsdbError::General(s.to_string())
    }
}

impl From<String> for TsdbError {
    fn from(s: String) -> Self {
        TsdbError::General(s)
    }
}

impl From<ValkeyError> for TsdbError {
    fn from(e: ValkeyError) -> Self {
        let msg = e.to_string();
        valkey_error_string_to_tsdb_error(&msg)
    }
}

fn valkey_error_string_to_tsdb_error(s: &str) -> TsdbError {
    match s {
        error_consts::CHUNK_COMPRESSION => TsdbError::ChunkEncoding, // capacity unknown
        error_consts::CHUNK_DECOMPRESSION => TsdbError::ChunkDecoding,
        error_consts::SAMPLE_TOO_OLD => TsdbError::SampleTooOld,
        error_consts::DUPLICATE_SAMPLE => TsdbError::DuplicateSample(s.to_string()),
        _ => TsdbError::General(s.to_string()),
    }
}
