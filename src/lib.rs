//! # wf-connector-api
//!
//! Minimal Arrow-native connector API for warp-fusion.
//!
//! ## Design
//!
//! `wp-connector-api` sources produce `SourceEvent { payload: RawData }`,
//! designed for downstream parse pipelines. CEP engines like warp-fusion
//! operate on Arrow `RecordBatch` directly.
//!
//! `wf-connector-api` fills this gap — one trait for sources, extensible
//! to sinks in the future (e.g. `BatchSink` for Arrow-native output).
//!
//! ## Relationship with `wp-connector-api`
//!
//! | | wp-connector-api | wf-connector-api |
//! |---|---|---|
//! | Source model | `SourceEvent { payload: RawData }` | `(stream, RecordBatch)` |
//! | Consumer | parse pipeline (WPL) | CEP engine (warp-fusion) |
//! | Sink model | `SinkFactory` (bytes/data records) | TBD (`BatchSink`) |
//! | Error model | `SourceResult<T>` (own error) | `SourceResult<T>` (orion-error) |
//!
//! `wp-connectors` (the implementation crate) can implement BOTH traits
//! for the same connector (Kafka / File / TCP), sharing connection logic.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orion_error::{OrionError, StructError, UnifiedReason};

// -- Error -------------------------------------------------------------------

/// Connector error reason — all variants carry detail via `with_detail()`.
#[derive(Debug, Clone, PartialEq, OrionError)]
pub enum SourceReason {
    /// I/O error from the underlying transport.
    #[orion_error(message = "I/O error", identity = "sys.wf_connector.io")]
    Io,
    /// Failed to establish connection / bind.
    #[orion_error(message = "connection error", identity = "sys.wf_connector.connect")]
    Connect,
    /// Message / frame decoding failed.
    #[orion_error(message = "decode error", identity = "sys.wf_connector.decode")]
    Decode,
    /// Referenced connector not found in registry.
    #[orion_error(message = "connector not found", identity = "sys.wf_connector.not_found")]
    NotFound,
    /// Catch-all for unexpected errors.
    #[orion_error(transparent)]
    General(UnifiedReason),
}

impl SourceReason {
    pub fn fail<T>(&self, detail: impl Into<String>) -> SourceResult<T> {
        let err = SourceError::from(self.clone()).with_detail(detail.into());
        Err(err)
    }
}

pub type SourceError = StructError<SourceReason>;
pub type SourceResult<T> = Result<T, SourceError>;

// -- Source ------------------------------------------------------------------

/// A batch-oriented data source that produces Arrow RecordBatches.
///
/// Each call returns zero or more `(stream_name, RecordBatch)` pairs.
/// An empty `Vec` means "no data available right now" (not EOF).
#[async_trait]
pub trait BatchSource: Send {
    /// Attempt to receive zero or more batches.
    async fn receive_batch(&mut self) -> SourceResult<Vec<(String, RecordBatch)>>;

    /// Human-readable source identifier for logging / metrics.
    fn source_name(&self) -> &str;
}

// -- Sink (TBD) --------------------------------------------------------------

// Future extension:
//
// ```ignore
// #[async_trait]
// pub trait BatchSink: Send {
//     async fn send_batch(&mut self, stream: &str, batch: &RecordBatch)
//         -> SourceResult<()>;
//     async fn flush(&mut self) -> SourceResult<()>;
// }
// ```
