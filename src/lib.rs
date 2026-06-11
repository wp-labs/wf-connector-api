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
//! `wf-connector-api` fills this gap for Arrow-native source consumption.
//! (Sink output uses the existing `wp-connector-api` `SinkRuntime` —
//! adding `send_batch()` to it is sufficient, no new trait needed.)
//!
//! ## Relationship with `wp-connector-api`
//!
//! | | wp-connector-api | wf-connector-api |
//! |---|---|---|
//! | Source data | `SourceEvent { payload: RawData }` | `RecordBatch` (columnar) |
//! | Consumer | parse pipeline (WPL) | CEP engine (warp-fusion) |
//! | Error model | `SourceResult<T>` (orion-error) | `SourceResult<T>` (orion-error) |
//! | Lifecycle | `start()` / `receive()` / `close()` | `start()` / `receive_batch()` / `close()` |
//!
//! `wp-connectors` (the implementation crate) can implement BOTH traits
//! for the same connector (Kafka / File / TCP), sharing connection logic.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orion_error::conversion::ToStructError;
use orion_error::{OrionError, StructError, UnifiedReason};
use std::error::Error as StdError;

// -- Error -------------------------------------------------------------------

/// Connector error reason.
///
/// All leaf variants carry detail via `err_detail()`. `SourceError` wraps
/// each variant with a detail string and optional source error.
#[derive(Debug, Clone, PartialEq, OrionError)]
pub enum SourceReason {
    /// End of stream — no more data will be produced.
    #[orion_error(message = "end of stream", identity = "sys.wf_connector.eof")]
    EOF,
    /// No data currently available (not EOF); caller should retry.
    #[orion_error(message = "no data available", identity = "sys.wf_connector.not_data")]
    NotData,
    /// I/O error from the underlying transport.
    #[orion_error(message = "I/O error", identity = "sys.wf_connector.io")]
    Io,
    /// Failed to establish connection / bind / subscribe.
    #[orion_error(message = "connection error", identity = "sys.wf_connector.connect")]
    Connect,
    /// Message / frame decoding failed.
    #[orion_error(message = "decode error", identity = "sys.wf_connector.decode")]
    Decode,
    /// Referenced connector not found in registry.
    #[orion_error(
        message = "connector not found",
        identity = "sys.wf_connector.not_found"
    )]
    NotFound,
    /// Catch-all for unexpected errors.
    #[orion_error(transparent)]
    General(UnifiedReason),
}

impl SourceReason {
    /// Create an error with detail message.
    pub fn err_detail<S: Into<String>>(self, detail: S) -> SourceError {
        self.to_err().with_detail(detail.into())
    }

    /// Create an error with a source (chained) error.
    pub fn err_source<E>(self, source: E) -> SourceError
    where
        E: StdError + Send + Sync + 'static,
    {
        self.to_err().with_source(source)
    }
}

pub type SourceError = StructError<SourceReason>;
pub type SourceResult<T> = Result<T, SourceError>;

// -- Source ------------------------------------------------------------------

/// A batch-oriented data source that produces Arrow [`RecordBatch`]es.
///
/// # Lifecycle
///
/// 1. `start()` — initialize (connect, subscribe, bind)
/// 2. `receive_batch()` — pull data in a loop
/// 3. `close()` — release resources (unsubscribe, close connections)
///
/// `close()` must be idempotent — safe to call multiple times, even before `start()`.
///
/// # Empty vs EOF
///
/// - Return `Ok(vec![])` when no data is currently available (caller should retry).
/// - Return `Err(SourceReason::EOF.into())` when the stream has ended.
#[async_trait]
pub trait BatchSource: Send {
    /// Initialize the source. Called once before the first `receive_batch()`.
    ///
    /// Default is a no-op.
    async fn start(&mut self) -> SourceResult<()> {
        Ok(())
    }

    /// Receive zero or more [`RecordBatch`]es.
    ///
    /// An empty `Vec` means "no data right now" — the caller should poll again.
    /// An error with `SourceReason::EOF` means the stream has ended.
    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>>;

    /// Close the source and release all resources.
    ///
    /// Must be idempotent — safe to call multiple times or before `start()`.
    /// Default is a no-op.
    async fn close(&mut self) -> SourceResult<()> {
        Ok(())
    }

    /// Unique identifier for this source instance (logging / metrics).
    fn identifier(&self) -> &str;
}

// -- Sink (not needed as a separate trait) -----------------------------------
//
// Arrow-native sink output is handled by the existing `wp-connector-api`
// `SinkRuntime`. Adding a `send_batch()` method to `SinkRuntime` (which
// already has `send_record()`) is sufficient — no new trait required.
// File / Arrow IPC / TCP backends can natively accept RecordBatch.
