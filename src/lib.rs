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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field as ArrowField, Schema};
    use orion_error::dev::testing::assert_err_identity;
    use orion_error::reason::ErrorCategory;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    /// The futures produced by this crate are ready on the first poll, so a
    /// plain `block_on` is enough — no async runtime needs to be pulled in
    /// just for the tests.
    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(out) => return out,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "n",
            DataType::Int32,
            false,
        )]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        RecordBatch::try_new(schema, vec![array]).expect("test batch should be valid")
    }

    // -- error model ---------------------------------------------------------

    #[test]
    fn every_leaf_reason_exposes_its_stable_code() {
        let cases = [
            (SourceReason::EOF, "sys.wf_connector.eof"),
            (SourceReason::NotData, "sys.wf_connector.not_data"),
            (SourceReason::Io, "sys.wf_connector.io"),
            (SourceReason::Connect, "sys.wf_connector.connect"),
            (SourceReason::Decode, "sys.wf_connector.decode"),
            (SourceReason::NotFound, "sys.wf_connector.not_found"),
        ];

        for (reason, code) in cases {
            let err = reason.err_detail("why");
            assert_err_identity(&err, code, ErrorCategory::Sys);
        }
    }

    #[test]
    fn err_detail_stores_the_detail_and_has_no_source() {
        let err = SourceReason::Decode.err_detail("bad frame");

        assert_eq!(err.detail().as_deref(), Some("bad frame"));
        assert_eq!(err.reason(), &SourceReason::Decode);
        assert!(err.source_ref().is_none());
        assert!(err.to_string().contains("bad frame"), "display = {err}");
    }

    #[test]
    fn err_source_keeps_the_underlying_error() {
        let io = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe closed");
        let err = SourceReason::Io.err_source(io);

        let source = err.source_ref().expect("the underlying error must be kept");
        assert!(
            source.to_string().contains("pipe closed"),
            "source = {source}"
        );
        assert!(err.detail().is_none());
    }

    #[test]
    fn helpers_return_the_declared_aliases() {
        fn takes_source_error(_: SourceError) {}
        fn takes_source_result(_: SourceResult<()>) {}

        takes_source_error(SourceReason::Connect.err_detail("refused"));
        takes_source_result(Ok(()));
    }

    #[test]
    fn source_error_is_send_sync_and_convertible_to_a_std_error() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SourceError>();
        assert_send_sync::<SourceReason>();

        // `StructError` 自身不实现 `std::error::Error`，需要显式转换后才能当 std 错误传递。
        let boxed: Box<dyn StdError + Send + Sync + 'static> = SourceReason::Decode
            .err_detail("bad frame")
            .into_boxed_std();
        assert!(boxed.to_string().contains("bad frame"));
    }

    // -- BatchSource ---------------------------------------------------------

    /// Implements only the two required methods, so `start()` / `close()`
    /// exercise their default bodies.
    struct MinimalSource {
        remaining: Vec<RecordBatch>,
    }

    #[async_trait]
    impl BatchSource for MinimalSource {
        async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
            match self.remaining.pop() {
                Some(batch) => Ok(vec![batch]),
                None => Err(SourceReason::EOF.err_detail("no more batches")),
            }
        }

        fn identifier(&self) -> &str {
            "minimal"
        }
    }

    #[test]
    fn lifecycle_defaults_are_no_ops_and_idempotent() {
        let mut source = MinimalSource { remaining: vec![] };

        block_on(source.start()).expect("default start() must succeed");
        block_on(source.close()).expect("default close() must succeed");
        block_on(source.close()).expect("close() must stay safe when called repeatedly");
        assert_eq!(source.identifier(), "minimal");
    }

    #[test]
    fn batches_pass_through_unchanged() {
        let mut source = MinimalSource {
            remaining: vec![batch(vec![1, 2, 3])],
        };

        let got = block_on(source.receive_batch()).expect("the batch should be produced");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].num_rows(), 3);
        assert_eq!(got[0].num_columns(), 1);
    }

    #[test]
    fn exhausted_source_reports_eof() {
        let mut source = MinimalSource { remaining: vec![] };

        let err = block_on(source.receive_batch()).expect_err("an exhausted source must error");
        assert_eq!(err.reason(), &SourceReason::EOF);
        assert_eq!(err.detail().as_deref(), Some("no more batches"));
    }

    #[test]
    fn a_source_may_report_no_data_before_yielding_batches() {
        struct PollingSource {
            polls: usize,
            batch: Option<RecordBatch>,
        }

        #[async_trait]
        impl BatchSource for PollingSource {
            async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
                self.polls += 1;
                match self.polls {
                    // 第一次：暂无数据 —— 调用方应重试，而不是当成流结束
                    1 => Ok(vec![]),
                    // 第二次：给出数据
                    2 => Ok(vec![self.batch.take().expect("the batch is still pending")]),
                    _ => Err(SourceReason::EOF.err_detail("stream ended")),
                }
            }

            fn identifier(&self) -> &str {
                "polling"
            }
        }

        let mut source = PollingSource {
            polls: 0,
            batch: Some(batch(vec![9])),
        };

        assert!(
            block_on(source.receive_batch())
                .expect("first poll is Ok")
                .is_empty(),
            "an empty Vec means 'no data right now'"
        );
        assert_eq!(block_on(source.receive_batch()).expect("data").len(), 1);
        assert_eq!(
            block_on(source.receive_batch())
                .expect_err("then EOF")
                .reason(),
            &SourceReason::EOF
        );
    }

    #[test]
    fn a_source_is_usable_behind_a_trait_object() {
        let mut source: Box<dyn BatchSource> = Box::new(MinimalSource {
            remaining: vec![batch(vec![7])],
        });

        let got = block_on(source.receive_batch()).expect("the batch should be produced");
        assert_eq!(got[0].num_rows(), 1);
        block_on(source.start()).expect("default start() must succeed");
        block_on(source.close()).expect("default close() must succeed");
    }
}
