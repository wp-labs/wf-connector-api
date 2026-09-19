# Changelog

## [0.3.0] — 2026-09-19

### ⚠️ BREAKING CHANGES

- `arrow` 59 → 60. Arrow types are part of this crate's public API
  (`BatchSource::receive_batch() -> SourceResult<Vec<RecordBatch>>`), and Cargo treats the major
  version of a dependency that appears in the public API as part of the public API itself. This is
  therefore a breaking upgrade: consumers must move to `arrow` 60 **in the same change**. A graph
  holding both 59 and 60 has two distinct `RecordBatch` types, so passing a batch returned by this
  crate into arrow-59 code fails to compile.

### Changed

- Version `0.2.0` → `0.3.0`, matching the `arrow` 60 breaking upgrade above.
- README: add the standard badge set (crates.io, CI, codecov, crates.io downloads, license, Rust
  edition). The license badge is rendered without a link because the repository has no `LICENSE`
  file even though `Cargo.toml` declares `license = "Apache-2.0"`.

### Added

- 10 unit tests (the crate previously had none), covering three areas:
  - **Stable error codes**: every leaf variant is pinned to its `stable_code`
    (`sys.wf_connector.eof` / `not_data` / `io` / `connect` / `decode` / `not_found`) and to
    `ErrorCategory::Sys`, asserted with orion-error's own `dev::testing::assert_err_identity`.
  - **Error model**: `err_detail()` stores the detail and leaves no source, and `Display` includes
    it; `err_source()` keeps the underlying `io::Error`; `SourceError` is `Send + Sync`; and
    `StructError` is *not* a `std::error::Error`, so it must go through `into_boxed_std()`.
  - **`BatchSource` lifecycle**: the default `start()` / `close()` bodies are no-ops and `close()`
    is idempotent; real `RecordBatch`es pass through unchanged; an exhausted source reports
    `SourceReason::EOF`; the documented “empty `Vec` means no data right now, `EOF` means the stream
    ended” contract; and a source is usable behind `Box<dyn BatchSource>`.
  - The tests add no dependency: a minimal `Waker::noop()`-based `block_on` drives the futures,
    which are ready on their first poll.

## [0.2.0] — 2026-06-25

### Change
 - arraw up 59
 
## [0.1.0] — 2026-06-11

### Added

- `BatchSource` trait — lifecycle-aware Arrow-native source API
  - `start()` — initialize connection
  - `receive_batch()` — pull `Vec<RecordBatch>`
  - `close()` — release resources (idempotent)
  - `identifier()` — unique instance identifier
- `SourceReason` enum with `orion-error` derive
  - `EOF` — explicit end-of-stream signal
  - `NotData` — temporarily no data available
  - `Io`, `Connect`, `Decode`, `NotFound` — transport/format errors
  - `General(UnifiedReason)` — catch-all
- `SourceError` / `SourceResult<T>` type aliases
- `err_detail()` / `err_source()` convenience methods
- `BatchSink` placeholder (TBD)
