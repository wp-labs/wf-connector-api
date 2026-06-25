# Changelog

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
