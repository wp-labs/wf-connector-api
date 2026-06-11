# wf-connector-api

Minimal Arrow-native connector API for [warp-fusion](https://github.com/wp-labs/warp-fusion).

## Overview

```rust
use wf_connector_api::{BatchSource, SourceError, SourceResult};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

struct MySource;

#[async_trait]
impl BatchSource for MySource {
    async fn receive_batch(&mut self) -> SourceResult<Vec<(String, RecordBatch)>> {
        // Produce (stream_name, RecordBatch) pairs
        Ok(Vec::new())
    }

    fn source_name(&self) -> &str { "my_source" }
}
```

## Relationship with `wp-connector-api`

| | wp-connector-api | wf-connector-api |
|---|---|---|
| Source data | `SourceEvent { payload: RawData }` | `(stream, RecordBatch)` |
| Consumer | parse pipeline (WPL) | CEP engine (warp-fusion) |
| Error model | own error types | orion-error |

`wp-connectors` can implement both traits for the same connector, sharing connection logic.

## License

Apache-2.0
