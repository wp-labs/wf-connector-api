# wf-connector-api

Minimal Arrow-native connector API for [warp-fusion](https://github.com/wp-labs/warp-fusion).

## Overview

```rust
use wf_connector_api::{BatchSource, SourceResult};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

struct MySource;

#[async_trait]
impl BatchSource for MySource {
    async fn start(&mut self) -> SourceResult<()> { Ok(()) }

    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
        Ok(vec![])
    }

    async fn close(&mut self) -> SourceResult<()> { Ok(()) }

    fn identifier(&self) -> &str { "my_source" }
}
```

## Lifecycle

```
start() → receive_batch() loop → close()
```

- `start()` — initialize (connect, subscribe, bind)
- `receive_batch()` — pull data; empty Vec = no data right now, `EOF` error = stream ended
- `close()` — release resources; idempotent

## Relationship with `wp-connector-api`

| | wp-connector-api | wf-connector-api |
|---|---|---|
| Source data | `SourceEvent { payload: RawData }` | `RecordBatch` (Arrow columnar) |
| Consumer | parse pipeline (WPL) | CEP engine (warp-fusion) |
| Lifecycle | `start()` / `receive()` / `close()` | `start()` / `receive_batch()` / `close()` |
| Error model | `SourceResult<T>` (orion-error) | `SourceResult<T>` (orion-error) |

`wp-connectors` can implement both traits for the same connector, sharing connection logic.

## License

Apache-2.0
