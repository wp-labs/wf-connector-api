# wf-connector-api

[![Crates.io](https://img.shields.io/crates/v/wf-connector-api.svg)](https://crates.io/crates/wf-connector-api)
[![CI](https://img.shields.io/github/actions/workflow/status/wp-labs/wf-connector-api/ci.yml?branch=main)](https://github.com/wp-labs/wf-connector-api/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/wp-labs/wf-connector-api/graph/badge.svg?token=6SVCXBHB6B)](https://codecov.io/gh/wp-labs/wf-connector-api)
[![Crates.io downloads](https://img.shields.io/crates/d/wf-connector-api)](https://crates.io/crates/wf-connector-api)
![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)
[![Rust Edition](https://img.shields.io/badge/edition-2021-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2021/index.html)

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
