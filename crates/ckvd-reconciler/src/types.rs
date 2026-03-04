// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! Type aliases for the reconciler crate.

/// Dedup key: (symbol, interval, open_time_ms).
///
/// Uses millisecond timestamps (i64) to avoid expensive PyO3 datetime
/// conversion in the hot path. Python bridge converts
/// `datetime → int(dt.timestamp() * 1000)` before calling Rust.
pub type DedupKey = (String, String, i64);
