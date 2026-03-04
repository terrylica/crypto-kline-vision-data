// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! Rust-accelerated reconciler core for CKVD streaming.
//!
//! Provides:
//! - `PyDedupEngine`: bounded AHashSet with FIFO eviction for kline deduplication
//! - `detect_gap()`: pure i64 arithmetic gap detection for kline streams
//!
//! Python bridge (`src/ckvd/_reconciler.py`) handles datetime↔i64 conversion
//! and falls back to a pure-Python implementation when this extension is not built.

mod dedup;
mod gap_detector;
mod types;

use pyo3::prelude::*;

/// Python module: `ckvd._reconciler_rs`
#[pymodule]
fn _reconciler_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<dedup::PyDedupEngine>()?;
    m.add_function(wrap_pyfunction!(gap_detector::detect_gap, m)?)?;
    Ok(())
}
