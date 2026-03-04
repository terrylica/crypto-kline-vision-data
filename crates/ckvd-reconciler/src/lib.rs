// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! Rust-accelerated reconciler core for CKVD streaming.
//!
//! Provides:
//! - `DedupEngine`: bounded AHashSet with FIFO eviction for kline deduplication
//! - `detect_gap()`: pure i64 arithmetic gap detection for kline streams
//!
//! When built with `feature = "python"` (via maturin), also provides:
//! - `PyDedupEngine`: PyO3-exposed wrapper around `DedupEngine`
//! - `py_detect_gap()`: PyO3-exposed wrapper around `detect_gap()`
//!
//! Python bridge (`src/ckvd/_reconciler.py`) handles datetime↔i64 conversion
//! and falls back to a pure-Python implementation when this extension is not built.

pub mod dedup;
pub mod gap_detector;
pub mod types;

/// Python module: `ckvd._reconciler_rs`
///
/// Only compiled when the `python` feature is enabled (maturin sets this).
#[cfg(feature = "python")]
mod pymodule {
    use pyo3::prelude::*;

    #[pymodule]
    pub fn _reconciler_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<super::dedup::PyDedupEngine>()?;
        m.add_function(wrap_pyfunction!(super::gap_detector::py_detect_gap, m)?)?;
        Ok(())
    }
}
