// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! DedupEngine: bounded AHashSet with FIFO eviction for kline deduplication.
//!
//! Replaces Python's `set + deque` pattern with a single Rust structure
//! using AHashSet for O(1) lookups and VecDeque for FIFO eviction order.
//!
//! Core logic lives in `DedupEngine` (plain Rust, no PyO3 dependency).
//! `PyDedupEngine` is a thin PyO3 wrapper, gated behind `feature = "python"`.

use std::collections::VecDeque;

use ahash::AHashSet;

use crate::types::DedupKey;

/// Core dedup engine — plain Rust, no PyO3 dependency.
///
/// When the set reaches `max_capacity`, the oldest key is evicted before
/// inserting a new one. This bounds memory usage regardless of stream duration.
pub struct DedupEngine {
    seen: AHashSet<DedupKey>,
    order: VecDeque<DedupKey>,
    max_capacity: usize,
}

impl DedupEngine {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            seen: AHashSet::with_capacity(max_capacity),
            order: VecDeque::with_capacity(max_capacity),
            max_capacity,
        }
    }

    /// Check if key exists, insert if not. Returns `true` if DUPLICATE (already seen).
    pub fn check_and_insert(&mut self, symbol: String, interval: String, open_time_ms: i64) -> bool {
        let key: DedupKey = (symbol, interval, open_time_ms);

        if self.seen.contains(&key) {
            return true; // duplicate
        }

        // Evict oldest if at capacity
        if self.seen.len() >= self.max_capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }

        self.seen.insert(key.clone());
        self.order.push_back(key);
        false // new key
    }

    /// Check if a key exists without inserting.
    pub fn contains(&self, symbol: &str, interval: &str, open_time_ms: i64) -> bool {
        let key: DedupKey = (symbol.to_owned(), interval.to_owned(), open_time_ms);
        self.seen.contains(&key)
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
    }
}

// ---------------------------------------------------------------------------
// PyO3 wrapper — only compiled with `feature = "python"`
// ---------------------------------------------------------------------------

#[cfg(feature = "python")]
mod py {
    use pyo3::prelude::*;
    use super::DedupEngine;

    /// PyO3-exposed wrapper around `DedupEngine`.
    #[pyclass]
    pub struct PyDedupEngine {
        inner: DedupEngine,
    }

    #[pymethods]
    impl PyDedupEngine {
        #[new]
        pub fn new(max_capacity: usize) -> Self {
            Self {
                inner: DedupEngine::new(max_capacity),
            }
        }

        /// Check if key exists, insert if not. Returns `True` if DUPLICATE (already seen).
        ///
        /// Usage pattern: `if engine.check_and_insert(...): continue  # skip duplicate`
        pub fn check_and_insert(&mut self, symbol: String, interval: String, open_time_ms: i64) -> bool {
            self.inner.check_and_insert(symbol, interval, open_time_ms)
        }

        /// Check if a key exists without inserting.
        pub fn contains(&self, symbol: &str, interval: &str, open_time_ms: i64) -> bool {
            self.inner.contains(symbol, interval, open_time_ms)
        }

        pub fn __len__(&self) -> usize {
            self.inner.len()
        }

        /// Remove all entries.
        pub fn clear(&mut self) {
            self.inner.clear();
        }
    }
}

#[cfg(feature = "python")]
pub use py::PyDedupEngine;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_new_key() {
        let mut engine = DedupEngine::new(10);
        let is_dup = engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        assert!(!is_dup);
        assert_eq!(engine.len(), 1);
    }

    #[test]
    fn test_duplicate_detected() {
        let mut engine = DedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        let is_dup = engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        assert!(is_dup);
        assert_eq!(engine.len(), 1);
    }

    #[test]
    fn test_different_symbols_not_duplicate() {
        let mut engine = DedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        let is_dup = engine.check_and_insert("ETHUSDT".into(), "1h".into(), 1000);
        assert!(!is_dup);
        assert_eq!(engine.len(), 2);
    }

    #[test]
    fn test_fifo_eviction_at_capacity() {
        let mut engine = DedupEngine::new(3);
        engine.check_and_insert("A".into(), "1h".into(), 1);
        engine.check_and_insert("B".into(), "1h".into(), 2);
        engine.check_and_insert("C".into(), "1h".into(), 3);
        // At capacity — inserting D evicts A
        engine.check_and_insert("D".into(), "1h".into(), 4);
        assert_eq!(engine.len(), 3);
        assert!(!engine.contains("A", "1h", 1)); // evicted
        assert!(engine.contains("D", "1h", 4)); // present
    }

    #[test]
    fn test_contains_without_insert() {
        let mut engine = DedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        assert!(engine.contains("BTCUSDT", "1h", 1000));
        assert!(!engine.contains("ETHUSDT", "1h", 1000));
    }

    #[test]
    fn test_clear() {
        let mut engine = DedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        engine.clear();
        assert_eq!(engine.len(), 0);
        assert!(!engine.contains("BTCUSDT", "1h", 1000));
    }
}
