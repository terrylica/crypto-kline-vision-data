// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! DedupEngine: bounded AHashSet with FIFO eviction for kline deduplication.
//!
//! Replaces Python's `set + deque` pattern with a single Rust structure
//! using AHashSet for O(1) lookups and VecDeque for FIFO eviction order.

use std::collections::VecDeque;

use ahash::AHashSet;
use pyo3::prelude::*;

use crate::types::DedupKey;

/// PyO3-exposed dedup engine with bounded capacity and FIFO eviction.
///
/// When the set reaches `max_capacity`, the oldest key is evicted before
/// inserting a new one. This bounds memory usage regardless of stream duration.
#[pyclass]
pub struct PyDedupEngine {
    seen: AHashSet<DedupKey>,
    order: VecDeque<DedupKey>,
    max_capacity: usize,
}

#[pymethods]
impl PyDedupEngine {
    #[new]
    fn new(max_capacity: usize) -> Self {
        Self {
            seen: AHashSet::with_capacity(max_capacity),
            order: VecDeque::with_capacity(max_capacity),
            max_capacity,
        }
    }

    /// Check if key exists, insert if not. Returns `True` if DUPLICATE (already seen).
    ///
    /// Usage pattern: `if engine.check_and_insert(...): continue  # skip duplicate`
    fn check_and_insert(&mut self, symbol: String, interval: String, open_time_ms: i64) -> bool {
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
    fn contains(&self, symbol: &str, interval: &str, open_time_ms: i64) -> bool {
        let key: DedupKey = (symbol.to_owned(), interval.to_owned(), open_time_ms);
        self.seen.contains(&key)
    }

    fn __len__(&self) -> usize {
        self.seen.len()
    }

    /// Remove all entries.
    fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_new_key() {
        let mut engine = PyDedupEngine::new(10);
        let is_dup = engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        assert!(!is_dup);
        assert_eq!(engine.__len__(), 1);
    }

    #[test]
    fn test_duplicate_detected() {
        let mut engine = PyDedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        let is_dup = engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        assert!(is_dup);
        assert_eq!(engine.__len__(), 1);
    }

    #[test]
    fn test_different_symbols_not_duplicate() {
        let mut engine = PyDedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        let is_dup = engine.check_and_insert("ETHUSDT".into(), "1h".into(), 1000);
        assert!(!is_dup);
        assert_eq!(engine.__len__(), 2);
    }

    #[test]
    fn test_fifo_eviction_at_capacity() {
        let mut engine = PyDedupEngine::new(3);
        engine.check_and_insert("A".into(), "1h".into(), 1);
        engine.check_and_insert("B".into(), "1h".into(), 2);
        engine.check_and_insert("C".into(), "1h".into(), 3);
        // At capacity — inserting D evicts A
        engine.check_and_insert("D".into(), "1h".into(), 4);
        assert_eq!(engine.__len__(), 3);
        assert!(!engine.contains("A", "1h", 1)); // evicted
        assert!(engine.contains("D", "1h", 4)); // present
    }

    #[test]
    fn test_contains_without_insert() {
        let mut engine = PyDedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        assert!(engine.contains("BTCUSDT", "1h", 1000));
        assert!(!engine.contains("ETHUSDT", "1h", 1000));
    }

    #[test]
    fn test_clear() {
        let mut engine = PyDedupEngine::new(10);
        engine.check_and_insert("BTCUSDT".into(), "1h".into(), 1000);
        engine.clear();
        assert_eq!(engine.__len__(), 0);
        assert!(!engine.contains("BTCUSDT", "1h", 1000));
    }
}
