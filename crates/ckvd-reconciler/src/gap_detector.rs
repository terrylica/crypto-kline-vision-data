// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! Gap detection for kline streams using i64 millisecond timestamps.
//!
//! Pure arithmetic — no datetime types, no chrono dependency.
//! Python bridge converts `datetime → int(dt.timestamp() * 1000)` before calling.

use pyo3::prelude::*;

/// Detect if there is a gap between two consecutive kline timestamps.
///
/// Returns `(has_gap, capped_end_ms)`:
/// - `has_gap`: True if `current_ms - prev_ms > interval_ms`
/// - `capped_end_ms`: The gap end, capped at `prev_ms + interval_ms * max_gap_intervals`
///
/// # Arguments
/// * `prev_ms` - Previous confirmed open_time in milliseconds
/// * `current_ms` - Current update's open_time in milliseconds
/// * `interval_ms` - Candle interval in milliseconds (e.g. 3600000 for 1h)
/// * `max_gap_intervals` - Maximum gap size in intervals (caps large gaps)
#[pyfunction]
pub fn detect_gap(prev_ms: i64, current_ms: i64, interval_ms: i64, max_gap_intervals: i64) -> (bool, i64) {
    let gap = current_ms - prev_ms;

    if gap <= interval_ms {
        return (false, current_ms);
    }

    // Cap gap at max_gap_intervals
    let max_gap_ms = interval_ms * max_gap_intervals;
    let capped_end = if gap > max_gap_ms {
        prev_ms + max_gap_ms
    } else {
        current_ms
    };

    (true, capped_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR_MS: i64 = 3_600_000;

    #[test]
    fn test_no_gap_consecutive() {
        let (has_gap, _) = detect_gap(0, HOUR_MS, HOUR_MS, 1440);
        assert!(!has_gap);
    }

    #[test]
    fn test_gap_detected() {
        // 3-hour gap with 1h interval
        let (has_gap, capped_end) = detect_gap(0, 3 * HOUR_MS, HOUR_MS, 1440);
        assert!(has_gap);
        assert_eq!(capped_end, 3 * HOUR_MS);
    }

    #[test]
    fn test_gap_capped() {
        // 2000-interval gap, capped at 1440
        let (has_gap, capped_end) = detect_gap(0, 2000 * HOUR_MS, HOUR_MS, 1440);
        assert!(has_gap);
        assert_eq!(capped_end, 1440 * HOUR_MS);
    }

    #[test]
    fn test_exactly_one_interval_no_gap() {
        let (has_gap, _) = detect_gap(0, HOUR_MS, HOUR_MS, 1440);
        assert!(!has_gap);
    }

    #[test]
    fn test_two_intervals_is_gap() {
        let (has_gap, _) = detect_gap(0, 2 * HOUR_MS, HOUR_MS, 1440);
        assert!(has_gap);
    }

    #[test]
    fn test_negative_gap_no_gap() {
        // Current before previous (out-of-order)
        let (has_gap, _) = detect_gap(5 * HOUR_MS, 3 * HOUR_MS, HOUR_MS, 1440);
        assert!(!has_gap);
    }
}
