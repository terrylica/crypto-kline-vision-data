// ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
//! ws-mock integration tests: scripted Binance kline sequences through DedupEngine + detect_gap.
//!
//! Uses ws-mock's `forward_from_channel()` to script kline messages, then verifies
//! gap detection and dedup behavior at the Rust level.

use std::time::Duration;

use _reconciler_rs::dedup::DedupEngine;
use _reconciler_rs::gap_detector::detect_gap;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ws_mock::matchers::Any;
use ws_mock::ws_mock_server::WsMock;
use ws_mock::ws_mock_server::WsMockServer;

const HOUR_MS: i64 = 3_600_000;
const BASE_MS: i64 = 1_705_276_800_000; // 2024-01-15T00:00:00Z

/// Minimal Binance kline JSON structure for testing.
#[derive(Debug, Deserialize)]
struct BinanceKline {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "k")]
    kline: KlineData,
}

#[derive(Debug, Deserialize)]
struct KlineData {
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "t")]
    open_time_ms: i64,
    #[serde(rename = "x")]
    is_closed: bool,
}

fn make_kline_json(symbol: &str, interval: &str, open_time_ms: i64, is_closed: bool) -> String {
    format!(
        r#"{{"s":"{}","k":{{"i":"{}","t":{},"x":{},"o":"42000","h":"42100","l":"41900","c":"42050","v":"1500"}}}}"#,
        symbol, interval, open_time_ms, is_closed
    )
}

/// Collect kline messages from a ws-mock server, running through dedup + gap detection.
struct KlineCollector {
    dedup: DedupEngine,
    last_confirmed_ms: Option<i64>,
    gaps_detected: Vec<(i64, i64)>, // (gap_start_ms, capped_end_ms)
    updates_received: usize,
    duplicates_skipped: usize,
}

impl KlineCollector {
    fn new(max_capacity: usize) -> Self {
        Self {
            dedup: DedupEngine::new(max_capacity),
            last_confirmed_ms: None,
            gaps_detected: Vec::new(),
            updates_received: 0,
            duplicates_skipped: 0,
        }
    }

    fn process(&mut self, kline: &BinanceKline) {
        if !kline.kline.is_closed {
            return;
        }

        let open_time_ms = kline.kline.open_time_ms;

        // Gap detection
        if let Some(prev_ms) = self.last_confirmed_ms {
            let (has_gap, capped_end) =
                detect_gap(prev_ms, open_time_ms, HOUR_MS, 1440);
            if has_gap {
                self.gaps_detected.push((prev_ms + HOUR_MS, capped_end));
            }
        }

        // Dedup
        let is_dup = self.dedup.check_and_insert(
            kline.symbol.clone(),
            kline.kline.interval.clone(),
            open_time_ms,
        );
        if is_dup {
            self.duplicates_skipped += 1;
            return;
        }

        self.updates_received += 1;
        self.last_confirmed_ms = Some(open_time_ms);
    }
}

/// Helper: send kline messages via channel, receive and process through collector.
async fn run_scenario(
    messages: Vec<String>,
    max_capacity: usize,
) -> KlineCollector {
    let server = WsMockServer::start().await;
    let (tx, rx) = mpsc::channel::<Message>(messages.len() + 1);

    WsMock::new()
        .matcher(Any::new())
        .forward_from_channel(rx)
        .mount(&server)
        .await;

    // Connect client
    let (ws, _) = connect_async(server.uri().await)
        .await
        .expect("Failed to connect");
    let (mut _write, mut read) = ws.split();

    // Send all messages
    for msg in &messages {
        tx.send(Message::Text(msg.clone().into()))
            .await
            .expect("Failed to send");
    }
    drop(tx); // Signal no more messages

    // Receive and process
    let mut collector = KlineCollector::new(max_capacity);
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(500), read.next()).await {
        if let Message::Text(text) = msg {
            if let Ok(kline) = serde_json::from_str::<BinanceKline>(&text) {
                collector.process(&kline);
            }
        }
    }

    collector
}

// ---------------------------------------------------------------------------
// Scenario 37: Normal sequence — no gaps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_normal_sequence_no_gaps() {
    let messages: Vec<String> = (0..20)
        .map(|i| make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true))
        .collect();

    let collector = run_scenario(messages, 1440).await;

    assert_eq!(collector.updates_received, 20);
    assert!(collector.gaps_detected.is_empty());
    assert_eq!(collector.duplicates_skipped, 0);
}

// ---------------------------------------------------------------------------
// Scenario 38: Single gap (5 + skip(3) + 5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_gap() {
    let mut messages = Vec::new();
    // 5 consecutive klines: hours 0-4
    for i in 0..5 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }
    // Skip hours 5, 6, 7 — resume at hour 8
    for i in 8..13 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }

    let collector = run_scenario(messages, 1440).await;

    assert_eq!(collector.updates_received, 10);
    assert_eq!(collector.gaps_detected.len(), 1);
    // Gap from hour 5 (prev+1) to hour 8
    let (gap_start, gap_end) = collector.gaps_detected[0];
    assert_eq!(gap_start, BASE_MS + 5 * HOUR_MS);
    assert_eq!(gap_end, BASE_MS + 8 * HOUR_MS);
}

// ---------------------------------------------------------------------------
// Scenario 39: Multiple gaps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_gaps() {
    let mut messages = Vec::new();
    // 3 klines: hours 0-2
    for i in 0..3 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }
    // Skip hours 3, 4 — resume at 5
    for i in 5..8 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }
    // Skip hours 8-12 — resume at 13
    for i in 13..16 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }

    let collector = run_scenario(messages, 1440).await;

    assert_eq!(collector.updates_received, 9);
    assert_eq!(collector.gaps_detected.len(), 2);
}

// ---------------------------------------------------------------------------
// Scenario 40: Duplicate messages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_duplicate_messages() {
    let mut messages = Vec::new();
    for i in 0..5 {
        let msg = make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true);
        messages.push(msg.clone());
        messages.push(msg); // duplicate
    }

    let collector = run_scenario(messages, 1440).await;

    assert_eq!(collector.updates_received, 5);
    assert_eq!(collector.duplicates_skipped, 5);
    assert!(collector.gaps_detected.is_empty());
}

// ---------------------------------------------------------------------------
// Scenario 41: Server shutdown mid-stream (client gets close frame)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_server_shutdown_mid_stream() {
    let server = WsMockServer::start().await;
    let (tx, rx) = mpsc::channel::<Message>(10);

    WsMock::new()
        .matcher(Any::new())
        .forward_from_channel(rx)
        .mount(&server)
        .await;

    let (ws, _) = connect_async(server.uri().await)
        .await
        .expect("Failed to connect");
    let (mut _write, mut read) = ws.split();

    // Send 5 messages then drop sender (simulates server done)
    for i in 0..5 {
        tx.send(Message::Text(
            make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true).into(),
        ))
        .await
        .unwrap();
    }
    drop(tx);

    let mut collector = KlineCollector::new(1440);
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(500), read.next()).await {
        if let Message::Text(text) = msg {
            if let Ok(kline) = serde_json::from_str::<BinanceKline>(&text) {
                collector.process(&kline);
            }
        }
    }

    assert_eq!(collector.updates_received, 5);
    assert!(collector.gaps_detected.is_empty());
}

// ---------------------------------------------------------------------------
// Scenario 42: Delayed messages (all arrive, no false gaps)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delayed_messages_no_false_gaps() {
    let server = WsMockServer::start().await;
    let (tx, rx) = mpsc::channel::<Message>(20);

    WsMock::new()
        .matcher(Any::new())
        .forward_from_channel(rx)
        .mount(&server)
        .await;

    let (ws, _) = connect_async(server.uri().await)
        .await
        .expect("Failed to connect");
    let (mut _write, mut read) = ws.split();

    // Send with small delays
    let send_tx = tx.clone();
    tokio::spawn(async move {
        for i in 0..10 {
            send_tx
                .send(Message::Text(
                    make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true).into(),
                ))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        drop(send_tx);
    });
    drop(tx);

    let mut collector = KlineCollector::new(1440);
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(2), read.next()).await {
        if let Message::Text(text) = msg {
            if let Ok(kline) = serde_json::from_str::<BinanceKline>(&text) {
                collector.process(&kline);
            }
        }
    }

    assert_eq!(collector.updates_received, 10);
    assert!(collector.gaps_detected.is_empty());
}

// ---------------------------------------------------------------------------
// Scenario 43: Burst 10K messages — DedupEngine handles without panic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_burst_10k_messages() {
    let capacity = 1440;
    let mut dedup = DedupEngine::new(capacity);
    let mut updates = 0;

    // 10K unique klines
    for i in 0..10_000i64 {
        let is_dup = dedup.check_and_insert("BTCUSDT".into(), "1h".into(), BASE_MS + i * HOUR_MS);
        if !is_dup {
            updates += 1;
        }
    }

    assert_eq!(updates, 10_000);
    // Engine stays bounded at capacity
    assert!(dedup.len() <= capacity);
}

// ---------------------------------------------------------------------------
// Scenario 44: Cross-symbol interleave — gaps detected per-symbol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cross_symbol_interleave() {
    let mut messages = Vec::new();
    // Interleave BTCUSDT (consecutive) and ETHUSDT (with gap)
    for i in 0..5 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
        messages.push(make_kline_json("ETHUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }
    // ETHUSDT gap: skip hours 5,6,7
    for i in 5..10 {
        messages.push(make_kline_json("BTCUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }
    // ETHUSDT resumes at hour 8
    for i in 8..10 {
        messages.push(make_kline_json("ETHUSDT", "1h", BASE_MS + i * HOUR_MS, true));
    }

    // Process with per-symbol gap tracking
    let server = WsMockServer::start().await;
    let (tx, rx) = mpsc::channel::<Message>(messages.len() + 1);

    WsMock::new()
        .matcher(Any::new())
        .forward_from_channel(rx)
        .mount(&server)
        .await;

    let (ws, _) = connect_async(server.uri().await)
        .await
        .expect("Failed to connect");
    let (mut _write, mut read) = ws.split();

    for msg in &messages {
        tx.send(Message::Text(msg.clone().into())).await.unwrap();
    }
    drop(tx);

    // Track gaps per symbol
    let mut dedup = DedupEngine::new(1440);
    let mut last_confirmed: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut gaps_per_symbol: std::collections::HashMap<String, Vec<(i64, i64)>> =
        std::collections::HashMap::new();

    while let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(500), read.next()).await {
        if let Message::Text(text) = msg {
            if let Ok(kline) = serde_json::from_str::<BinanceKline>(&text) {
                if !kline.kline.is_closed {
                    continue;
                }
                let sym = &kline.symbol;
                let ot = kline.kline.open_time_ms;

                if let Some(&prev) = last_confirmed.get(sym) {
                    let (has_gap, capped) = detect_gap(prev, ot, HOUR_MS, 1440);
                    if has_gap {
                        gaps_per_symbol
                            .entry(sym.clone())
                            .or_default()
                            .push((prev + HOUR_MS, capped));
                    }
                }

                let is_dup = dedup.check_and_insert(sym.clone(), "1h".into(), ot);
                if !is_dup {
                    last_confirmed.insert(sym.clone(), ot);
                }
            }
        }
    }

    // BTCUSDT: no gaps (consecutive 0-9)
    assert!(
        gaps_per_symbol.get("BTCUSDT").map_or(true, |v| v.is_empty()),
        "BTCUSDT should have no gaps"
    );
    // ETHUSDT: 1 gap (hours 5-7 skipped)
    let eth_gaps = gaps_per_symbol.get("ETHUSDT").expect("ETHUSDT should have gaps");
    assert_eq!(eth_gaps.len(), 1);
}
