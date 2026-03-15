fn test_target() -> LiveMarketTarget {
    LiveMarketTarget {
        market_id: "mkt".to_string(),
        symbol: "BTCUSDT".to_string(),
        timeframe: "5m".to_string(),
        token_id_yes: "yes".to_string(),
        token_id_no: "no".to_string(),
        end_date: None,
    }
}

fn test_exec_cfg() -> LiveExecutionConfig {
    LiveExecutionConfig {
        min_quote_usdc: 1.0,
        entry_slippage_bps: 18.0,
        exit_slippage_bps: 22.0,
        force_slippage_bps: None,
    }
}

fn test_book(min_order_size: f64) -> GatewayBookSnapshot {
    GatewayBookSnapshot {
        token_id: "yes".to_string(),
        min_order_size,
        tick_size: 0.01,
        best_bid: Some(0.49),
        best_ask: Some(0.50),
        best_bid_size: Some(100.0),
        best_ask_size: Some(100.0),
        bid_depth_top3: Some(300.0),
        ask_depth_top3: Some(300.0),
        bid_levels: Some(3),
        ask_levels: Some(3),
    }
}

#[test]
fn normalize_book_snapshot_values_from_cents_scale() {
    assert_eq!(normalize_book_probability(62.0, true), Some(0.62));
    assert_eq!(normalize_book_probability(99.0, true), Some(0.99));
    assert_eq!(normalize_book_tick_size(1.0, true), 0.01);
}

#[test]
fn normalize_book_snapshot_values_from_probability_scale() {
    assert_eq!(normalize_book_probability(0.62, false), Some(0.62));
    assert_eq!(normalize_book_tick_size(0.01, false), 0.01);
}

fn test_api_state() -> ApiState {
    let gateway_http_client = reqwest::Client::builder()
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    ApiState {
        ch_url: None,
        redis_prefix: "test".to_string(),
        redis_client: None,
        redis_manager: None,
        chart_cache: Arc::new(RwLock::new(HashMap::new())),
        live_position_states: Arc::new(RwLock::new(HashMap::new())),
        live_decision_guard: Arc::new(RwLock::new(HashMap::new())),
        live_events: Arc::new(RwLock::new(VecDeque::new())),
        live_event_seq: Arc::new(RwLock::new(0)),
        live_pending_orders: Arc::new(RwLock::new(HashMap::new())),
        live_runtime_snapshots: Arc::new(RwLock::new(HashMap::new())),
        live_runtime_controls: Arc::new(RwLock::new(HashMap::new())),
        live_persist_inflight: Arc::new(RwLock::new(HashSet::new())),
        live_execution_aggr_states: Arc::new(RwLock::new(HashMap::new())),
        live_rust_executor: Arc::new(RwLock::new(None)),
        live_rust_book_cache: Arc::new(RwLock::new(HashMap::new())),
        runtime_alert_throttle: Arc::new(RwLock::new(HashMap::new())),
        runtime_daily_report_sent: Arc::new(RwLock::new(HashSet::new())),
        strategy_heavy_slots: Arc::new(Semaphore::new(1)),
        strategy_live_source_slots: Arc::new(Semaphore::new(4)),
        runtime_event_samples: Arc::new(RwLock::new(HashMap::new())),
        gateway_http_client: Arc::new(gateway_http_client),
        live_kill_switch: Arc::new(RwLock::new(HashMap::new())),
    }
}

#[test]
fn taker_buy_uses_buy_amount_mode_with_min_order_size_floor() {
    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 50.0,
        "quote_size_usdc": 1.0,
        "tif": "FAK",
        "style": "taker"
    });
    let payload = decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&test_book(5.0)),
        None,
    )
    .expect("payload");
    let size = payload
        .get("size")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let price = payload
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let amount = payload
        .get("buy_amount_usdc")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let mode = payload
        .get("amount_mode")
        .and_then(Value::as_str)
        .unwrap_or_default();

    assert!(
        size >= 5.0,
        "taker buy must keep min_order_size floor, got {size}"
    );
    assert!(
        amount + 1e-9 >= size * price,
        "buy amount should cover min-order notional, got amount={amount}, size={size}, price={price}"
    );
    assert_eq!(mode, "buy_usdc");
}

fn env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

struct ScopedEnvVar {
    key: &'static str,
    prev: Option<String>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        unsafe {
            if let Some(prev) = &self.prev {
                std::env::set_var(self.key, prev);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

#[test]
fn live_snapshot_prefix_defaults_to_runtime_prefix() {
    let _guard = env_lock().lock().expect("env lock");
    let _snapshot_prefix = ScopedEnvVar::set("FORGE_FEV1_LIVE_SNAPSHOT_REDIS_PREFIX", "");
    assert_eq!(live_snapshot_redis_prefix("forge9820"), "forge9820");
    assert_eq!(live_snapshot_event_channel("forge9820"), "forge9820:snapshot:events");
}

#[test]
fn live_snapshot_prefix_can_override_runtime_prefix() {
    let _guard = env_lock().lock().expect("env lock");
    let _snapshot_prefix = ScopedEnvVar::set("FORGE_FEV1_LIVE_SNAPSHOT_REDIS_PREFIX", "forge");
    let prefix = live_snapshot_redis_prefix("forge9820");
    assert_eq!(prefix, "forge");
    assert_eq!(live_snapshot_event_channel(&prefix), "forge:snapshot:events");
}

#[test]
fn fixed_entry_quote_uses_buy_amount_mode_without_size_lock() {
    let _guard = env_lock().lock().expect("env lock");
    let _fixed_quote = ScopedEnvVar::set("FORGE_FEV1_FIXED_ENTRY_QUOTE_USDC", "5");
    let _fixed_shares = ScopedEnvVar::set("FORGE_FEV1_FIXED_ENTRY_SIZE_SHARES", "");

    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 50.0,
        "quote_size_usdc": 1.0
    });
    let payload = decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&test_book(5.0)),
        None,
    )
    .expect("payload");

    assert_eq!(
        payload.get("buy_amount_usdc").and_then(Value::as_f64),
        Some(5.0)
    );
    assert_eq!(
        payload.get("amount_mode").and_then(Value::as_str),
        Some("buy_usdc")
    );
    let notes = payload
        .get("execution_notes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        notes.iter().any(|v| v.as_str() == Some("force_entry_fixed_quote")),
        "expected fixed quote note, got {notes:?}"
    );
    assert!(
        !notes.iter().any(|v| v.as_str() == Some("force_entry_fixed_size")),
        "fixed quote entry should not size-lock, got {notes:?}"
    );
}

#[tokio::test]
async fn gate_live_decisions_accepts_fixed_entry_quote_when_required() {
    let _guard = env_lock().lock().expect("env lock");
    let _require = ScopedEnvVar::set("FORGE_FEV1_REQUIRE_FIXED_ENTRY_SIZE", "true");
    let _fixed_quote = ScopedEnvVar::set("FORGE_FEV1_FIXED_ENTRY_QUOTE_USDC", "5");
    let _fixed_shares = ScopedEnvVar::set("FORGE_FEV1_FIXED_ENTRY_SIZE_SHARES", "");

    let state = test_api_state();
    let decision = json!({
        "action": "enter",
        "side": "DOWN",
        "round_id": "SOLUSDT_5m_1",
        "ts_ms": chrono::Utc::now().timestamp_millis(),
        "decision_id": "intent-quote-1"
    });
    let (accepted, skipped, _) = gate_live_decisions(
        &state,
        "SOLUSDT",
        "5m",
        &[decision],
        &LiveTriggerMode::SignalPool,
        true,
    )
    .await;
    assert_eq!(accepted.len(), 1, "expected fixed USD sizing to satisfy gate");
    assert!(skipped.is_empty(), "unexpected skips: {skipped:?}");
}

#[tokio::test]
async fn gate_live_decisions_paper_mirror_rejects_add_action() {
    let state = test_api_state();
    let decision = json!({
        "action": "add",
        "side": "DOWN",
        "round_id": "SOLUSDT_5m_1",
        "ts_ms": chrono::Utc::now().timestamp_millis(),
        "decision_id": "intent-add-1"
    });
    let (accepted, skipped, _) = gate_live_decisions(
        &state,
        "SOLUSDT",
        "5m",
        &[decision],
        &LiveTriggerMode::PaperMirror,
        true,
    )
    .await;
    assert!(accepted.is_empty());
    assert_eq!(
        skipped
            .first()
            .and_then(|v| v.get("reason"))
            .and_then(Value::as_str),
        Some("trigger_mode_action_not_allowed")
    );
}

#[tokio::test]
async fn gate_live_decisions_paper_mirror_does_not_promote_enter_to_add() {
    let _guard = env_lock().lock().expect("env lock");
    let _require = ScopedEnvVar::set("FORGE_FEV1_REQUIRE_FIXED_ENTRY_SIZE", "false");
    let state = test_api_state();
    let now_ms = chrono::Utc::now().timestamp_millis();
    state
        .put_live_position_state(
            "SOLUSDT",
            "5m",
            LivePositionState {
                symbol: "SOLUSDT".to_string(),
                market_type: "5m".to_string(),
                state: "in_position".to_string(),
                side: Some("DOWN".to_string()),
                entry_round_id: Some("SOLUSDT_5m_prev".to_string()),
                entry_market_id: None,
                entry_token_id: None,
                entry_ts_ms: Some(now_ms - 10_000),
                entry_price_cents: Some(51.0),
                entry_quote_usdc: Some(5.0),
                net_quote_usdc: 5.0,
                vwap_entry_cents: Some(51.0),
                last_action: Some("enter".to_string()),
                last_reason: Some("paper_commit".to_string()),
                total_entries: 1,
                total_exits: 0,
                total_adds: 0,
                open_add_layers: 0,
                total_reduces: 0,
                realized_pnl_usdc: 0.0,
                last_fill_pnl_usdc: 0.0,
                position_cost_usdc: 5.0,
                position_size_shares: 1.0,
                updated_ts_ms: now_ms,
            },
        )
        .await;
    let decision = json!({
        "action": "enter",
        "side": "DOWN",
        "round_id": "SOLUSDT_5m_1",
        "ts_ms": now_ms,
        "decision_id": "intent-enter-1"
    });
    let (accepted, skipped, _) = gate_live_decisions(
        &state,
        "SOLUSDT",
        "5m",
        &[decision],
        &LiveTriggerMode::PaperMirror,
        true,
    )
    .await;
    assert!(accepted.is_empty());
    assert_eq!(
        skipped
            .first()
            .and_then(|v| v.get("reason"))
            .and_then(Value::as_str),
        Some("already_in_position")
    );
}

#[test]
fn entry_payload_normalizes_legacy_maker_input_to_fak_taker() {
    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 50.0,
        "quote_size_usdc": 1.0,
        "tif": "GTD",
        "style": "maker"
    });
    let payload = decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&test_book(5.0)),
        None,
    )
    .expect("payload");
    let size = payload
        .get("size")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let tif = payload
        .get("tif")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let style = payload
        .get("style")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        size >= 5.0,
        "normalized entry must keep min_order_size floor, got {size}"
    );
    assert_eq!(tif, "FAK");
    assert_eq!(style, "taker");
}

#[test]
fn taker_buy_quote_amount_is_quantized_to_cents_with_ceiling() {
    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 50.0,
        "quote_size_usdc": 2.501,
        "tif": "FAK",
        "style": "taker"
    });
    let payload = decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&test_book(5.0)),
        None,
    )
    .expect("payload");
    let amount = payload
        .get("buy_amount_usdc")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let price = payload
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let size = payload
        .get("size")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let cents_scaled = amount * 100.0;
    assert!(
        (cents_scaled - cents_scaled.round()).abs() < 1e-9,
        "buy amount should keep cent precision, got {amount}"
    );
    assert!(
        amount + 1e-9 >= (size * price),
        "buy amount should cover notional after rounding, amount={amount}, size={size}, price={price}"
    );
}

#[test]
fn retry_payload_exit_ladder_moves_price_and_slippage() {
    let cur = json!({
        "action": "exit",
        "reason": "signal_reverse",
        "side": "sell_yes",
        "price": 0.60,
        "size": 5.0,
        "quote_size_usdc": 3.0,
        "tif": "FAK",
        "style": "taker",
        "ttl_ms": 900,
        "max_slippage_bps": 22.0,
        "book_meta": {
            "tick_size": 0.01
        }
    });
    let nxt = build_retry_payload(&cur, "UNMATCHED", 0).expect("retry payload");
    let px = nxt.get("price").and_then(Value::as_f64).unwrap_or_default();
    let slip = nxt
        .get("max_slippage_bps")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let ttl = nxt
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    assert!(
        px <= 0.59 + 1e-9,
        "exit retry should move sell price down by >=1 tick, got {px}"
    );
    assert!(slip > 22.0, "exit retry should raise slippage, got {slip}");
    assert!(ttl <= 900, "exit retry should not increase ttl, got {ttl}");
}

#[test]
fn retry_payload_entry_ladder_keeps_taker_mode() {
    let cur = json!({
        "action": "enter",
        "reason": "fev1_signal_entry",
        "side": "buy_yes",
        "price": 0.51,
        "size": 5.0,
        "quote_size_usdc": 3.0,
        "tif": "FAK",
        "style": "taker",
        "ttl_ms": 1400,
        "max_slippage_bps": 18.0,
        "book_meta": {
            "tick_size": 0.01
        }
    });
    let nxt = build_retry_payload(&cur, "insufficient liquidity", 0).expect("retry payload");
    let tif = nxt.get("tif").and_then(Value::as_str).unwrap_or_default();
    let style = nxt.get("style").and_then(Value::as_str).unwrap_or_default();
    let px = nxt.get("price").and_then(Value::as_f64).unwrap_or_default();
    assert_eq!(tif, "FAK");
    assert_eq!(style, "taker");
    assert!(
        px >= 0.52 - 1e-9,
        "entry retry should move buy price up by >=1 tick, got {px}"
    );
}

#[test]
fn can_retry_only_on_liquidity_like_errors() {
    assert!(can_retry_on_liquidity("insufficient liquidity"));
    assert!(can_retry_on_liquidity(
        "no orders found to match with FAK order"
    ));
    assert!(!can_retry_on_liquidity(
        "unauthorized: invalid api key or signature"
    ));
    assert!(!can_retry_on_liquidity("insufficient balance"));
    assert!(!can_retry_on_liquidity(
        "bad request: min order size violation"
    ));
}

#[test]
fn retry_payload_exit_keeps_full_size() {
    let cur = json!({
        "action": "exit",
        "reason": "signal_reverse",
        "side": "sell_yes",
        "price": 0.60,
        "size": 6.1,
        "quote_size_usdc": 3.0,
        "tif": "FAK",
        "style": "taker",
        "ttl_ms": 900,
        "max_slippage_bps": 22.0,
        "book_meta": {
            "tick_size": 0.01
        }
    });
    let nxt = build_retry_payload(&cur, "UNMATCHED", 0).expect("retry payload");
    let sz = nxt.get("size").and_then(Value::as_f64).unwrap_or_default();
    assert!(
        (sz - 6.1).abs() < 1e-9,
        "exit retry must keep full size, got {sz}"
    );
}

#[test]
fn price_parity_band_blocks_entry_when_fresh_price_exceeds_paper_band() {
    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 50.0,
        "quote_size_usdc": 5.0,
        "max_slippage_bps": 20.0
    });
    let book = GatewayBookSnapshot {
        token_id: "yes".to_string(),
        min_order_size: 0.01,
        tick_size: 0.01,
        best_bid: Some(0.59),
        best_ask: Some(0.60),
        best_bid_size: Some(100.0),
        best_ask_size: Some(100.0),
        bid_depth_top3: Some(300.0),
        ask_depth_top3: Some(300.0),
        bid_levels: Some(3),
        ask_levels: Some(3),
    };
    let err = try_decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&book),
        None,
    )
    .expect_err("fresh repricing should reject parity exhaustion");
    assert!(err.contains("live_price_parity_band_exhausted"));
}

#[test]
fn price_parity_anchors_to_paper_exec_when_available() {
    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 50.0,
        "signal_price_cents": 50.0,
        "paper_entry_exec_price_cents": 59.4,
        "quote_size_usdc": 5.0,
        "max_slippage_bps": 20.0
    });
    let book = GatewayBookSnapshot {
        token_id: "yes".to_string(),
        min_order_size: 0.01,
        tick_size: 0.01,
        best_bid: Some(0.59),
        best_ask: Some(0.60),
        best_bid_size: Some(100.0),
        best_ask_size: Some(100.0),
        bid_depth_top3: Some(300.0),
        ask_depth_top3: Some(300.0),
        bid_levels: Some(3),
        ask_levels: Some(3),
    };
    let payload = try_decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&book),
        None,
    )
    .expect("paper exec anchor should keep submit within parity band");
    assert_eq!(
        payload
            .get("price_parity")
            .and_then(|v| v.get("parity_anchor_source"))
            .and_then(Value::as_str),
        Some("paper_entry_exec_price_cents")
    );
    assert_eq!(
        payload
            .get("price_parity")
            .and_then(|v| v.get("parity_anchor_price_cents"))
            .and_then(Value::as_f64)
            .map(|v| (v * 10.0).round() / 10.0),
        Some(59.4)
    );
}

#[test]
fn paper_commit_entry_uses_wider_parity_band() {
    let decision = json!({
        "action": "enter",
        "side": "UP",
        "price_cents": 54.0,
        "signal_price_cents": 54.0,
        "paper_entry_exec_price_cents": 56.0,
        "paper_record_source": "paper_commit",
        "signal_source": "paper_commit",
        "quote_size_usdc": 5.0,
        "max_slippage_bps": 20.0
    });
    let book = GatewayBookSnapshot {
        token_id: "yes".to_string(),
        min_order_size: 0.01,
        tick_size: 0.01,
        best_bid: Some(0.58),
        best_ask: Some(0.595),
        best_bid_size: Some(100.0),
        best_ask_size: Some(100.0),
        bid_depth_top3: Some(300.0),
        ask_depth_top3: Some(300.0),
        bid_levels: Some(3),
        ask_levels: Some(3),
    };
    let payload = try_decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&book),
        None,
    )
    .expect("paper commit mirror entry should tolerate a wider live repricing band");
    assert_eq!(
        payload
            .get("price_parity")
            .and_then(|v| v.get("parity_anchor_source"))
            .and_then(Value::as_str),
        Some("paper_entry_exec_price_cents")
    );
    assert_eq!(
        payload
            .get("price_parity")
            .and_then(|v| v.get("allowed_band_cents"))
            .and_then(Value::as_f64)
            .map(|v| (v * 10.0).round() / 10.0),
        Some(4.0)
    );
}

#[test]
fn paper_commit_entry_keeps_paper_limit_when_book_anchor_is_pathological() {
    let decision = json!({
        "action": "enter",
        "side": "DOWN",
        "price_cents": 73.0,
        "signal_price_cents": 73.0,
        "paper_entry_exec_price_cents": 74.2,
        "paper_record_source": "paper_commit",
        "signal_source": "paper_commit",
        "quote_size_usdc": 5.0,
        "max_slippage_bps": 24.0
    });
    let book = GatewayBookSnapshot {
        token_id: "down".to_string(),
        min_order_size: 5.0,
        tick_size: 0.01,
        best_bid: Some(0.01),
        best_ask: Some(0.99),
        best_bid_size: Some(5000.0),
        best_ask_size: Some(5000.0),
        bid_depth_top3: Some(5000.0),
        ask_depth_top3: Some(5000.0),
        bid_levels: Some(10),
        ask_levels: Some(10),
    };
    let payload = try_decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&book),
        None,
    )
    .expect("paper commit mirror entry should keep paper limit instead of anchoring to pathological book");
    assert_eq!(payload.get("price").and_then(Value::as_f64), Some(0.73));
    let notes = payload
        .get("execution_notes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(notes.iter().any(|v| {
        v.as_str()
            .map(|s| s.starts_with("skip_book_anchor_parity:"))
            .unwrap_or(false)
    }));
}

#[test]
fn exit_price_parity_anchors_to_paper_exit_exec_when_available() {
    let decision = json!({
        "action": "exit",
        "side": "DOWN",
        "price_cents": 62.0,
        "signal_price_cents": 62.0,
        "paper_exit_exec_price_cents": 58.0,
        "position_size_shares": 5.0,
        "max_slippage_bps": 25.0
    });
    let book = GatewayBookSnapshot {
        token_id: "no".to_string(),
        min_order_size: 0.01,
        tick_size: 0.01,
        best_bid: Some(0.58),
        best_ask: Some(0.59),
        best_bid_size: Some(100.0),
        best_ask_size: Some(100.0),
        bid_depth_top3: Some(300.0),
        ask_depth_top3: Some(300.0),
        bid_levels: Some(3),
        ask_levels: Some(3),
    };
    let payload = try_decision_to_live_payload(
        &decision,
        &test_target(),
        &test_exec_cfg(),
        Some(&book),
        None,
    )
    .expect("paper exit anchor should keep submit within parity band");
    assert_eq!(
        payload
            .get("price_parity")
            .and_then(|v| v.get("parity_anchor_source"))
            .and_then(Value::as_str),
        Some("paper_exit_exec_price_cents")
    );
    assert_eq!(
        payload
            .get("price_parity")
            .and_then(|v| v.get("parity_anchor_price_cents"))
            .and_then(Value::as_f64)
            .map(|v| (v * 10.0).round() / 10.0),
        Some(58.0)
    );
}

#[test]
fn execution_price_trace_includes_paper_exec_deltas() {
    let decision = json!({
        "action": "enter",
        "side": "DOWN",
        "price_cents": 56.0,
        "signal_price_cents": 56.0,
        "paper_entry_exec_price_cents": 57.45
    });
    let request = json!({
        "price": 0.58,
        "price_parity": {
            "submit_price_cents": 58.0
        }
    });
    let final_request = request.clone();
    let response = json!({"price": 0.59});
    let trace = build_execution_price_trace(&decision, &request, &final_request, Some(&response));
    assert_eq!(
        trace
            .get("paper_entry_exec_price_cents")
            .and_then(Value::as_f64),
        Some(57.45)
    );
    assert_eq!(
        trace
            .get("paper_exec_vs_submit_cents")
            .and_then(Value::as_f64)
            .map(|v| (v * 100.0).round() / 100.0),
        Some(0.55)
    );
    assert_eq!(
        trace
            .get("paper_exec_vs_accepted_cents")
            .and_then(Value::as_f64)
            .map(|v| (v * 100.0).round() / 100.0),
        Some(1.55)
    );
}

#[test]
fn decision_round_target_match_rejects_cross_round_target() {
    let start_ms = 1_700_000_000_000_i64;
    let mut target = test_target();
    target.end_date = end_date_iso_from_ms(start_ms + 300_000);
    let ok = json!({"round_id": format!("BTCUSDT_5m_{start_ms}")});
    let bad = json!({"round_id": format!("BTCUSDT_5m_{}", start_ms + 300_000)});
    assert!(decision_round_matches_target(&ok, &target));
    assert!(!decision_round_matches_target(&bad, &target));
}

#[test]
fn decision_round_target_match_rejects_entry_without_round_id() {
    let mut target = test_target();
    target.end_date = end_date_iso_from_ms(1_700_000_300_000);
    let enter_missing_round = json!({"action":"enter","side":"UP"});
    let exit_missing_round = json!({"action":"exit","side":"UP"});
    assert!(!decision_round_matches_target(
        &enter_missing_round,
        &target
    ));
    assert!(decision_round_matches_target(&exit_missing_round, &target));
}

#[test]
fn validate_entry_target_readiness_rejects_expired_entry_target() {
    let decision = json!({"action":"enter","side":"UP","round_id":"BTCUSDT_5m_1700000000000"});
    let mut target = test_target();
    target.end_date = end_date_iso_from_ms(1_700_000_000_000);
    let err = validate_entry_target_readiness(&decision, &target, 1_700_000_360_000)
        .expect_err("entry on expired target should be blocked");
    assert_eq!(err, "entry_target_expired");
}

#[test]
fn validate_entry_target_readiness_allows_exit_even_without_target_end_date() {
    let decision = json!({"action":"exit","side":"UP"});
    let mut target = test_target();
    target.end_date = None;
    assert!(validate_entry_target_readiness(&decision, &target, 1_700_000_360_000).is_ok());
}

#[test]
fn parse_round_end_ts_uses_timeframe_duration() {
    let start = 1_700_000_000_000_i64;
    assert_eq!(
        parse_round_end_ts_ms(&format!("BTCUSDT_5m_{start}")),
        Some(start + 300_000)
    );
    assert_eq!(
        parse_round_end_ts_ms(&format!("BTCUSDT_15m_{start}")),
        Some(start + 900_000)
    );
}

#[test]
fn extract_fill_meta_prefers_size_matched_over_quote_backsolve() {
    let pending = LivePendingOrder {
        symbol: "BTCUSDT".to_string(),
        market_type: "5m".to_string(),
        order_id: "oid".to_string(),
        market_id: "mkt".to_string(),
        token_id: "yes".to_string(),
        action: "enter".to_string(),
        side: "UP".to_string(),
        round_id: "r".to_string(),
        decision_key: "k".to_string(),
        intent_id: "did".to_string(),
        decision_id: "did".to_string(),
        price_cents: 99.0,
        quote_size_usdc: 4.95,
        order_size_shares: 5.0,
        tif: "FAK".to_string(),
        style: "taker".to_string(),
        submit_reason: "test".to_string(),
        submitted_ts_ms: 0,
        ack_ts_ms: 0,
        decision_ts_ms: 0,
        trigger_ts_ms: 0,
        cancel_after_ms: 1000,
        cancel_due_at_ms: 0,
        terminal_due_at_ms: 4_000,
        retry_count: 0,
        size_locked: false,
        accepted_trade_ids: Vec::new(),
    };
    let fill = json!({
        "event": "order_terminal",
        "state": "filled",
        "fill_price_cents": 81.0,
        "fill_quote_usdc": 4.95,
        "size_matched": 6.111111
    });
    let meta = extract_pending_fill_meta(Some(&fill), &pending);
    let shares = meta.fill_size_shares.unwrap_or(0.0);
    assert!(
        (shares - 6.111111).abs() < 1e-6,
        "should keep exact matched shares, got {shares}"
    );
}

#[test]
fn extract_fill_meta_caps_locked_fill_size_to_order_size() {
    let pending = LivePendingOrder {
        symbol: "BTCUSDT".to_string(),
        market_type: "5m".to_string(),
        order_id: "oid".to_string(),
        market_id: "mkt".to_string(),
        token_id: "yes".to_string(),
        action: "enter".to_string(),
        side: "UP".to_string(),
        round_id: "r".to_string(),
        decision_key: "k".to_string(),
        intent_id: "did".to_string(),
        decision_id: "did".to_string(),
        price_cents: 99.0,
        quote_size_usdc: 4.95,
        order_size_shares: 5.0,
        tif: "FAK".to_string(),
        style: "taker".to_string(),
        submit_reason: "test".to_string(),
        submitted_ts_ms: 0,
        ack_ts_ms: 0,
        decision_ts_ms: 0,
        trigger_ts_ms: 0,
        cancel_after_ms: 1000,
        cancel_due_at_ms: 0,
        terminal_due_at_ms: 4_000,
        retry_count: 0,
        size_locked: true,
        accepted_trade_ids: Vec::new(),
    };
    let fill = json!({
        "event": "order_terminal",
        "state": "filled",
        "fill_price_cents": 99.0,
        "fill_quote_usdc": 10.00101861,
        "fill_size_shares": 10.102039,
        "original_size_shares": 5.0
    });
    let meta = extract_pending_fill_meta(Some(&fill), &pending);
    assert_eq!(meta.fill_size_shares, Some(5.0));
    assert_eq!(meta.fill_quote_usdc, Some(4.95));
    assert_eq!(meta.reported_fill_size_shares, Some(10.102039));
    assert_eq!(meta.reported_fill_quote_usdc, Some(10.00101861));
    assert!(meta.size_guard_triggered);
}

#[test]
fn extract_fill_meta_locked_exit_reprices_from_quote_when_raw_price_is_inverted() {
    let pending = LivePendingOrder {
        symbol: "BTCUSDT".to_string(),
        market_type: "5m".to_string(),
        order_id: "oid".to_string(),
        market_id: "mkt".to_string(),
        token_id: "down".to_string(),
        action: "exit".to_string(),
        side: "DOWN".to_string(),
        round_id: "r".to_string(),
        decision_key: "k".to_string(),
        intent_id: "did".to_string(),
        decision_id: "did".to_string(),
        price_cents: 1.0,
        quote_size_usdc: 4.95,
        order_size_shares: 5.1,
        tif: "FAK".to_string(),
        style: "taker".to_string(),
        submit_reason: "test".to_string(),
        submitted_ts_ms: 0,
        ack_ts_ms: 0,
        decision_ts_ms: 0,
        trigger_ts_ms: 0,
        cancel_after_ms: 1000,
        cancel_due_at_ms: 0,
        terminal_due_at_ms: 4_000,
        retry_count: 0,
        size_locked: true,
        accepted_trade_ids: Vec::new(),
    };
    let fill = json!({
        "event": "order_terminal",
        "state": "filled",
        "fill_price_cents": 1.0,
        "fill_quote_usdc": 4.95
    });
    let meta = extract_pending_fill_meta(Some(&fill), &pending);
    assert_eq!(meta.fill_size_shares, Some(5.1));
    assert!(
        (meta.fill_price_cents.unwrap_or_default() - 97.0588235294).abs() < 1e-6,
        "expected repriced fill cents near 97.0588, got {:?}",
        meta.fill_price_cents
    );
    assert_eq!(meta.fill_quote_usdc, Some(4.95));
    assert!(!meta.size_guard_triggered);
}

#[test]
fn locked_market_missing_submit_error_detects_orderbook_missing() {
    assert!(is_locked_market_missing_submit_error(
        "post_order failed: Status: error(400 Bad Request) making POST call to /order with {\"error\":\"the orderbook 123 does not exist\"}"
    ));
    assert!(!is_locked_market_missing_submit_error(
        "insufficient balance"
    ));
}

#[test]
fn market_buy_amount_is_ceiled_to_two_decimals() {
    let v = ceil_market_buy_amount_usdc(1.001);
    assert!(
        (v - 1.01).abs() < 1e-9,
        "market buy amount should ceil to cent precision, got {v}"
    );
}

#[test]
fn usdc_micro_roundtrip_is_stable() {
    let v = 12.345_678_4;
    let micros = usdc_to_micros(v);
    let roundtrip = micros_to_usdc(micros);
    assert!(
        (roundtrip - 12.345_678).abs() < 1e-9,
        "micro-usdc roundtrip should be stable, got {roundtrip}"
    );
}

#[test]
fn usdc_micro_roundtrip_preserves_negative_values() {
    let v = -0.450_000_4;
    let micros = usdc_to_micros(v);
    let roundtrip = micros_to_usdc(micros);
    assert!(
        (roundtrip + 0.45).abs() < 1e-9,
        "negative micro-usdc roundtrip should be stable, got {roundtrip}"
    );
}

#[test]
fn canonical_post_order_amounts_map_buy_and_sell_sides_correctly() {
    let (buy_shares, buy_quote) =
        canonical_post_order_amounts(PmSide::Buy, 5.0, 3.6, 3.599_999, 5.142_856);
    assert!((buy_shares - 5.142_856).abs() < 1e-6);
    assert!((buy_quote - 3.599_999).abs() < 1e-6);

    let (sell_shares, sell_quote) =
        canonical_post_order_amounts(PmSide::Sell, 5.0, 3.15, 5.0, 3.15);
    assert!((sell_shares - 5.0).abs() < 1e-9);
    assert!((sell_quote - 3.15).abs() < 1e-9);
}

#[test]
fn fill_event_prefers_trade_rows_over_raw_order_price() {
    let order = polymarket_client_sdk::clob::types::response::OpenOrderResponse::builder()
        .id("oid")
        .status(PmOrderStatusType::Matched)
        .owner(uuid::Uuid::nil())
        .maker_address(PmAddress::ZERO)
        .market(polymarket_client_sdk::types::B256::ZERO)
        .asset_id(PmU256::ZERO)
        .side(PmSide::Sell)
        .original_size(PmDecimal::from_str("5").expect("decimal"))
        .size_matched(PmDecimal::from_str("5").expect("decimal"))
        .price(PmDecimal::from_str("0.01").expect("decimal"))
        .associate_trades(vec!["trade-1".to_string()])
        .outcome("YES")
        .created_at(Utc::now())
        .expiration(Utc::now())
        .order_type(PmOrderType::FAK)
        .build();
    let trade = polymarket_client_sdk::clob::types::response::TradeResponse::builder()
        .id("trade-1")
        .taker_order_id("oid")
        .market(polymarket_client_sdk::types::B256::ZERO)
        .asset_id(PmU256::ZERO)
        .side(PmSide::Sell)
        .size(PmDecimal::from_str("5").expect("decimal"))
        .fee_rate_bps(PmDecimal::ZERO)
        .price(PmDecimal::from_str("0.63").expect("decimal"))
        .status(polymarket_client_sdk::clob::types::TradeStatusType::Matched)
        .match_time(Utc::now())
        .last_update(Utc::now())
        .outcome("YES")
        .bucket_index(0)
        .owner(uuid::Uuid::nil())
        .maker_address(PmAddress::ZERO)
        .maker_orders(Vec::new())
        .transaction_hash(polymarket_client_sdk::types::B256::ZERO)
        .trader_side(polymarket_client_sdk::clob::types::TraderSide::Taker)
        .build();

    let fill = fill_event_from_open_order(&order, &[trade]);
    assert_eq!(fill.get("fill_source").and_then(Value::as_str), Some("trade_ids"));
    assert_eq!(
        fill.get("fill_price_cents").and_then(Value::as_f64),
        Some(63.0)
    );
    assert_eq!(
        fill.get("fill_quote_usdc").and_then(Value::as_f64),
        Some(3.15)
    );
}

#[tokio::test]
async fn apply_pending_confirmation_exit_realizes_pnl() {
    let state = test_api_state();
    state
        .put_live_position_state(
            "SOLUSDT",
            "5m",
            LivePositionState {
                symbol: "SOLUSDT".to_string(),
                market_type: "5m".to_string(),
                state: "in_position".to_string(),
                side: Some("UP".to_string()),
                entry_round_id: Some("SOLUSDT_5m_1".to_string()),
                entry_market_id: Some("mkt".to_string()),
                entry_token_id: Some("yes".to_string()),
                entry_ts_ms: Some(1_000),
                entry_price_cents: Some(72.0),
                entry_quote_usdc: Some(3.6),
                net_quote_usdc: 3.6,
                vwap_entry_cents: Some(72.0),
                last_action: Some("enter_UP".to_string()),
                last_reason: Some("rust_order_terminal_filled".to_string()),
                total_entries: 1,
                total_exits: 0,
                total_adds: 0,
                open_add_layers: 0,
                total_reduces: 0,
                realized_pnl_usdc: 0.0,
                last_fill_pnl_usdc: 0.0,
                position_cost_usdc: 0.0,
                position_size_shares: 5.0,
                updated_ts_ms: 1_000,
            },
        )
        .await;
    let pending = LivePendingOrder {
        symbol: "SOLUSDT".to_string(),
        market_type: "5m".to_string(),
        order_id: "oid".to_string(),
        market_id: "mkt".to_string(),
        token_id: "yes".to_string(),
        action: "exit".to_string(),
        side: "UP".to_string(),
        round_id: "SOLUSDT_5m_1".to_string(),
        decision_key: "k".to_string(),
        intent_id: "intent-1".to_string(),
        decision_id: "intent-1".to_string(),
        price_cents: 1.0,
        quote_size_usdc: 3.6,
        order_size_shares: 5.0,
        tif: "FAK".to_string(),
        style: "taker".to_string(),
        submit_reason: "stop_loss_wide_grace".to_string(),
        submitted_ts_ms: 1_200,
        ack_ts_ms: 1_250,
        decision_ts_ms: 1_000,
        trigger_ts_ms: 1_050,
        cancel_after_ms: 1_000,
        cancel_due_at_ms: 0,
        terminal_due_at_ms: 4_000,
        retry_count: 0,
        size_locked: true,
        accepted_trade_ids: vec!["trade-1".to_string()],
    };
    let fill = json!({
        "fill_price_cents": 63.0,
        "fill_quote_usdc": 3.15,
        "fill_size_shares": 5.0
    });

    apply_pending_confirmation(&state, &pending, "rust_order_terminal_filled", Some(&fill)).await;

    let next = state.get_live_position_state("SOLUSDT", "5m").await;
    assert_eq!(next.state, "flat");
    assert!(next.position_size_shares.abs() < 1e-9);
    assert!(
        (next.realized_pnl_usdc + 0.45).abs() < 1e-9,
        "expected realized pnl of -0.45 usdc, got {}",
        next.realized_pnl_usdc
    );
    assert!(
        (next.last_fill_pnl_usdc + 0.45).abs() < 1e-9,
        "expected last fill pnl of -0.45 usdc, got {}",
        next.last_fill_pnl_usdc
    );
}

#[tokio::test]
async fn gate_live_decisions_blocks_entry_after_liquidity_reject_limit() {
    let state = test_api_state();
    let now_ms = chrono::Utc::now().timestamp_millis();
    state
        .set_live_execution_aggr_state(
            "SOLUSDT",
            "5m",
            crate::api::LiveExecutionAggState::for_test_liquidity_rejects(
                "5m",
                "SOLUSDT_5m_1",
                "DOWN",
                4,
                now_ms,
            ),
        )
        .await;
    let decision = json!({
        "action": "enter",
        "side": "DOWN",
        "round_id": "SOLUSDT_5m_1",
        "ts_ms": now_ms,
        "decision_id": "intent-1"
    });
    let (accepted, skipped, _) = gate_live_decisions(
        &state,
        "SOLUSDT",
        "5m",
        &[decision],
        &LiveTriggerMode::SignalPool,
        true,
    )
    .await;
    assert!(accepted.is_empty());
    assert_eq!(
        skipped
            .first()
            .and_then(|v| v.get("reason"))
            .and_then(Value::as_str),
        Some("entry_liquidity_reject_limit_reached")
    );
}

#[tokio::test]
async fn locked_exit_target_expired_is_rejected_before_submit() {
    let target = LiveMarketTarget {
        market_id: "1529923".to_string(),
        symbol: "SOLUSDT".to_string(),
        timeframe: "5m".to_string(),
        token_id_yes: "yes".to_string(),
        token_id_no: "no".to_string(),
        end_date: Some("2026-03-09T03:50:00Z".to_string()),
    };
    let position = LivePositionState {
        symbol: "SOLUSDT".to_string(),
        market_type: "5m".to_string(),
        state: "in_position".to_string(),
        side: Some("DOWN".to_string()),
        entry_round_id: Some("SOLUSDT_5m_1773027900000".to_string()),
        entry_market_id: Some("1529923".to_string()),
        entry_token_id: Some("24791353838135686396150788674032116106865477312181400776264735735012421228433".to_string()),
        entry_ts_ms: Some(1773028009000),
        entry_price_cents: Some(88.0),
        entry_quote_usdc: Some(4.4),
        net_quote_usdc: 4.4,
        vwap_entry_cents: Some(88.0),
        last_action: Some("enter".to_string()),
        last_reason: Some("rust_order_terminal_filled".to_string()),
        total_entries: 1,
        total_exits: 0,
        total_adds: 0,
        open_add_layers: 0,
        total_reduces: 0,
        realized_pnl_usdc: 0.0,
        last_fill_pnl_usdc: 0.0,
        position_cost_usdc: 4.4,
        position_size_shares: 5.0,
        updated_ts_ms: 1773040388000,
    };

    let err = validate_locked_exit_target_tradable(&target, &position)
        .await
        .expect_err("expired locked market should be rejected before submit");
    assert_eq!(err.reason, "locked_exit_market_expired");
}

#[test]
fn entry_decision_market_override_replaces_scope_target() {
    let target = LiveMarketTarget {
        market_id: "old-market".to_string(),
        symbol: "SOLUSDT".to_string(),
        timeframe: "5m".to_string(),
        token_id_yes: "old-yes".to_string(),
        token_id_no: "old-no".to_string(),
        end_date: Some("2026-03-15T02:55:00Z".to_string()),
    };
    let decision = json!({
        "action": "enter",
        "side": "DOWN",
        "market_id": "1582888",
        "token_id_yes": "yes-1582888",
        "token_id_no": "no-1582888",
        "target_end_date": "2026-03-15T03:00:00Z"
    });
    let effective = build_effective_target_for_decision(
        &target,
        &decision,
        &LivePositionState::flat("SOLUSDT", "5m", 0),
    );
    assert_eq!(effective.market_id, "1582888");
    assert_eq!(effective.token_id_yes, "yes-1582888");
    assert_eq!(effective.token_id_no, "no-1582888");
    assert_eq!(effective.end_date.as_deref(), Some("2026-03-15T03:00:00Z"));
}

#[test]
fn resolved_outcome_marks_local_token_as_winning() {
    let detail = GammaMarketDetail {
        market_id: "1529923".to_string(),
        condition_id: Some(
            "0x1cdce4553d69a5fda87c776131673631e3e3c976b9d16ac2b4d4051e227b749a".to_string(),
        ),
        active: true,
        closed: true,
        enable_order_book: true,
        end_date: Some("2026-03-09T03:50:00Z".to_string()),
        token_ids: vec!["up".to_string(), "down".to_string()],
        outcomes: vec!["Up".to_string(), "Down".to_string()],
        outcome_prices: vec![0.0, 1.0],
        uma_resolution_status: Some("resolved".to_string()),
        slug: Some("sol-updown-5m-1773027900".to_string()),
        question: Some("Solana Up or Down".to_string()),
    };
    let position = LivePositionState {
        symbol: "SOLUSDT".to_string(),
        market_type: "5m".to_string(),
        state: "in_position".to_string(),
        side: Some("DOWN".to_string()),
        entry_round_id: Some("SOLUSDT_5m_1773027900000".to_string()),
        entry_market_id: Some("1529923".to_string()),
        entry_token_id: Some("down".to_string()),
        entry_ts_ms: Some(1773028009000),
        entry_price_cents: Some(88.0),
        entry_quote_usdc: Some(4.4),
        net_quote_usdc: 4.4,
        vwap_entry_cents: Some(88.0),
        last_action: Some("enter".to_string()),
        last_reason: Some("rust_order_terminal_filled".to_string()),
        total_entries: 1,
        total_exits: 0,
        total_adds: 0,
        open_add_layers: 0,
        total_reduces: 0,
        realized_pnl_usdc: 0.0,
        last_fill_pnl_usdc: 0.0,
        position_cost_usdc: 4.4,
        position_size_shares: 5.0,
        updated_ts_ms: 1773040388000,
    };

    let resolution = analyze_resolved_position_outcome(Some(&detail), &position);
    assert!(resolution.market_closed);
    assert!(resolution.market_resolved);
    assert_eq!(resolution.local_token_result, "winning");
    assert_eq!(resolution.local_outcome_label.as_deref(), Some("Down"));
    assert_eq!(resolution.winning_outcome_label.as_deref(), Some("Down"));
    assert_eq!(resolution.winning_token_id.as_deref(), Some("down"));
    assert_eq!(resolution.local_expected_claim_value_usdc, Some(5.0));
}

#[test]
fn resolved_position_local_clear_requires_pause_and_resolution() {
    assert!(can_clear_resolved_position_local_state(
        true,
        0,
        LiveRuntimeControlMode::ForcePause,
        true,
        false
    ));
    assert!(!can_clear_resolved_position_local_state(
        true,
        1,
        LiveRuntimeControlMode::ForcePause,
        true,
        false
    ));
    assert!(!can_clear_resolved_position_local_state(
        true,
        0,
        LiveRuntimeControlMode::Normal,
        true,
        false
    ));
    assert!(!can_clear_resolved_position_local_state(
        false,
        0,
        LiveRuntimeControlMode::ForcePause,
        true,
        false
    ));
    assert!(!can_clear_resolved_position_local_state(
        true,
        0,
        LiveRuntimeControlMode::ForcePause,
        false,
        false
    ));
    assert!(can_clear_resolved_position_local_state(
        true,
        0,
        LiveRuntimeControlMode::ForcePause,
        false,
        true
    ));
}
