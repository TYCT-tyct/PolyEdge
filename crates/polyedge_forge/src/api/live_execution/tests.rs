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

    #[test]
    fn maker_buy_still_respects_min_order_size_floor() {
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
        assert!(
            size >= 5.0,
            "maker buy must keep min_order_size floor, got {size}"
        );
        assert!(
            payload.get("buy_amount_usdc").is_none(),
            "maker path should stay in share-size mode"
        );
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
        assert!(can_retry_on_liquidity("no orders found to match with FAK order"));
        assert!(!can_retry_on_liquidity(
            "unauthorized: invalid api key or signature"
        ));
        assert!(!can_retry_on_liquidity("insufficient balance"));
        assert!(!can_retry_on_liquidity("bad request: min order size violation"));
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
        assert!(!decision_round_matches_target(&enter_missing_round, &target));
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
            price_cents: 99.0,
            quote_size_usdc: 4.95,
            order_size_shares: 5.0,
            tif: "FAK".to_string(),
            style: "taker".to_string(),
            submit_reason: "test".to_string(),
            submitted_ts_ms: 0,
            cancel_after_ms: 1000,
            retry_count: 0,
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
