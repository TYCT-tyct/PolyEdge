use crate::models::MarketMeta;

pub fn market_pair_key(market: &MarketMeta) -> String {
    format!("{}:{}", market.symbol, market.timeframe)
}

pub fn market_in_sampling_window(
    market: &MarketMeta,
    now_ms: i64,
    prestart_allow_ms: i64,
    sample_end_grace_ms: i64,
) -> bool {
    now_ms.saturating_add(prestart_allow_ms) >= market.start_ts_ms
        && now_ms <= market.end_ts_ms.saturating_add(sample_end_grace_ms)
}

fn market_selection_rank(
    market: &MarketMeta,
    now_ms: i64,
    sample_end_grace_ms: i64,
) -> (u8, i64, i64) {
    if now_ms >= market.start_ts_ms
        && now_ms <= market.end_ts_ms.saturating_add(sample_end_grace_ms)
    {
        return (
            0,
            market.end_ts_ms.saturating_sub(now_ms).abs(),
            market.start_ts_ms.abs(),
        );
    }
    if now_ms < market.start_ts_ms {
        return (
            1,
            market.start_ts_ms.saturating_sub(now_ms),
            market.start_ts_ms.abs(),
        );
    }
    (
        2,
        now_ms.saturating_sub(market.end_ts_ms),
        market.start_ts_ms.saturating_abs(),
    )
}

pub fn trim_candidate_pool(
    markets: &mut Vec<MarketMeta>,
    now_ms: i64,
    prestart_allow_ms: i64,
    sample_end_grace_ms: i64,
    max_candidates: usize,
) {
    markets.sort_by_key(|market| {
        (
            !market_in_sampling_window(market, now_ms, prestart_allow_ms, sample_end_grace_ms),
            market_selection_rank(market, now_ms, sample_end_grace_ms),
        )
    });
    markets.dedup_by(|a, b| a.market_id == b.market_id);
    markets.truncate(max_candidates.max(1));
}

#[cfg(test)]
mod tests {
    use super::{market_in_sampling_window, trim_candidate_pool};
    use crate::models::MarketMeta;

    #[test]
    fn sampling_window_uses_tight_end_grace_for_fast_switch() {
        let market = MarketMeta {
            market_id: "m".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            token_id_yes: None,
            token_id_no: None,
            target_price: None,
            start_ts_ms: 1_000,
            end_ts_ms: 2_000,
        };
        assert!(market_in_sampling_window(&market, 1_999, 30_000, 300));
        assert!(market_in_sampling_window(&market, 2_250, 30_000, 300));
        assert!(!market_in_sampling_window(&market, 2_301, 30_000, 300));
    }

    #[test]
    fn trim_candidate_pool_keeps_current_next_next2() {
        let now_ms = 1_000_000;
        let mk = |id: &str, start: i64, end: i64| MarketMeta {
            market_id: id.to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            title: "BTC".to_string(),
            token_id_yes: None,
            token_id_no: None,
            target_price: None,
            start_ts_ms: start,
            end_ts_ms: end,
        };
        let mut markets = vec![
            mk("past", now_ms - 600_000, now_ms - 300_000),
            mk("current", now_ms - 60_000, now_ms + 240_000),
            mk("next", now_ms + 240_000, now_ms + 540_000),
            mk("next2", now_ms + 540_000, now_ms + 840_000),
            mk("next3", now_ms + 840_000, now_ms + 1_140_000),
        ];
        trim_candidate_pool(&mut markets, now_ms, 30_000, 300, 3);
        let ids = markets.into_iter().map(|m| m.market_id).collect::<Vec<_>>();
        assert_eq!(ids, vec!["current", "next", "next2"]);
    }
}
