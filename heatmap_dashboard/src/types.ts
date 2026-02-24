export type MarketType = "5m" | "15m";
export type WindowType = "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "all";

export interface LiveSnapshot {
  timestamp_ms: number | null;
  round_id: string;
  market_type: MarketType;
  btc_price: number | null;
  target_price: number | null;
  delta_pct: number | null;
  delta_pct_smooth?: number | null;
  mid_yes?: number | null;
  mid_no?: number | null;
  mid_yes_smooth?: number | null;
  mid_no_smooth?: number | null;
  best_bid_up: number | null;
  best_ask_up: number | null;
  best_bid_down: number | null;
  best_ask_down: number | null;
  time_remaining_s: number | null;
  slug: string;
  start_time_ms: number;
  end_time_ms: number;
  velocity_bps_per_sec?: number | null;
  acceleration?: number | null;
}

export interface WsLivePayload {
  "5m": LiveSnapshot | null;
  "15m": LiveSnapshot | null;
}

export interface ChartPoint {
  timestamp_ms: number;
  delta_pct: number | null;
  delta_pct_smooth?: number | null;
  mid_yes?: number | null;
  mid_no?: number | null;
  mid_yes_smooth?: number | null;
  mid_no_smooth?: number | null;
  best_bid_up: number | null;
  best_ask_up: number | null;
  best_bid_down: number | null;
  best_ask_down: number | null;
  round_id: string;
  time_remaining_s: number | null;
  btc_price: number | null;
  target_price: number | null;
}

export interface ChartRound {
  round_id: string;
  start_ts_ms?: number;
  end_ts_ms?: number;
  start_time_ms?: number;
  end_time_ms?: number;
  target_price?: number | null;
  outcome?: number | null;
}

export interface ChartResponse {
  points: ChartPoint[];
  rounds: ChartRound[];
  total_samples: number;
  downsampled: boolean;
  step: number;
}

export interface StatsResponse {
  total_rounds: number;
  total_samples: number;
  up_count: number;
  down_count: number;
  first_sample_ms: number | null;
  last_sample_ms: number | null;
  uptime_hours: number;
  market_accuracy: number | null;
  market_accuracy_n: number;
}

export interface RoundHistoryRow {
  round_id: string;
  market_id: string;
  market_type: string;
  start_time_ms: number;
  end_time_ms: number;
  target_price: number;
  final_btc_price: number;
  outcome: number;
  delta_pct: number | null;
  mkt_price_cents: number | null;
}

export interface RoundsResponse {
  market_type: MarketType;
  count: number;
  rounds: RoundHistoryRow[];
}

export interface AvailableRoundRow {
  round_id: string;
  market_id: string;
  start_time_ms: number;
  end_time_ms: number;
  date: string;
}

export interface AvailableRoundsResponse {
  market_type: MarketType;
  days: Array<{ date: string; round_count: number }>;
  rounds: AvailableRoundRow[];
}

export interface RoundChartResponse {
  points: ChartPoint[];
  round: {
    round_id: string;
    market_type: MarketType;
    start_time_ms: number;
    end_time_ms: number;
    target_price: number | null;
    outcome: number | null;
    slug: string;
  } | null;
  total_samples: number;
  step: number;
}

export interface HeatmapCell {
  delta_bin_pct: number;
  time_left_s_bin: number;
  avg_up_price_cents: number;
  sample_count: number;
  opacity: number;
}

export interface HeatmapResponse {
  market_type: MarketType;
  lookback_hours: number;
  max_sample_count: number;
  cells: HeatmapCell[];
}

export interface AccuracyPoint {
  timestamp_ms: number;
  round_id: string;
  accuracy_pct: number;
  sample_count: number;
}

export interface AccuracySeriesResponse {
  market_type: MarketType;
  window: number | null;
  lookback_hours: number;
  bucket_minutes?: number;
  processed_rounds?: number;
  series: AccuracyPoint[];
  latest_accuracy_pct: number | null;
}
