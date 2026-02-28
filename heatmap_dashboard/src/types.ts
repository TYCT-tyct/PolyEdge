export type MarketType = "5m" | "15m";
export type MarketSymbol = "BTCUSDT" | "ETHUSDT" | "SOLUSDT" | "XRPUSDT";
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

export interface CollectorTimeframeStatus {
  status: "ok" | "lagging" | "stalled" | "missing";
  timestamp_ms: number | null;
  age_ms: number | null;
  remaining_ms: number | null;
  round_id: string;
}

export interface CollectorStatusResponse {
  ok: boolean;
  ts_ms: number;
  timeframes: {
    "5m": CollectorTimeframeStatus;
    "15m": CollectorTimeframeStatus;
  };
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

export interface StrategyPaperTrade {
  id: number;
  side: "UP" | "DOWN";
  entry_round_id: string;
  entry_ts_ms: number;
  exit_ts_ms: number;
  entry_price_cents: number;
  entry_price_raw_cents: number;
  entry_fee_cents: number;
  entry_slippage_cents: number;
  exit_price_cents: number;
  exit_price_raw_cents: number;
  exit_fee_cents: number;
  exit_slippage_cents: number;
  exit_emergency_penalty_cents?: number;
  pnl_gross_cents: number;
  total_cost_cents: number;
  pnl_net_cents: number;
  pnl_cents: number;
  peak_pnl_cents: number;
  duration_s: number;
  entry_score: number;
  exit_score: number;
  entry_reason: string;
  exit_reason: string;
}

export interface StrategyLiveDecision {
  decision_id?: string;
  ts_ms?: number;
  round_id?: string;
  action?: "enter" | "add" | "reduce" | "exit" | string;
  side?: "UP" | "DOWN" | string;
  price_cents?: number;
  quote_size_usdc?: number;
  reason?: string;
}

export interface StrategyLiveExecutionOrder {
  ok: boolean;
  accepted?: boolean;
  endpoint?: string;
  decision_key?: string;
  request?: Record<string, unknown>;
  response?: Record<string, unknown>;
  error?: string;
  reason?: string;
  decision?: StrategyLiveDecision;
}

export interface StrategyLivePositionState {
  market_type: string;
  state: "flat" | "in_position" | string;
  side?: "UP" | "DOWN" | string | null;
  entry_round_id?: string | null;
  entry_ts_ms?: number | null;
  entry_price_cents?: number | null;
  entry_quote_usdc?: number | null;
  net_quote_usdc?: number;
  vwap_entry_cents?: number | null;
  last_action?: string | null;
  last_reason?: string | null;
  total_entries?: number;
  total_adds?: number;
  total_reduces?: number;
  total_exits?: number;
  realized_pnl_usdc?: number;
  last_fill_pnl_usdc?: number;
  updated_ts_ms?: number;
}

export interface StrategyPaperResponse {
  source?: "replay" | "live" | "auto" | string;
  source_fallback_error?: string | null;
  market_type: string;
  autotune_context?: string;
  autotune_active_key?: string;
  autotune_live_key?: string;
  autotune_live_found?: boolean;
  runtime_defaults?: {
    lookback_minutes?: number;
    max_points?: number;
    max_trades?: number;
  };
  runtime_control?: {
    mode?: "normal" | "graceful_stop" | "force_pause" | string;
    requested_at_ms?: number;
    updated_at_ms?: number;
    completed_at_ms?: number | null;
    note?: string | null;
    effective_live_execute?: boolean;
    effective_drain_only?: boolean;
    position_side_before_cycle?: "UP" | "DOWN" | null | string;
    pending_orders_before_cycle?: number;
  };
  lookback_minutes: number;
  samples: number;
  config_source?: string;
  baseline_profile?: string;
  autotune?: unknown;
  config: {
    entry_threshold_base: number;
    entry_threshold_cap: number;
    spread_limit_prob: number;
    stop_loss_cents: number;
    trail_drawdown_cents: number;
    reverse_signal_threshold: number;
    reverse_signal_ticks: number;
  };
  current: {
    timestamp_ms: number;
    round_id: string;
    remaining_s: number;
    score: number;
    entry_threshold: number;
    confirmed: boolean;
    suggested_side: "UP" | "DOWN" | "WAIT";
    suggested_action: "ENTER_UP" | "ENTER_DOWN" | "WAIT" | "RISK_OFF" | "HOLD";
    confidence: number;
    p_up_pct: number;
    delta_pct: number;
    spread_up_cents: number;
    spread_down_cents: number;
  } | null;
  summary: {
    trade_count: number;
    win_rate_pct: number;
    avg_pnl_cents: number;
    total_pnl_cents: number;
    net_pnl_cents: number;
    gross_pnl_cents: number;
    total_cost_cents: number;
    total_entry_fee_cents: number;
    total_exit_fee_cents: number;
    total_slippage_cents: number;
    net_margin_pct: number;
    max_drawdown_cents: number;
  };
  live_execution?: {
    summary?: Record<string, unknown>;
    decisions?: StrategyLiveDecision[];
    paper_records?: Record<string, unknown>[];
    live_records?: Record<string, unknown>[];
    parity_check?: {
      status?: "ok" | "dry_run" | "blocked_no_market_target" | "blocked_by_gate_or_state" | "gateway_rejected_all" | string;
      level?: "ok" | "warn" | "critical" | string;
      paper?: {
        decision_count?: number;
        decisions?: number;
        entry_count?: number;
        entries?: number;
        add_count?: number;
        reduce_count?: number;
        exit_count?: number;
        exits?: number;
      };
      live?: {
        submitted_count?: number;
        submitted_entry_count?: number;
        submitted_add_count?: number;
        submitted_reduce_count?: number;
        submitted_exit_count?: number;
        simulated_submitted_count?: number;
        simulated_submitted_entry_count?: number;
        simulated_submitted_add_count?: number;
        simulated_submitted_reduce_count?: number;
        simulated_submitted_exit_count?: number;
        accepted_count?: number;
        rejected_count?: number;
        skipped_count?: number;
        entries?: { submitted?: number; accepted?: number; rejected?: number; skipped?: number };
        exits?: { submitted?: number; accepted?: number; rejected?: number; skipped?: number };
        accepted?: number;
        rejected?: number;
        skipped?: number;
        no_live_market_target?: boolean;
      };
    };
    gated?: {
      selected_count?: number;
      submitted_count?: number;
      skipped_count?: number;
      submitted_decisions?: StrategyLiveDecision[];
      skipped_decisions?: Array<{
        reason?: string;
        decision?: StrategyLiveDecision;
      }>;
    };
    gateway?: {
      primary_url?: string;
      backup_url?: string | null;
      timeout_ms?: number;
      entry_slippage_bps?: number;
      exit_slippage_bps?: number;
      min_quote_usdc?: number;
    };
    capital?: {
      auto_enabled?: boolean;
      dynamic_quote_usdc?: number;
      base_quote_usdc?: number;
      target_utilization?: number;
      reserve_usdc?: number;
      small_threshold_usdc?: number;
      max_add_layers?: number;
      available_after_capital_guard_usdc?: number;
      capital_skipped_count?: number;
      open_pending_orders?: number;
      state?: {
        market_type?: string;
        equity_base_usdc?: number;
        equity_estimate_usdc?: number;
        max_equity_usdc?: number;
        realized_pnl_usdc?: number;
        reserved_pending_usdc?: number;
        available_to_trade_usdc?: number;
        utilization_ratio?: number;
        tune_factor?: number;
        risk_state?: string;
        consecutive_wins?: number;
        consecutive_losses?: number;
        bankroll_mode?: string;
        bankroll_defense_armed?: boolean;
        day_cn_bucket?: number;
        day_start_equity_usdc?: number;
        daily_drawdown_ratio?: number;
        last_realized_pnl_usdc?: number;
        last_quote_usdc?: number;
        updated_ts_ms?: number;
      };
    };
    execution?: {
      mode?: "dry_run" | "real_gateway" | string;
      error?: string;
      target?: Record<string, unknown> | null;
      orders?: StrategyLiveExecutionOrder[];
    };
    state_machine?: StrategyLivePositionState;
    events?: Record<string, unknown>[];
  };
  live_execute?: boolean;
  strategy_alias?: string;
  trades: StrategyPaperTrade[];
}
