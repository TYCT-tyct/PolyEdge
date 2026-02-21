use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use chrono::{TimeZone, Utc};
use core_types::{
    BookTop, Direction, ExecutionStyle, FillEvent, OrderAck, OrderSide, PaperAction,
    PaperDailySummary, PaperFill, PaperIntent, PaperLiveReport, PaperTradeRecord, Stage,
};
use tokio::sync::RwLock;

use crate::paper_sqlite::PaperSqliteWriter;
use crate::report_io::{append_jsonl, dataset_path};
use crate::seat_runtime::SeatRuntimeHandle;
use crate::seat_types::SeatDecisionRecord;
use crate::stats_utils::percentile;

const HISTORY_CAP: usize = 20_000;
const DEFAULT_BINARY_SETTLE_HIGH: f64 = 0.995;
const DEFAULT_BINARY_SETTLE_LOW: f64 = 0.005;

static GLOBAL_PAPER_RUNTIME: OnceLock<Arc<PaperRuntimeHandle>> = OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) struct PaperIntentCtx {
    pub(crate) market_id: String,
    pub(crate) symbol: String,
    pub(crate) timeframe: String,
    pub(crate) stage: Stage,
    pub(crate) direction: Direction,
    pub(crate) velocity_bps_per_sec: f64,
    pub(crate) edge_bps: f64,
    pub(crate) prob_fast: f64,
    pub(crate) prob_settle: f64,
    pub(crate) confidence: f64,
    pub(crate) action: PaperAction,
    pub(crate) intent: ExecutionStyle,
    pub(crate) requested_size_usdc: f64,
    pub(crate) requested_size_contracts: f64,
    pub(crate) entry_price: f64,
}

#[derive(Debug, Clone)]
struct PaperLot {
    market_id: String,
    symbol: String,
    timeframe: String,
    side: OrderSide,
    stage: Stage,
    direction: Direction,
    velocity_bps_per_sec: f64,
    edge_bps: f64,
    prob_fast: f64,
    prob_settle: f64,
    confidence: f64,
    intent: ExecutionStyle,
    requested_size_usdc: f64,
    entry_price: f64,
    remaining_size: f64,
    entry_fee_per_contract: f64,
    opened_ts_ms: i64,
    seat_layer: Option<String>,
    tuned_params_before: Option<serde_json::Value>,
    tuned_params_after: Option<serde_json::Value>,
    rollback_triggered: Option<String>,
    shadow_pnl_comparison: Option<f64>,
}

#[derive(Debug, Clone)]
struct PaperDailyAgg {
    starting_bankroll: f64,
    ending_bankroll: f64,
    trades: u64,
    wins: u64,
    fee_total_usdc: f64,
    pnl_total_usdc: f64,
    durations_ms: Vec<f64>,
}

#[derive(Default)]
struct PaperRuntimeState {
    bankroll: f64,
    peak_bankroll: f64,
    max_drawdown_pct: f64,
    trades: u64,
    wins: u64,
    losses: u64,
    fee_total_usdc: f64,
    pnl_total_usdc: f64,
    intents: HashMap<String, PaperIntent>,
    open_lots: HashMap<String, VecDeque<PaperLot>>,
    chainlink_aux_by_market: HashMap<String, f64>,
    records: VecDeque<PaperTradeRecord>,
    daily: BTreeMap<String, PaperDailyAgg>,
}

#[derive(Clone)]
pub(crate) struct PaperRuntimeHandle {
    enabled: bool,
    run_id: String,
    initial_capital: f64,
    seat: Arc<SeatRuntimeHandle>,
    sqlite: PaperSqliteWriter,
    state: Arc<RwLock<PaperRuntimeState>>,
}

impl PaperRuntimeHandle {
    pub(crate) fn new(
        enabled: bool,
        run_id: String,
        initial_capital: f64,
        sqlite_enabled: bool,
        seat: Arc<SeatRuntimeHandle>,
    ) -> Arc<Self> {
        let sqlite_name = std::env::var("POLYEDGE_PAPER_SQLITE")
            .unwrap_or_else(|_| "paper_summary.sqlite".to_string());
        let sqlite_path = dataset_path("reports", &sqlite_name);
        let sqlite = PaperSqliteWriter::spawn(sqlite_path, sqlite_enabled && enabled);
        let mut state = PaperRuntimeState::default();
        state.bankroll = initial_capital;
        state.peak_bankroll = initial_capital;
        Arc::new(Self {
            enabled,
            run_id,
            initial_capital,
            seat,
            sqlite,
            state: Arc::new(RwLock::new(state)),
        })
    }

    pub(crate) async fn register_order_intent(&self, ack: &OrderAck, ctx: PaperIntentCtx) {
        if !self.enabled {
            return;
        }
        let seat_status = self.seat.status().await;
        let last_decision = self.seat.history(1).into_iter().next();
        let (tuned_before, tuned_after, rollback_triggered, shadow_pnl_comparison) =
            extract_last_decision(last_decision.as_ref());

        let intent = PaperIntent {
            ts_ms: ack.ts_ms,
            order_id: ack.order_id.clone(),
            market_id: ctx.market_id,
            symbol: ctx.symbol,
            timeframe: ctx.timeframe,
            stage: ctx.stage,
            direction: ctx.direction,
            velocity_bps_per_sec: ctx.velocity_bps_per_sec,
            edge_bps: ctx.edge_bps,
            prob_fast: ctx.prob_fast,
            prob_settle: ctx.prob_settle,
            confidence: ctx.confidence,
            action: ctx.action,
            intent: ctx.intent,
            requested_size_usdc: ctx.requested_size_usdc,
            requested_size_contracts: ctx.requested_size_contracts,
            entry_price: ctx.entry_price,
            seat_layer: Some(enum_text(&seat_status.current_layer)),
            tuned_params_before: tuned_before,
            tuned_params_after: tuned_after,
            rollback_triggered,
            shadow_pnl_comparison,
        };
        let mut s = self.state.write().await;
        s.intents.insert(ack.order_id.clone(), intent);
    }

    pub(crate) async fn on_fill(&self, fill: &FillEvent) {
        if !self.enabled {
            return;
        }
        let mut s = self.state.write().await;
        let ts_ms = fill.ts_ms.max(Utc::now().timestamp_millis());
        let mut intent = s.intents.remove(fill.order_id.as_str()).unwrap_or_else(|| {
            fallback_intent(
                fill,
                ts_ms,
                self.seat.history(1).into_iter().next().as_ref(),
            )
        });
        intent.requested_size_contracts = intent.requested_size_contracts.max(fill.size);
        intent.requested_size_usdc = intent
            .requested_size_usdc
            .max(fill.price * intent.requested_size_contracts);
        let mid = fill.mid_price.unwrap_or(fill.price).max(1e-9);
        let slippage_bps = fill
            .slippage_bps
            .unwrap_or_else(|| ((fill.price - mid) / mid) * 10_000.0);
        let _paper_fill = PaperFill {
            ts_ms,
            order_id: fill.order_id.clone(),
            market_id: fill.market_id.clone(),
            side: fill.side.clone(),
            style: fill.style.clone(),
            requested_size_usdc: intent.requested_size_usdc,
            executed_size_usdc: fill.price * fill.size,
            entry_price: intent.entry_price,
            fill_price: fill.price,
            mid_price: mid,
            slippage_bps,
            fee_usdc: fill.fee,
        };

        match fill.side {
            OrderSide::BuyYes | OrderSide::BuyNo => {
                self.open_lot_locked(&mut s, fill, intent, ts_ms);
            }
            OrderSide::SellYes | OrderSide::SellNo => {
                self.close_lot_locked(&mut s, fill, intent, ts_ms, slippage_bps);
            }
        }
        self.persist_reports_locked(&mut s);
    }

    pub(crate) async fn on_book(&self, book: &BookTop, chainlink_settlement_price: Option<f64>) {
        if !self.enabled {
            return;
        }
        let mut s = self.state.write().await;
        let now_ms = book.ts_ms.max(Utc::now().timestamp_millis());
        if let Some(price) = chainlink_settlement_price {
            if price.is_finite() && price > 0.0 {
                s.chainlink_aux_by_market
                    .insert(book.market_id.clone(), price);
            }
        }
        self.force_settle_expired_locked(&mut s, book, now_ms);
        self.persist_reports_locked(&mut s);
    }

    pub(crate) async fn reset(&self) {
        let mut s = self.state.write().await;
        *s = PaperRuntimeState {
            bankroll: self.initial_capital,
            peak_bankroll: self.initial_capital,
            ..PaperRuntimeState::default()
        };
        self.sqlite.reset();
        self.persist_reports_locked(&mut s);
    }

    pub(crate) async fn live_report(&self) -> PaperLiveReport {
        let s = self.state.read().await;
        build_live_report(&s, &self.run_id, self.initial_capital)
    }

    pub(crate) async fn history(&self, limit: usize) -> Vec<PaperTradeRecord> {
        let s = self.state.read().await;
        s.records.iter().rev().take(limit).cloned().collect()
    }

    pub(crate) async fn daily(&self) -> Vec<PaperDailySummary> {
        let s = self.state.read().await;
        build_daily_summaries(&s)
    }

    pub(crate) async fn summary_json(&self) -> serde_json::Value {
        let s = self.state.read().await;
        let live = build_live_report(&s, &self.run_id, self.initial_capital);
        let analytics = build_analytics(&s.records);
        serde_json::json!({
            "ts_ms": Utc::now().timestamp_millis(),
            "run_id": self.run_id,
            "initial_capital": self.initial_capital,
            "bankroll": live.bankroll,
            "roi_pct": live.roi_pct,
            "win_rate": live.win_rate,
            "trades": live.trades,
            "fee_total_usdc": live.fee_total_usdc,
            "pnl_total_usdc": live.pnl_total_usdc,
            "fee_ratio": live.fee_ratio,
            "avg_trade_duration_ms": live.avg_trade_duration_ms,
            "median_trade_duration_ms": live.median_trade_duration_ms,
            "open_positions_count": live.open_positions_count,
            "stage_distribution": analytics.stage_distribution,
            "action_distribution": analytics.action_distribution,
            "timeframe_distribution": analytics.timeframe_distribution,
            "seat": {
                "layer_distribution": analytics.seat_layer_distribution,
                "rollback_count": analytics.seat_rollback_count,
                "shadow_pnl_mean": analytics.shadow_pnl_mean,
            },
            "slippage": {
                "avg_abs_slippage_bps": analytics.avg_abs_slippage_bps,
                "avg_taker_abs_slippage_bps": analytics.avg_taker_abs_slippage_bps,
                "worst_abs_slippage_bps": analytics.worst_abs_slippage_bps,
            },
            "reversal": {
                "count": analytics.reversal_count,
                "losing_count": analytics.reversal_losing_count,
                "loss_rate": analytics.reversal_loss_rate,
            }
        })
    }

    fn open_lot_locked(
        &self,
        s: &mut PaperRuntimeState,
        fill: &FillEvent,
        mut intent: PaperIntent,
        ts_ms: i64,
    ) {
        let key = lot_key(fill.market_id.as_str(), &fill.side);
        let has_existing = s
            .open_lots
            .get(&key)
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        if matches!(intent.action, PaperAction::Enter | PaperAction::Add) {
            intent.action = if has_existing {
                PaperAction::Add
            } else {
                PaperAction::Enter
            };
        }
        let lot = PaperLot {
            market_id: fill.market_id.clone(),
            symbol: intent.symbol.clone(),
            timeframe: intent.timeframe.clone(),
            side: fill.side.clone(),
            stage: intent.stage.clone(),
            direction: intent.direction.clone(),
            velocity_bps_per_sec: intent.velocity_bps_per_sec,
            edge_bps: intent.edge_bps,
            prob_fast: intent.prob_fast,
            prob_settle: intent.prob_settle,
            confidence: intent.confidence,
            intent: fill.style.clone(),
            requested_size_usdc: intent.requested_size_usdc,
            entry_price: fill.price,
            remaining_size: fill.size.max(0.0),
            entry_fee_per_contract: if fill.size > 0.0 {
                fill.fee / fill.size
            } else {
                0.0
            },
            opened_ts_ms: ts_ms,
            seat_layer: intent.seat_layer,
            tuned_params_before: intent.tuned_params_before,
            tuned_params_after: intent.tuned_params_after,
            rollback_triggered: intent.rollback_triggered,
            shadow_pnl_comparison: intent.shadow_pnl_comparison,
        };
        s.fee_total_usdc += fill.fee;
        s.open_lots.entry(key).or_default().push_back(lot);
    }

    fn close_lot_locked(
        &self,
        s: &mut PaperRuntimeState,
        fill: &FillEvent,
        intent: PaperIntent,
        ts_ms: i64,
        slippage_bps: f64,
    ) {
        let close_target_side = match fill.side {
            OrderSide::SellYes => OrderSide::BuyYes,
            OrderSide::SellNo => OrderSide::BuyNo,
            _ => return,
        };
        let key = lot_key(fill.market_id.as_str(), &close_target_side);
        let Some(mut lots) = s.open_lots.remove(&key) else {
            return;
        };

        let mut remaining = fill.size.max(0.0);
        if remaining <= 0.0 {
            if !lots.is_empty() {
                s.open_lots.insert(key, lots);
            }
            return;
        }
        let exit_fee_per_contract = if fill.size > 0.0 {
            fill.fee / fill.size
        } else {
            0.0
        };
        while remaining > 1e-12 {
            let Some(front) = lots.front_mut() else {
                break;
            };
            let close_size = front.remaining_size.min(remaining);
            if close_size <= 1e-12 {
                break;
            }
            let entry_fee = front.entry_fee_per_contract * close_size;
            let exit_fee = exit_fee_per_contract * close_size;
            let pnl = (fill.price - front.entry_price) * close_size - entry_fee - exit_fee;
            let bankroll_before = s.bankroll;
            s.bankroll += pnl;
            s.peak_bankroll = s.peak_bankroll.max(s.bankroll);
            if s.peak_bankroll > 0.0 {
                let dd = (s.peak_bankroll - s.bankroll) / s.peak_bankroll;
                s.max_drawdown_pct = s.max_drawdown_pct.max(dd.max(0.0));
            }
            s.trades = s.trades.saturating_add(1);
            if pnl >= 0.0 {
                s.wins = s.wins.saturating_add(1);
            } else {
                s.losses = s.losses.saturating_add(1);
            }
            s.fee_total_usdc += exit_fee;
            s.pnl_total_usdc += pnl;
            let close_action = match intent.action {
                PaperAction::ReversalExit | PaperAction::LateHeavy | PaperAction::DoubleSide => {
                    intent.action.clone()
                }
                _ => {
                    if matches!(front.stage, Stage::Late) {
                        PaperAction::LateHeavy
                    } else {
                        PaperAction::ReversalExit
                    }
                }
            };
            let chainlink_settlement_price = s
                .chainlink_aux_by_market
                .get(&front.market_id)
                .copied()
                .filter(|v| v.is_finite() && *v > 0.0);
            let record = PaperTradeRecord {
                ts_ms,
                paper_mode: "shadow".to_string(),
                market_id: front.market_id.clone(),
                symbol: front.symbol.clone(),
                timeframe: front.timeframe.clone(),
                stage: front.stage.clone(),
                direction: front.direction.clone(),
                velocity_bps_per_sec: front.velocity_bps_per_sec,
                edge_bps: front.edge_bps,
                prob_fast: front.prob_fast,
                prob_settle: front.prob_settle,
                confidence: front.confidence,
                action: close_action,
                intent: fill.style.clone(),
                requested_size_usdc: front.requested_size_usdc,
                executed_size_usdc: fill.price * close_size,
                entry_price: front.entry_price,
                fill_price: fill.price,
                slippage_bps,
                fee_usdc: entry_fee + exit_fee,
                realized_pnl_usdc: pnl,
                bankroll_before,
                bankroll_after: s.bankroll,
                settlement_price: fill.price,
                chainlink_settlement_price,
                settlement_source: "exit_fill".to_string(),
                forced_settlement: false,
                trade_duration_ms: ts_ms.saturating_sub(front.opened_ts_ms),
                seat_layer: front.seat_layer.clone(),
                tuned_params_before: front.tuned_params_before.clone(),
                tuned_params_after: front.tuned_params_after.clone(),
                rollback_triggered: front.rollback_triggered.clone(),
                shadow_pnl_comparison: front.shadow_pnl_comparison,
            };
            self.push_record_locked(s, record);
            front.remaining_size -= close_size;
            if front.remaining_size <= 1e-12 {
                lots.pop_front();
            }
            remaining -= close_size;
        }
        if !lots.is_empty() {
            s.open_lots.insert(key, lots);
        }
    }

    fn force_settle_expired_locked(&self, s: &mut PaperRuntimeState, book: &BookTop, now_ms: i64) {
        let market_id = book.market_id.as_str();
        let binary_outcome = infer_binary_outcome_from_book(book);
        let mut keys = Vec::new();
        keys.push(lot_key(market_id, &OrderSide::BuyYes));
        keys.push(lot_key(market_id, &OrderSide::BuyNo));
        for key in keys {
            let Some(outcome) = binary_outcome else {
                // Do not settle against PM midpoint while unresolved. Keep lots open until
                // market converges to a binary outcome-like state (near 0/1).
                continue;
            };
            let Some(lots) = s.open_lots.get_mut(&key) else {
                continue;
            };
            let chainlink_settlement_price = s
                .chainlink_aux_by_market
                .get(market_id)
                .copied()
                .filter(|v| v.is_finite() && *v > 0.0);
            let mut settled = Vec::new();
            while let Some(front) = lots.front() {
                if !is_expired(front.opened_ts_ms, now_ms, front.timeframe.as_str()) {
                    break;
                }
                settled.push(front.clone());
                lots.pop_front();
            }
            for lot in settled {
                let close_price = match (outcome, &lot.side) {
                    (BinaryOutcome::Yes, OrderSide::BuyYes | OrderSide::SellYes) => 1.0,
                    (BinaryOutcome::Yes, OrderSide::BuyNo | OrderSide::SellNo) => 0.0,
                    (BinaryOutcome::No, OrderSide::BuyYes | OrderSide::SellYes) => 0.0,
                    (BinaryOutcome::No, OrderSide::BuyNo | OrderSide::SellNo) => 1.0,
                };
                let entry_fee = lot.entry_fee_per_contract * lot.remaining_size;
                let pnl = (close_price - lot.entry_price) * lot.remaining_size - entry_fee;
                let bankroll_before = s.bankroll;
                s.bankroll += pnl;
                s.peak_bankroll = s.peak_bankroll.max(s.bankroll);
                if s.peak_bankroll > 0.0 {
                    let dd = (s.peak_bankroll - s.bankroll) / s.peak_bankroll;
                    s.max_drawdown_pct = s.max_drawdown_pct.max(dd.max(0.0));
                }
                s.trades = s.trades.saturating_add(1);
                if pnl >= 0.0 {
                    s.wins = s.wins.saturating_add(1);
                } else {
                    s.losses = s.losses.saturating_add(1);
                }
                s.pnl_total_usdc += pnl;
                let record = PaperTradeRecord {
                    ts_ms: now_ms,
                    paper_mode: "shadow".to_string(),
                    market_id: lot.market_id.clone(),
                    symbol: lot.symbol.clone(),
                    timeframe: lot.timeframe.clone(),
                    stage: lot.stage.clone(),
                    direction: lot.direction.clone(),
                    velocity_bps_per_sec: lot.velocity_bps_per_sec,
                    edge_bps: lot.edge_bps,
                    prob_fast: lot.prob_fast,
                    prob_settle: lot.prob_settle,
                    confidence: lot.confidence,
                    action: PaperAction::LateHeavy,
                    intent: lot.intent.clone(),
                    requested_size_usdc: lot.requested_size_usdc,
                    executed_size_usdc: close_price * lot.remaining_size,
                    entry_price: lot.entry_price,
                    fill_price: close_price,
                    slippage_bps: 0.0,
                    fee_usdc: entry_fee,
                    realized_pnl_usdc: pnl,
                    bankroll_before,
                    bankroll_after: s.bankroll,
                    settlement_price: close_price,
                    chainlink_settlement_price,
                    settlement_source: binary_settlement_source(outcome).to_string(),
                    forced_settlement: true,
                    trade_duration_ms: now_ms.saturating_sub(lot.opened_ts_ms),
                    seat_layer: lot.seat_layer.clone(),
                    tuned_params_before: lot.tuned_params_before.clone(),
                    tuned_params_after: lot.tuned_params_after.clone(),
                    rollback_triggered: lot.rollback_triggered.clone(),
                    shadow_pnl_comparison: lot.shadow_pnl_comparison,
                };
                self.push_record_locked(s, record);
            }
        }
    }

    fn push_record_locked(&self, s: &mut PaperRuntimeState, record: PaperTradeRecord) {
        update_daily(s, &record);
        append_jsonl(
            &dataset_path("normalized", "paper_records.jsonl"),
            &serde_json::json!(record),
        );
        self.sqlite.push_trade(&record);
        s.records.push_back(record);
        if s.records.len() > HISTORY_CAP {
            let _ = s.records.pop_front();
        }
    }

    fn persist_reports_locked(&self, s: &mut PaperRuntimeState) {
        let live = build_live_report(s, &self.run_id, self.initial_capital);
        let analytics = build_analytics(&s.records);
        let summary = serde_json::json!({
            "run_id": self.run_id,
            "ts_ms": live.ts_ms,
            "initial_capital": self.initial_capital,
            "bankroll": live.bankroll,
            "roi_pct": live.roi_pct,
            "trades": live.trades,
            "wins": live.wins,
            "losses": live.losses,
            "win_rate": live.win_rate,
            "max_drawdown_pct": live.max_drawdown_pct,
            "fee_total_usdc": live.fee_total_usdc,
            "pnl_total_usdc": live.pnl_total_usdc,
            "fee_ratio": live.fee_ratio,
            "avg_trade_duration_ms": live.avg_trade_duration_ms,
            "median_trade_duration_ms": live.median_trade_duration_ms,
            "open_positions_count": live.open_positions_count,
            "stage_distribution": analytics.stage_distribution,
            "action_distribution": analytics.action_distribution,
            "timeframe_distribution": analytics.timeframe_distribution,
            "seat": {
                "layer_distribution": analytics.seat_layer_distribution,
                "rollback_count": analytics.seat_rollback_count,
                "shadow_pnl_mean": analytics.shadow_pnl_mean,
            },
            "slippage": {
                "avg_abs_slippage_bps": analytics.avg_abs_slippage_bps,
                "avg_taker_abs_slippage_bps": analytics.avg_taker_abs_slippage_bps,
                "worst_abs_slippage_bps": analytics.worst_abs_slippage_bps,
            },
            "reversal": {
                "count": analytics.reversal_count,
                "losing_count": analytics.reversal_losing_count,
                "loss_rate": analytics.reversal_loss_rate,
            }
        });
        let diagnosis = serde_json::json!({
            "ts_ms": live.ts_ms,
            "alerts": build_diagnosis_alerts(&live, &analytics),
            "root_causes": build_root_causes(&live, &analytics),
            "slippage": {
                "avg_abs_slippage_bps": analytics.avg_abs_slippage_bps,
                "avg_taker_abs_slippage_bps": analytics.avg_taker_abs_slippage_bps,
                "worst_abs_slippage_bps": analytics.worst_abs_slippage_bps,
            },
            "reversal": {
                "count": analytics.reversal_count,
                "losing_count": analytics.reversal_losing_count,
                "loss_rate": analytics.reversal_loss_rate,
            },
            "seat": {
                "layer_distribution": analytics.seat_layer_distribution,
                "rollback_count": analytics.seat_rollback_count,
                "shadow_pnl_mean": analytics.shadow_pnl_mean,
            },
            "stage_distribution": analytics.stage_distribution,
            "action_distribution": analytics.action_distribution,
        });
        let daily = build_daily_summaries(s);
        write_json_file(dataset_path("reports", "paper_live_latest.json"), &live);
        write_json_file(
            dataset_path("reports", "paper_summary_latest.json"),
            &summary,
        );
        write_json_file(
            dataset_path("reports", "paper_diagnosis_latest.json"),
            &diagnosis,
        );
        write_daily_csv(
            dataset_path("reports", "daily_compound_summary.csv"),
            &daily,
        );
        for day in &daily {
            self.sqlite.push_daily(day);
        }
        self.sqlite.push_summary(&live);
    }
}

pub(crate) fn set_global_paper_runtime(handle: Arc<PaperRuntimeHandle>) {
    let _ = GLOBAL_PAPER_RUNTIME.set(handle);
}

pub(crate) fn global_paper_runtime() -> Option<Arc<PaperRuntimeHandle>> {
    GLOBAL_PAPER_RUNTIME.get().cloned()
}

fn fallback_intent(
    fill: &FillEvent,
    ts_ms: i64,
    decision: Option<&SeatDecisionRecord>,
) -> PaperIntent {
    let (tuned_before, tuned_after, rollback_triggered, shadow_pnl_comparison) =
        extract_last_decision(decision);
    PaperIntent {
        ts_ms,
        order_id: fill.order_id.clone(),
        market_id: fill.market_id.clone(),
        symbol: fill.market_id.clone(),
        timeframe: "unknown".to_string(),
        stage: Stage::Early,
        direction: match fill.side {
            OrderSide::BuyYes | OrderSide::SellNo => Direction::Up,
            OrderSide::BuyNo | OrderSide::SellYes => Direction::Down,
        },
        velocity_bps_per_sec: 0.0,
        edge_bps: 0.0,
        prob_fast: 0.5,
        prob_settle: 0.5,
        confidence: 0.0,
        action: PaperAction::Enter,
        intent: fill.style.clone(),
        requested_size_usdc: fill.price * fill.size,
        requested_size_contracts: fill.size,
        entry_price: fill.price,
        seat_layer: None,
        tuned_params_before: tuned_before,
        tuned_params_after: tuned_after,
        rollback_triggered,
        shadow_pnl_comparison,
    }
}

fn extract_last_decision(
    decision: Option<&SeatDecisionRecord>,
) -> (
    Option<serde_json::Value>,
    Option<serde_json::Value>,
    Option<String>,
    Option<f64>,
) {
    let Some(d) = decision else {
        return (None, None, None, None);
    };
    let before = serde_json::to_value(&d.previous).ok();
    let after = serde_json::to_value(&d.candidate).ok();
    let rollback_triggered = if d.rollback {
        Some(d.decision.clone())
    } else {
        None
    };
    let shadow_pnl = d
        .notes
        .iter()
        .find_map(|v| v.strip_prefix("shadow_ev_usdc_p50="))
        .and_then(|v| v.parse::<f64>().ok());
    (before, after, rollback_triggered, shadow_pnl)
}

fn update_daily(s: &mut PaperRuntimeState, record: &PaperTradeRecord) {
    let day = Utc
        .timestamp_millis_opt(record.ts_ms)
        .single()
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string();
    let entry = s.daily.entry(day).or_insert_with(|| PaperDailyAgg {
        starting_bankroll: record.bankroll_before,
        ending_bankroll: record.bankroll_after,
        trades: 0,
        wins: 0,
        fee_total_usdc: 0.0,
        pnl_total_usdc: 0.0,
        durations_ms: Vec::new(),
    });
    entry.trades = entry.trades.saturating_add(1);
    if record.realized_pnl_usdc >= 0.0 {
        entry.wins = entry.wins.saturating_add(1);
    }
    entry.ending_bankroll = record.bankroll_after;
    entry.fee_total_usdc += record.fee_usdc;
    entry.pnl_total_usdc += record.realized_pnl_usdc;
    entry
        .durations_ms
        .push(record.trade_duration_ms.max(0) as f64);
}

fn build_live_report(s: &PaperRuntimeState, run_id: &str, initial_capital: f64) -> PaperLiveReport {
    let durations = s
        .records
        .iter()
        .map(|r| r.trade_duration_ms.max(0) as f64)
        .collect::<Vec<_>>();
    let avg_trade_duration_ms = if durations.is_empty() {
        0.0
    } else {
        durations.iter().sum::<f64>() / durations.len() as f64
    };
    let median_trade_duration_ms = percentile(&durations, 0.50).unwrap_or(0.0);
    let win_rate = if s.trades == 0 {
        0.0
    } else {
        s.wins as f64 / s.trades as f64
    };
    let roi_pct = if initial_capital.abs() < 1e-9 {
        0.0
    } else {
        (s.bankroll - initial_capital) / initial_capital * 100.0
    };
    let fee_ratio = if s.pnl_total_usdc.abs() < 1e-9 {
        0.0
    } else {
        (s.fee_total_usdc / s.pnl_total_usdc.abs()).abs()
    };
    let open_positions_count = s.open_lots.values().map(|v| v.len()).sum::<usize>();
    PaperLiveReport {
        ts_ms: Utc::now().timestamp_millis(),
        run_id: run_id.to_string(),
        initial_capital,
        bankroll: s.bankroll,
        trades: s.trades,
        wins: s.wins,
        losses: s.losses,
        win_rate,
        roi_pct,
        max_drawdown_pct: s.max_drawdown_pct * 100.0,
        fee_total_usdc: s.fee_total_usdc,
        pnl_total_usdc: s.pnl_total_usdc,
        fee_ratio,
        avg_trade_duration_ms,
        median_trade_duration_ms,
        trade_count_source: "shadow_fill".to_string(),
        open_positions_count,
    }
}

fn build_daily_summaries(s: &PaperRuntimeState) -> Vec<PaperDailySummary> {
    s.daily
        .iter()
        .map(|(day, agg)| {
            let win_rate = if agg.trades == 0 {
                0.0
            } else {
                agg.wins as f64 / agg.trades as f64
            };
            let daily_roi_pct = if agg.starting_bankroll.abs() < 1e-9 {
                0.0
            } else {
                (agg.ending_bankroll - agg.starting_bankroll) / agg.starting_bankroll * 100.0
            };
            let avg_trade_duration_ms = if agg.durations_ms.is_empty() {
                0.0
            } else {
                agg.durations_ms.iter().sum::<f64>() / agg.durations_ms.len() as f64
            };
            let median_trade_duration_ms = percentile(&agg.durations_ms, 0.50).unwrap_or(0.0);
            PaperDailySummary {
                utc_day: day.clone(),
                starting_bankroll: agg.starting_bankroll,
                ending_bankroll: agg.ending_bankroll,
                daily_roi_pct,
                trades: agg.trades,
                win_rate,
                fee_total_usdc: agg.fee_total_usdc,
                pnl_total_usdc: agg.pnl_total_usdc,
                avg_trade_duration_ms,
                median_trade_duration_ms,
            }
        })
        .collect()
}

#[derive(Default)]
struct PaperAnalytics {
    stage_distribution: BTreeMap<String, u64>,
    action_distribution: BTreeMap<String, u64>,
    timeframe_distribution: BTreeMap<String, u64>,
    seat_layer_distribution: BTreeMap<String, u64>,
    seat_rollback_count: u64,
    shadow_pnl_mean: f64,
    avg_abs_slippage_bps: f64,
    avg_taker_abs_slippage_bps: f64,
    worst_abs_slippage_bps: f64,
    reversal_count: u64,
    reversal_losing_count: u64,
    reversal_loss_rate: f64,
}

fn build_analytics(records: &VecDeque<PaperTradeRecord>) -> PaperAnalytics {
    let mut out = PaperAnalytics::default();
    let mut abs_slippage_sum = 0.0;
    let mut abs_slippage_cnt = 0_u64;
    let mut taker_abs_slippage_sum = 0.0;
    let mut taker_abs_slippage_cnt = 0_u64;
    let mut shadow_pnl_sum = 0.0;
    let mut shadow_pnl_cnt = 0_u64;

    for r in records {
        *out.stage_distribution
            .entry(enum_text(&r.stage))
            .or_default() += 1;
        *out.action_distribution
            .entry(enum_text(&r.action))
            .or_default() += 1;
        *out.timeframe_distribution
            .entry(r.timeframe.clone())
            .or_default() += 1;
        if let Some(layer) = &r.seat_layer {
            *out.seat_layer_distribution
                .entry(layer.clone())
                .or_default() += 1;
        }
        if r.rollback_triggered.is_some() {
            out.seat_rollback_count = out.seat_rollback_count.saturating_add(1);
        }
        if let Some(v) = r.shadow_pnl_comparison {
            if v.is_finite() {
                shadow_pnl_sum += v;
                shadow_pnl_cnt = shadow_pnl_cnt.saturating_add(1);
            }
        }

        let abs_slippage = r.slippage_bps.abs();
        if abs_slippage.is_finite() {
            abs_slippage_sum += abs_slippage;
            abs_slippage_cnt = abs_slippage_cnt.saturating_add(1);
            out.worst_abs_slippage_bps = out.worst_abs_slippage_bps.max(abs_slippage);
            if !matches!(r.intent, ExecutionStyle::Maker) {
                taker_abs_slippage_sum += abs_slippage;
                taker_abs_slippage_cnt = taker_abs_slippage_cnt.saturating_add(1);
            }
        }
        if matches!(r.action, PaperAction::ReversalExit) {
            out.reversal_count = out.reversal_count.saturating_add(1);
            if r.realized_pnl_usdc < 0.0 {
                out.reversal_losing_count = out.reversal_losing_count.saturating_add(1);
            }
        }
    }

    out.shadow_pnl_mean = if shadow_pnl_cnt == 0 {
        0.0
    } else {
        shadow_pnl_sum / shadow_pnl_cnt as f64
    };
    out.avg_abs_slippage_bps = if abs_slippage_cnt == 0 {
        0.0
    } else {
        abs_slippage_sum / abs_slippage_cnt as f64
    };
    out.avg_taker_abs_slippage_bps = if taker_abs_slippage_cnt == 0 {
        0.0
    } else {
        taker_abs_slippage_sum / taker_abs_slippage_cnt as f64
    };
    out.reversal_loss_rate = if out.reversal_count == 0 {
        0.0
    } else {
        out.reversal_losing_count as f64 / out.reversal_count as f64
    };
    out
}

fn build_diagnosis_alerts(live: &PaperLiveReport, analytics: &PaperAnalytics) -> Vec<String> {
    let mut alerts = Vec::new();
    if live.trades == 0 {
        alerts.push("no_trades_recorded".to_string());
    }
    if live.fee_ratio > 0.5 && live.trades > 0 {
        alerts.push("fee_ratio_high".to_string());
    }
    if live.max_drawdown_pct > 20.0 {
        alerts.push("drawdown_high".to_string());
    }
    if analytics.avg_taker_abs_slippage_bps > 15.0 && live.trades >= 10 {
        alerts.push("taker_slippage_high".to_string());
    }
    if analytics.reversal_loss_rate > 0.60 && analytics.reversal_count >= 10 {
        alerts.push("reversal_exit_underperforming".to_string());
    }
    if analytics.seat_rollback_count >= 2 {
        alerts.push("seat_rollback_frequent".to_string());
    }
    alerts
}

fn build_root_causes(live: &PaperLiveReport, analytics: &PaperAnalytics) -> Vec<String> {
    let mut causes = Vec::new();
    if live.trades == 0 {
        causes.push("entry_conditions_too_strict_or_feed_inactive".to_string());
    }
    if live.fee_ratio > 0.6 {
        causes.push("fee_dominates_edge_tune_min_edge_or_taker_usage".to_string());
    }
    if analytics.avg_taker_abs_slippage_bps > 15.0 {
        causes.push("taker_slippage_excessive_consider_maker_bias_or_spread_filter".to_string());
    }
    if analytics.reversal_loss_rate > 0.60 && analytics.reversal_count >= 10 {
        causes
            .push("reversal_exit_rule_quality_low_check_velocity_and_edge_thresholds".to_string());
    }
    if live.max_drawdown_pct > 20.0 {
        causes
            .push("risk_controls_too_loose_reduce_position_fraction_or_drawdown_limit".to_string());
    }
    if causes.is_empty() {
        causes.push("no_critical_issue_detected".to_string());
    }
    causes
}

fn lot_key(market_id: &str, side: &OrderSide) -> String {
    format!("{market_id}|{}", enum_text(side))
}

fn frame_ms(timeframe: &str) -> i64 {
    match timeframe {
        "5m" | "5min" => 5 * 60 * 1_000,
        "15m" | "15min" => 15 * 60 * 1_000,
        "1h" => 60 * 60 * 1_000,
        "1d" => 24 * 60 * 60 * 1_000,
        _ => i64::MAX,
    }
}

#[derive(Clone, Copy)]
enum BinaryOutcome {
    Yes,
    No,
}

fn binary_settlement_thresholds() -> (f64, f64) {
    let high = std::env::var("POLYEDGE_PAPER_BINARY_SETTLE_HIGH")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(DEFAULT_BINARY_SETTLE_HIGH)
        .clamp(0.50, 0.9999);
    let low = std::env::var("POLYEDGE_PAPER_BINARY_SETTLE_LOW")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(DEFAULT_BINARY_SETTLE_LOW)
        .clamp(0.0001, 0.50);
    (high.max(low), low.min(high))
}

fn infer_binary_outcome_from_book(book: &BookTop) -> Option<BinaryOutcome> {
    let bid_yes = book.bid_yes.clamp(0.0, 1.0);
    let ask_yes = book.ask_yes.clamp(0.0, 1.0);
    let bid_no = book.bid_no.clamp(0.0, 1.0);
    let ask_no = book.ask_no.clamp(0.0, 1.0);
    let (high, low) = binary_settlement_thresholds();
    if bid_yes >= high || ask_no <= low {
        Some(BinaryOutcome::Yes)
    } else if ask_yes <= low || bid_no >= high {
        Some(BinaryOutcome::No)
    } else {
        None
    }
}

fn binary_settlement_source(outcome: BinaryOutcome) -> &'static str {
    match outcome {
        BinaryOutcome::Yes => "binary_book_bbo_yes",
        BinaryOutcome::No => "binary_book_bbo_no",
    }
}

fn is_expired(opened_ts_ms: i64, now_ms: i64, timeframe: &str) -> bool {
    let fm = frame_ms(timeframe);
    if fm == i64::MAX || opened_ts_ms <= 0 || now_ms <= 0 {
        return false;
    }
    opened_ts_ms.div_euclid(fm) < now_ms.div_euclid(fm)
}

fn enum_text<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn write_json_file(path: PathBuf, value: &impl serde::Serialize) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_vec_pretty(value) {
        let _ = std::fs::write(path, raw);
    }
}

fn write_daily_csv(path: PathBuf, days: &[PaperDailySummary]) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut out = String::new();
    out.push_str("utc_day,starting_bankroll,ending_bankroll,daily_roi_pct,trades,win_rate,fee_total_usdc,pnl_total_usdc,avg_trade_duration_ms,median_trade_duration_ms\n");
    for d in days {
        out.push_str(&format!(
            "{},{:.6},{:.6},{:.6},{},{:.6},{:.6},{:.6},{:.3},{:.3}\n",
            d.utc_day,
            d.starting_bankroll,
            d.ending_bankroll,
            d.daily_roi_pct,
            d.trades,
            d.win_rate,
            d.fee_total_usdc,
            d.pnl_total_usdc,
            d.avg_trade_duration_ms,
            d.median_trade_duration_ms
        ));
    }
    let _ = std::fs::write(path, out);
}
