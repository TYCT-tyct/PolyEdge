use std::fs;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use core_types::{PaperDailySummary, PaperLiveReport, PaperTradeRecord};
use rusqlite::{params, Connection};

#[derive(Debug)]
enum SqliteMsg {
    Trade(PaperTradeRecord),
    Daily(PaperDailySummary),
    Summary(PaperLiveReport),
    Reset,
}

#[derive(Clone, Default)]
pub(crate) struct PaperSqliteWriter {
    tx: Option<mpsc::Sender<SqliteMsg>>,
}

impl PaperSqliteWriter {
    pub(crate) fn spawn(path: PathBuf, enabled: bool) -> Self {
        if !enabled {
            return Self { tx: None };
        }
        let (tx, rx) = mpsc::channel::<SqliteMsg>();
        thread::spawn(move || {
            if let Err(err) = run_writer(path, rx) {
                tracing::warn!(error = %err, "paper sqlite writer exited");
            }
        });
        Self { tx: Some(tx) }
    }

    pub(crate) fn push_trade(&self, record: &PaperTradeRecord) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(SqliteMsg::Trade(record.clone()));
        }
    }

    pub(crate) fn push_daily(&self, daily: &PaperDailySummary) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(SqliteMsg::Daily(daily.clone()));
        }
    }

    pub(crate) fn push_summary(&self, report: &PaperLiveReport) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(SqliteMsg::Summary(report.clone()));
        }
    }

    pub(crate) fn reset(&self) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(SqliteMsg::Reset);
        }
    }
}

fn run_writer(path: PathBuf, rx: mpsc::Receiver<SqliteMsg>) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    init_schema(&conn)?;

    while let Ok(msg) = rx.recv() {
        match msg {
            SqliteMsg::Trade(r) => insert_trade(&conn, &r)?,
            SqliteMsg::Daily(d) => upsert_daily(&conn, &d)?,
            SqliteMsg::Summary(s) => upsert_summary(&conn, &s)?,
            SqliteMsg::Reset => reset_all(&conn)?,
        }
    }
    Ok(())
}

fn init_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS paper_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms INTEGER NOT NULL,
            paper_mode TEXT NOT NULL,
            market_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            stage TEXT NOT NULL,
            direction TEXT NOT NULL,
            velocity_bps_per_sec REAL NOT NULL,
            edge_bps REAL NOT NULL,
            prob_fast REAL NOT NULL,
            prob_settle REAL NOT NULL,
            confidence REAL NOT NULL,
            action TEXT NOT NULL,
            intent TEXT NOT NULL,
            requested_size_usdc REAL NOT NULL,
            executed_size_usdc REAL NOT NULL,
            entry_price REAL NOT NULL,
            fill_price REAL NOT NULL,
            slippage_bps REAL NOT NULL,
            fee_usdc REAL NOT NULL,
            realized_pnl_usdc REAL NOT NULL,
            bankroll_before REAL NOT NULL,
            bankroll_after REAL NOT NULL,
            settlement_price REAL NOT NULL,
            settlement_source TEXT NOT NULL,
            forced_settlement INTEGER NOT NULL,
            trade_duration_ms INTEGER NOT NULL,
            seat_layer TEXT NULL,
            tuned_params_before TEXT NULL,
            tuned_params_after TEXT NULL,
            rollback_triggered TEXT NULL,
            shadow_pnl_comparison REAL NULL
        );
        CREATE INDEX IF NOT EXISTS idx_paper_records_ts ON paper_records(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_paper_records_market ON paper_records(market_id);

        CREATE TABLE IF NOT EXISTS paper_daily_summary (
            utc_day TEXT PRIMARY KEY,
            starting_bankroll REAL NOT NULL,
            ending_bankroll REAL NOT NULL,
            daily_roi_pct REAL NOT NULL,
            trades INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            fee_total_usdc REAL NOT NULL,
            pnl_total_usdc REAL NOT NULL,
            avg_trade_duration_ms REAL NOT NULL,
            median_trade_duration_ms REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_run_summary (
            run_id TEXT PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            initial_capital REAL NOT NULL,
            bankroll REAL NOT NULL,
            trades INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            roi_pct REAL NOT NULL,
            max_drawdown_pct REAL NOT NULL,
            fee_total_usdc REAL NOT NULL,
            pnl_total_usdc REAL NOT NULL,
            fee_ratio REAL NOT NULL,
            avg_trade_duration_ms REAL NOT NULL,
            median_trade_duration_ms REAL NOT NULL,
            trade_count_source TEXT NOT NULL,
            open_positions_count INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

fn insert_trade(conn: &Connection, r: &PaperTradeRecord) -> anyhow::Result<()> {
    conn.execute(
        r#"
        INSERT INTO paper_records (
            ts_ms, paper_mode, market_id, symbol, timeframe, stage, direction,
            velocity_bps_per_sec, edge_bps, prob_fast, prob_settle, confidence, action, intent,
            requested_size_usdc, executed_size_usdc, entry_price, fill_price, slippage_bps,
            fee_usdc, realized_pnl_usdc, bankroll_before, bankroll_after, settlement_price,
            settlement_source, forced_settlement, trade_duration_ms, seat_layer,
            tuned_params_before, tuned_params_after, rollback_triggered, shadow_pnl_comparison
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?
        )
        "#,
        params![
            r.ts_ms,
            r.paper_mode,
            r.market_id,
            r.symbol,
            r.timeframe,
            enum_text(&r.stage),
            enum_text(&r.direction),
            r.velocity_bps_per_sec,
            r.edge_bps,
            r.prob_fast,
            r.prob_settle,
            r.confidence,
            enum_text(&r.action),
            enum_text(&r.intent),
            r.requested_size_usdc,
            r.executed_size_usdc,
            r.entry_price,
            r.fill_price,
            r.slippage_bps,
            r.fee_usdc,
            r.realized_pnl_usdc,
            r.bankroll_before,
            r.bankroll_after,
            r.settlement_price,
            r.settlement_source,
            if r.forced_settlement { 1_i64 } else { 0_i64 },
            r.trade_duration_ms,
            r.seat_layer,
            r.tuned_params_before.as_ref().map(|v| v.to_string()),
            r.tuned_params_after.as_ref().map(|v| v.to_string()),
            r.rollback_triggered,
            r.shadow_pnl_comparison,
        ],
    )?;
    Ok(())
}

fn upsert_daily(conn: &Connection, d: &PaperDailySummary) -> anyhow::Result<()> {
    conn.execute(
        r#"
        INSERT INTO paper_daily_summary (
            utc_day, starting_bankroll, ending_bankroll, daily_roi_pct,
            trades, win_rate, fee_total_usdc, pnl_total_usdc,
            avg_trade_duration_ms, median_trade_duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(utc_day) DO UPDATE SET
            starting_bankroll=excluded.starting_bankroll,
            ending_bankroll=excluded.ending_bankroll,
            daily_roi_pct=excluded.daily_roi_pct,
            trades=excluded.trades,
            win_rate=excluded.win_rate,
            fee_total_usdc=excluded.fee_total_usdc,
            pnl_total_usdc=excluded.pnl_total_usdc,
            avg_trade_duration_ms=excluded.avg_trade_duration_ms,
            median_trade_duration_ms=excluded.median_trade_duration_ms
        "#,
        params![
            d.utc_day,
            d.starting_bankroll,
            d.ending_bankroll,
            d.daily_roi_pct,
            d.trades as i64,
            d.win_rate,
            d.fee_total_usdc,
            d.pnl_total_usdc,
            d.avg_trade_duration_ms,
            d.median_trade_duration_ms
        ],
    )?;
    Ok(())
}

fn upsert_summary(conn: &Connection, s: &PaperLiveReport) -> anyhow::Result<()> {
    conn.execute(
        r#"
        INSERT INTO paper_run_summary (
            run_id, ts_ms, initial_capital, bankroll, trades, wins, losses, win_rate, roi_pct,
            max_drawdown_pct, fee_total_usdc, pnl_total_usdc, fee_ratio,
            avg_trade_duration_ms, median_trade_duration_ms, trade_count_source, open_positions_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            ts_ms=excluded.ts_ms,
            initial_capital=excluded.initial_capital,
            bankroll=excluded.bankroll,
            trades=excluded.trades,
            wins=excluded.wins,
            losses=excluded.losses,
            win_rate=excluded.win_rate,
            roi_pct=excluded.roi_pct,
            max_drawdown_pct=excluded.max_drawdown_pct,
            fee_total_usdc=excluded.fee_total_usdc,
            pnl_total_usdc=excluded.pnl_total_usdc,
            fee_ratio=excluded.fee_ratio,
            avg_trade_duration_ms=excluded.avg_trade_duration_ms,
            median_trade_duration_ms=excluded.median_trade_duration_ms,
            trade_count_source=excluded.trade_count_source,
            open_positions_count=excluded.open_positions_count
        "#,
        params![
            s.run_id,
            s.ts_ms,
            s.initial_capital,
            s.bankroll,
            s.trades as i64,
            s.wins as i64,
            s.losses as i64,
            s.win_rate,
            s.roi_pct,
            s.max_drawdown_pct,
            s.fee_total_usdc,
            s.pnl_total_usdc,
            s.fee_ratio,
            s.avg_trade_duration_ms,
            s.median_trade_duration_ms,
            s.trade_count_source,
            s.open_positions_count as i64,
        ],
    )?;
    Ok(())
}

fn reset_all(conn: &Connection) -> anyhow::Result<()> {
    conn.execute("DELETE FROM paper_records", [])?;
    conn.execute("DELETE FROM paper_daily_summary", [])?;
    conn.execute("DELETE FROM paper_run_summary", [])?;
    Ok(())
}

fn enum_text<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}
