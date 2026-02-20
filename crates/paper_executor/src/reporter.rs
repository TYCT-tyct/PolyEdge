use anyhow::Result;
use core_types::TradeReport;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct PaperReporter {
    tx: mpsc::Sender<TradeReport>,
    _worker: JoinHandle<()>,
}

impl PaperReporter {
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_path_buf = db_path.as_ref().to_path_buf();

        // 10K capacity for paper testing micro-bursts
        let (tx, mut rx) = mpsc::channel::<TradeReport>(10_000);

        let worker = tokio::task::spawn_blocking(move || {
            let mut conn = Connection::open(&db_path_buf).expect("PaperReporter: Failed to open DB");
            Self::init_schema(&conn).expect("PaperReporter: Failed to initialize schema");

            let mut batch = Vec::with_capacity(50);

            // Blocking recv loop on the dedicated blocking thread
            while let Some(report) = rx.blocking_recv() {
                batch.push(report);

                // Drain any immediately available messages into the batch
                while batch.len() < 50 {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break, // Empty or disconnected
                    }
                }

                if let Err(e) = Self::insert_batch(&mut conn, &batch) {
                    eprintln!("PaperReporter: failed to insert batch: {}", e);
                }
                batch.clear();
            }
        });

        Ok(Self { tx, _worker: worker })
    }

    pub fn record_trade(&self, report: TradeReport) {
        if let Err(e) = self.tx.try_send(report) {
            eprintln!("PaperReporter: dropped trade report due to channel full: {}", e);
        }
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
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
                forced_settlement BOOLEAN NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id);
            CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp_ms);
            "#,
        )?;
        Ok(())
    }

    fn insert_batch(conn: &mut Connection, batch: &[TradeReport]) -> Result<()> {
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                r#"
                INSERT INTO trades (
                    timestamp_ms, market_id, symbol, timeframe, stage, direction,
                    velocity_bps_per_sec, edge_bps, prob_fast, prob_settle, confidence,
                    action, intent, requested_size_usdc, executed_size_usdc,
                    entry_price, fill_price, slippage_bps, fee_usdc, realized_pnl_usdc,
                    bankroll_before, bankroll_after, settlement_price, settlement_source,
                    forced_settlement
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                    ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25
                )
                "#,
            )?;

            for r in batch {
                stmt.execute(params![
                    r.timestamp_ms,
                    r.market_id,
                    r.symbol,
                    r.timeframe,
                    r.stage,
                    r.direction,
                    r.velocity_bps_per_sec,
                    r.edge_bps,
                    r.prob_fast,
                    r.prob_settle,
                    r.confidence,
                    r.action,
                    r.intent,
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
                    r.forced_settlement
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}
