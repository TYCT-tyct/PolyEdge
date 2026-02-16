use std::collections::HashMap;

use chrono::Utc;
use core_types::{FillEvent, OrderSide, PnLSnapshot};
use parking_lot::RwLock;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Default)]
pub struct Position {
    pub market_id: String,
    pub yes: f64,
    pub no: f64,
    pub avg_yes: f64,
    pub avg_no: f64,
}

#[derive(Default)]
pub struct PortfolioBook {
    positions: RwLock<HashMap<String, Position>>,
    realized: RwLock<f64>,
    equity_peak: RwLock<f64>,
}

impl PortfolioBook {
    pub fn apply_fill(&self, fill: &FillEvent) {
        let mut positions = self.positions.write();
        let pos = positions
            .entry(fill.market_id.clone())
            .or_insert_with(|| Position {
                market_id: fill.market_id.clone(),
                ..Position::default()
            });

        match fill.side {
            OrderSide::BuyYes => {
                pos.avg_yes = weighted_avg(pos.avg_yes, pos.yes, fill.price, fill.size);
                pos.yes += fill.size;
            }
            OrderSide::SellYes => {
                // Use min to prevent negative position: only close up to existing position
                let closed = fill.size.min(pos.yes);
                *self.realized.write() += (fill.price - pos.avg_yes) * closed - fill.fee;
                // Only reduce by actual closed amount, not full fill size
                pos.yes = (pos.yes - closed).max(0.0);
            }
            OrderSide::BuyNo => {
                pos.avg_no = weighted_avg(pos.avg_no, pos.no, fill.price, fill.size);
                pos.no += fill.size;
            }
            OrderSide::SellNo => {
                // Use min to prevent negative position: only close up to existing position
                let closed = fill.size.min(pos.no);
                *self.realized.write() += (fill.price - pos.avg_no) * closed - fill.fee;
                // Only reduce by actual closed amount, not full fill size
                pos.no = (pos.no - closed).max(0.0);
            }
        }
    }

    pub fn positions(&self) -> HashMap<String, Position> {
        self.positions.read().clone()
    }

    /// Calculate unrealized PnL based on current market prices
    /// If no prices provided, unrealized is treated as 0
    pub fn snapshot_with_prices(&self, prices: &HashMap<String, f64>) -> PnLSnapshot {
        let realized = *self.realized.read();

        // Calculate unrealized PnL using current market prices
        let positions = self.positions.read();
        let mut unrealized = 0.0;
        for (market_id, pos) in positions.iter() {
            if let Some(&current_price) = prices.get(market_id) {
                // Unrealized = (current_price - avg_price) * position_size
                // For yes position: (current - avg_yes) * yes
                // For no position: (current - (1-avg_no)) * no = (avg_no - (1-current)) * no
                unrealized += (current_price - pos.avg_yes).max(0.0) * pos.yes;
                unrealized += ((1.0 - pos.avg_no) - (1.0 - current_price)).max(0.0) * pos.no;
            }
        }
        drop(positions);

        let equity = realized + unrealized;

        let mut peak = self.equity_peak.write();
        if equity > *peak {
            *peak = equity;
        }
        let drawdown = if *peak <= 0.0 {
            0.0
        } else {
            ((*peak - equity) / peak.abs()).max(0.0)
        };

        PnLSnapshot {
            ts: Utc::now(),
            realized,
            unrealized,
            max_drawdown_pct: drawdown,
            daily_pnl: equity,
        }
    }

    /// Legacy snapshot without price info (unrealized = 0)
    pub fn snapshot(&self) -> PnLSnapshot {
        self.snapshot_with_prices(&HashMap::new())
    }
}

fn weighted_avg(current_avg: f64, current_qty: f64, px: f64, qty: f64) -> f64 {
    let total = current_qty + qty;
    if total <= 0.0 {
        return 0.0;
    }
    (current_avg * current_qty + px * qty) / total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pnl_snapshot_is_stable() {
        let book = PortfolioBook::default();
        let snap = book.snapshot();
        assert!(snap.realized.abs() < 1e-9);
    }
}
