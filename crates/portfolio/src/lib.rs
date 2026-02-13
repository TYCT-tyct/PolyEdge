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
                let closed = fill.size.min(pos.yes);
                *self.realized.write() += (fill.price - pos.avg_yes) * closed - fill.fee;
                pos.yes -= fill.size;
            }
            OrderSide::BuyNo => {
                pos.avg_no = weighted_avg(pos.avg_no, pos.no, fill.price, fill.size);
                pos.no += fill.size;
            }
            OrderSide::SellNo => {
                let closed = fill.size.min(pos.no);
                *self.realized.write() += (fill.price - pos.avg_no) * closed - fill.fee;
                pos.no -= fill.size;
            }
        }
    }

    pub fn positions(&self) -> HashMap<String, Position> {
        self.positions.read().clone()
    }

    pub fn snapshot(&self) -> PnLSnapshot {
        let realized = *self.realized.read();
        let unrealized = 0.0;
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
