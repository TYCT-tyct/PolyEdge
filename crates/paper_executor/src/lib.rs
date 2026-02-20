use std::collections::HashMap;

use chrono::Utc;
use core_types::{BookTop, ExecutionStyle, FillEvent, OrderAck, OrderSide, QuoteIntent};
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct ShadowOrder {
    pub order_id: String,
    pub intent: QuoteIntent,
    pub style: ExecutionStyle,
    pub reference_mid: f64,
    pub fee_rate_bps: f64,
}

pub struct ShadowExecutor {
    orders: RwLock<HashMap<String, ShadowOrder>>,
}

impl Default for ShadowExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ShadowExecutor {
    pub fn new() -> Self {
        Self {
            orders: RwLock::new(HashMap::new()),
        }
    }
    pub fn register_order(
        &self,
        ack: &OrderAck,
        intent: QuoteIntent,
        style: ExecutionStyle,
        reference_mid: f64,
        fee_rate_bps: f64,
    ) {
        self.orders.write().insert(
            ack.order_id.clone(),
            ShadowOrder {
                order_id: ack.order_id.clone(),
                intent,
                style,
                reference_mid,
                fee_rate_bps,
            },
        );
    }

    pub fn cancel(&self, order_id: &str) {
        self.orders.write().remove(order_id);
    }

    pub fn on_book(&self, book: &BookTop) -> Vec<FillEvent> {
        let mut fills = Vec::new();

        // Use write lock for entire operation to prevent race condition
        // between read (matching) and write (removing filled orders)
        let mut orders = self.orders.write();
        let to_remove: Vec<String> = orders
            .iter()
            .filter(|(_, order)| order.intent.market_id == book.market_id)
            .filter_map(|(id, order)| {
                let maybe_fill_price = match order.intent.side {
                    OrderSide::BuyYes if order.intent.price >= book.ask_yes => Some(book.ask_yes),
                    OrderSide::SellYes if order.intent.price <= book.bid_yes => Some(book.bid_yes),
                    OrderSide::BuyNo if order.intent.price >= book.ask_no => Some(book.ask_no),
                    OrderSide::SellNo if order.intent.price <= book.bid_no => Some(book.bid_no),
                    _ => None,
                };
                maybe_fill_price.map(|px| {
                    let mid_price = mid_for_side(book, &order.intent.side);
                    let slippage_bps = if mid_price > 0.0 {
                        Some(((px - mid_price) / mid_price) * 10_000.0)
                    } else {
                        None
                    };
                    let executed_size_usdc = (px * order.intent.size).max(0.0);
                    let fee = if order.fee_rate_bps.is_finite() && order.fee_rate_bps != 0.0 {
                        executed_size_usdc * (order.fee_rate_bps / 10_000.0)
                    } else {
                        match order.style {
                            ExecutionStyle::Maker => 0.0,
                            ExecutionStyle::Taker | ExecutionStyle::Arb => {
                                let taker_fee_rate = if !(0.02..=0.98).contains(&px) {
                                    0.001
                                } else {
                                    0.01
                                };
                                executed_size_usdc * taker_fee_rate
                            }
                        }
                    };
                    fills.push(FillEvent {
                        order_id: id.clone(),
                        market_id: order.intent.market_id.clone(),
                        side: order.intent.side.clone(),
                        style: order.style.clone(),
                        price: px,
                        size: order.intent.size,
                        fee,
                        mid_price: Some(mid_price.max(0.0)),
                        slippage_bps,
                        ts_ms: Utc::now().timestamp_millis(),
                    });
                    id.clone()
                })
            })
            .collect();

        let mut to_remove_intents = HashMap::new();

        // Remove filled orders within same write lock
        for id in to_remove {
            if let Some(order) = orders.remove(&id) {
                to_remove_intents.insert(id, order.intent);
            }
        }

        fills
    }

    pub fn open_orders(&self) -> usize {
        self.orders.read().len()
    }
}

fn mid_for_side(book: &BookTop, side: &OrderSide) -> f64 {
    match side {
        OrderSide::BuyYes | OrderSide::SellYes => (book.bid_yes + book.ask_yes) * 0.5,
        OrderSide::BuyNo | OrderSide::SellNo => (book.bid_no + book.ask_no) * 0.5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_crossing_buy_yes() {
        let shadow = ShadowExecutor::default();
        let ack = OrderAck {
            order_id: "o1".to_string(),
            market_id: "m1".to_string(),
            accepted: true,
            ts_ms: 1,
        };
        shadow.register_order(
            &ack,
            QuoteIntent {
                market_id: "m1".to_string(),
                side: OrderSide::BuyYes,
                price: 0.55,
                size: 1.0,
                ttl_ms: 1_000,
            },
            ExecutionStyle::Maker,
            0.52,
            -2.0,
        );
        let fills = shadow.on_book(&BookTop {
            market_id: "m1".to_string(),
            token_id_yes: "y".to_string(),
            token_id_no: "n".to_string(),
            bid_yes: 0.5,
            ask_yes: 0.54,
            bid_no: 0.46,
            ask_no: 0.5,
            ts_ms: 2,
            recv_ts_local_ns: 2_000_000,
        });
        assert_eq!(fills.len(), 1);
        assert!(fills[0].fee < 0.0);
    }

    #[test]
    fn taker_fee_curve_boundaries() {
        let shadow = ShadowExecutor::default();
        let book = BookTop {
            market_id: "m1".to_string(),
            token_id_yes: "y".to_string(),
            token_id_no: "n".to_string(),
            bid_yes: 0.50,
            ask_yes: 0.50,
            bid_no: 0.49,
            ask_no: 0.51,
            ts_ms: 2,
            recv_ts_local_ns: 2_000_000,
        };

        for (idx, px, expected_rate) in [
            (0, 0.0199, 0.001),
            (1, 0.02, 0.01),
            (2, 0.98, 0.01),
            (3, 0.9801, 0.001),
        ] {
            let order_id = format!("o{idx}");
            let ack = OrderAck {
                order_id: order_id.clone(),
                market_id: "m1".to_string(),
                accepted: true,
                ts_ms: 1,
            };
            let side = OrderSide::BuyYes;
            shadow.register_order(
                &ack,
                QuoteIntent {
                    market_id: "m1".to_string(),
                    side,
                    price: px,
                    size: 10.0,
                    ttl_ms: 1000,
                },
                ExecutionStyle::Taker,
                px,
                expected_rate * 10_000.0,
            );
            let mut book_local = book.clone();
            book_local.ask_yes = px;
            book_local.bid_yes = px;
            let fills = shadow.on_book(&book_local);
            assert_eq!(fills.len(), 1);
            let expected = px * 10.0 * expected_rate;
            assert!((fills[0].fee - expected).abs() < 1e-9);
        }
    }

    #[test]
    fn slippage_bps_sign_and_side_formula() {
        let shadow = ShadowExecutor::default();
        let book = BookTop {
            market_id: "m1".to_string(),
            token_id_yes: "y".to_string(),
            token_id_no: "n".to_string(),
            bid_yes: 0.48,
            ask_yes: 0.52,
            bid_no: 0.47,
            ask_no: 0.53,
            ts_ms: 2,
            recv_ts_local_ns: 2_000_000,
        };

        let cases = [
            ("by", OrderSide::BuyYes, 0.52, 400.0),
            ("sy", OrderSide::SellYes, 0.48, -400.0),
            ("bn", OrderSide::BuyNo, 0.53, 600.0),
            ("sn", OrderSide::SellNo, 0.47, -600.0),
        ];

        for (suffix, side, px, expected_slippage) in cases {
            let order_id = format!("o-{suffix}");
            let ack = OrderAck {
                order_id: order_id.clone(),
                market_id: "m1".to_string(),
                accepted: true,
                ts_ms: 1,
            };
            shadow.register_order(
                &ack,
                QuoteIntent {
                    market_id: "m1".to_string(),
                    side,
                    price: px,
                    size: 1.0,
                    ttl_ms: 1000,
                },
                ExecutionStyle::Taker,
                0.5,
                0.0,
            );
            let fills = shadow.on_book(&book);
            assert_eq!(fills.len(), 1);
            assert!((fills[0].slippage_bps.unwrap_or_default() - expected_slippage).abs() < 1e-9);
        }
    }
}
