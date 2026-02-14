use std::collections::HashMap;

use chrono::Utc;
use core_types::{BookTop, FillEvent, OrderAck, OrderSide, QuoteIntent};
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct ShadowOrder {
    pub order_id: String,
    pub intent: QuoteIntent,
}

#[derive(Default)]
pub struct ShadowExecutor {
    orders: RwLock<HashMap<String, ShadowOrder>>,
}

impl ShadowExecutor {
    pub fn register_order(&self, ack: &OrderAck, intent: QuoteIntent) {
        self.orders.write().insert(
            ack.order_id.clone(),
            ShadowOrder {
                order_id: ack.order_id.clone(),
                intent,
            },
        );
    }

    pub fn cancel(&self, order_id: &str) {
        self.orders.write().remove(order_id);
    }

    pub fn on_book(&self, book: &BookTop) -> Vec<FillEvent> {
        let mut fills = Vec::new();
        let mut to_remove = Vec::new();

        {
            let orders = self.orders.read();
            for (id, order) in orders.iter() {
                if order.intent.market_id != book.market_id {
                    continue;
                }

                let maybe_fill_price = match order.intent.side {
                    OrderSide::BuyYes if order.intent.price >= book.ask_yes => Some(book.ask_yes),
                    OrderSide::SellYes if order.intent.price <= book.bid_yes => Some(book.bid_yes),
                    OrderSide::BuyNo if order.intent.price >= book.ask_no => Some(book.ask_no),
                    OrderSide::SellNo if order.intent.price <= book.bid_no => Some(book.bid_no),
                    _ => None,
                };

                if let Some(px) = maybe_fill_price {
                    fills.push(FillEvent {
                        order_id: id.clone(),
                        market_id: order.intent.market_id.clone(),
                        side: order.intent.side.clone(),
                        price: px,
                        size: order.intent.size,
                        fee: 0.0,
                        ts_ms: Utc::now().timestamp_millis(),
                    });
                    to_remove.push(id.clone());
                }
            }
        }

        if !to_remove.is_empty() {
            let mut orders = self.orders.write();
            for id in to_remove {
                orders.remove(&id);
            }
        }

        fills
    }

    pub fn open_orders(&self) -> usize {
        self.orders.read().len()
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
    }
}
