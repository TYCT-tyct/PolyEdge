use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use core_types::{new_id, ExecutionVenue, OrderAck, QuoteIntent};
use parking_lot::RwLock;
use reqwest::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Paper,
    Live,
}

#[derive(Clone)]
pub struct ClobExecution {
    mode: ExecutionMode,
    http: Client,
    clob_endpoint: String,
    open_orders: Arc<RwLock<HashMap<String, QuoteIntent>>>,
}

impl ClobExecution {
    pub fn new(mode: ExecutionMode, clob_endpoint: String) -> Self {
        Self {
            mode,
            http: Client::new(),
            clob_endpoint,
            open_orders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn open_orders_count(&self) -> usize {
        self.open_orders.read().len()
    }
}

#[async_trait]
impl ExecutionVenue for ClobExecution {
    async fn place_order(&self, intent: QuoteIntent) -> Result<OrderAck> {
        match self.mode {
            ExecutionMode::Paper => {
                let order_id = new_id();
                self.open_orders
                    .write()
                    .insert(order_id.clone(), intent.clone());
                Ok(OrderAck {
                    order_id,
                    market_id: intent.market_id,
                    accepted: true,
                    ts_ms: Utc::now().timestamp_millis(),
                })
            }
            ExecutionMode::Live => {
                let payload = serde_json::json!({
                    "market_id": intent.market_id,
                    "side": intent.side.to_string(),
                    "price": intent.price,
                    "size": intent.size,
                    "ttl_ms": intent.ttl_ms,
                });

                let res = self
                    .http
                    .post(format!("{}/orders", self.clob_endpoint))
                    .json(&payload)
                    .send()
                    .await?;

                if !res.status().is_success() {
                    bail!("live order rejected: {}", res.status());
                }

                let order_id = new_id();
                Ok(OrderAck {
                    order_id,
                    market_id: intent.market_id,
                    accepted: true,
                    ts_ms: Utc::now().timestamp_millis(),
                })
            }
        }
    }

    async fn cancel_order(&self, order_id: &str, _market_id: &str) -> Result<()> {
        match self.mode {
            ExecutionMode::Paper => {
                self.open_orders.write().remove(order_id);
                Ok(())
            }
            ExecutionMode::Live => {
                let res = self
                    .http
                    .delete(format!("{}/orders/{order_id}", self.clob_endpoint))
                    .send()
                    .await?;
                if !res.status().is_success() {
                    bail!("cancel failed with status {}", res.status());
                }
                Ok(())
            }
        }
    }

    async fn flatten_all(&self) -> Result<()> {
        self.open_orders.write().clear();
        Ok(())
    }
}
