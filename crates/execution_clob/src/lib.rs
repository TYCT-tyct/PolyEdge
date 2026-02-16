use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use core_types::{new_id, ExecutionVenue, OrderAck, OrderAckV2, OrderIntentV2, QuoteIntent};
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
    open_orders: Arc<RwLock<HashMap<String, PaperOpenOrder>>>,
}

#[derive(Debug, Clone)]
struct PaperOpenOrder {
    intent: QuoteIntent,
    created_at: Instant,
}

impl ClobExecution {
    pub fn new(mode: ExecutionMode, clob_endpoint: String) -> Self {
        Self::new_with_timeout(mode, clob_endpoint, std::time::Duration::from_millis(3_000))
    }

    pub fn new_with_timeout(
        mode: ExecutionMode,
        clob_endpoint: String,
        timeout: std::time::Duration,
    ) -> Self {
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            mode,
            http,
            clob_endpoint,
            open_orders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn open_orders_count(&self) -> usize {
        self.prune_expired_orders();
        self.open_orders.read().len()
    }

    pub fn is_live(&self) -> bool {
        matches!(self.mode, ExecutionMode::Live)
    }

    pub fn open_order_notional_for_market(&self, market_id: &str) -> f64 {
        self.prune_expired_orders();
        self.open_orders
            .read()
            .values()
            .filter(|o| o.intent.market_id == market_id)
            .map(|o| o.intent.size * o.intent.price.abs())
            .sum()
    }

    pub fn open_order_notional_total(&self) -> f64 {
        self.prune_expired_orders();
        self.open_orders
            .read()
            .values()
            .map(|o| o.intent.size * o.intent.price.abs())
            .sum()
    }

    fn prune_expired_orders(&self) {
        let mut orders = self.open_orders.write();
        let now = Instant::now();
        orders.retain(|_, o| {
            let ttl = std::time::Duration::from_millis(o.intent.ttl_ms.max(1));
            now.duration_since(o.created_at) < ttl
        });
    }
}

#[async_trait]
impl ExecutionVenue for ClobExecution {
    async fn place_order(&self, intent: QuoteIntent) -> Result<OrderAck> {
        let ack_v2 = self.place_order_v2(OrderIntentV2::from(intent)).await?;
        Ok(OrderAck {
            order_id: ack_v2.order_id,
            market_id: ack_v2.market_id,
            accepted: ack_v2.accepted,
            ts_ms: ack_v2.ts_ms,
        })
    }

    async fn place_order_v2(&self, intent: OrderIntentV2) -> Result<OrderAckV2> {
        let started = Instant::now();
        match self.mode {
            ExecutionMode::Paper => {
                self.prune_expired_orders();
                let order_id = new_id();
                let paper_intent = QuoteIntent {
                    market_id: intent.market_id.clone(),
                    side: intent.side.clone(),
                    price: intent.price,
                    size: intent.size,
                    ttl_ms: intent.ttl_ms,
                };
                self.open_orders.write().insert(
                    order_id.clone(),
                    PaperOpenOrder {
                        intent: paper_intent,
                        created_at: Instant::now(),
                    },
                );
                Ok(OrderAckV2 {
                    order_id,
                    market_id: intent.market_id,
                    accepted: true,
                    accepted_size: intent.size,
                    reject_code: None,
                    exchange_latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                    ts_ms: Utc::now().timestamp_millis(),
                })
            }
            ExecutionMode::Live => {
                let payload = serde_json::json!({
                    "market_id": intent.market_id,
                    "token_id": intent.token_id,
                    "side": intent.side.to_string(),
                    "price": intent.price,
                    "size": intent.size,
                    "ttl_ms": intent.ttl_ms,
                    "style": intent.style.to_string(),
                    "tif": intent.tif.to_string(),
                    "client_order_id": intent.client_order_id,
                    "max_slippage_bps": intent.max_slippage_bps,
                    "fee_rate_bps": intent.fee_rate_bps,
                    "expected_edge_net_bps": intent.expected_edge_net_bps,
                    "hold_to_resolution": intent.hold_to_resolution,
                });

                let res = self
                    .http
                    .post(format!("{}/orders", self.clob_endpoint))
                    .json(&payload)
                    .send()
                    .await?;
                let status = res.status();
                let exchange_latency_ms = started.elapsed().as_secs_f64() * 1_000.0;

                if !status.is_success() {
                    return Ok(OrderAckV2 {
                        order_id: new_id(),
                        market_id: intent.market_id,
                        accepted: false,
                        accepted_size: 0.0,
                        reject_code: Some(format!("http_{}", status.as_u16())),
                        exchange_latency_ms,
                        ts_ms: Utc::now().timestamp_millis(),
                    });
                }

                let payload_value = res
                    .json::<serde_json::Value>()
                    .await
                    .unwrap_or_else(|_| serde_json::json!({}));
                let order_id = payload_value
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .or_else(|| payload_value.get("id").and_then(|v| v.as_str()))
                    .or_else(|| payload_value.get("orderID").and_then(|v| v.as_str()))
                    .map(ToString::to_string)
                    .unwrap_or_else(new_id);
                let accepted_size = payload_value
                    .get("accepted_size")
                    .and_then(|v| v.as_f64())
                    .or_else(|| payload_value.get("size").and_then(|v| v.as_f64()))
                    .unwrap_or(intent.size);
                let mut accepted = payload_value
                    .get("accepted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                let mut reject_code = payload_value
                    .get("reject_code")
                    .and_then(|v| v.as_str())
                    .or_else(|| payload_value.get("reason").and_then(|v| v.as_str()))
                    .or_else(|| payload_value.get("error").and_then(|v| v.as_str()))
                    .map(ToString::to_string);

                if accepted_size <= 0.0 {
                    accepted = false;
                    reject_code.get_or_insert_with(|| "zero_fill".to_string());
                }
                if accepted && matches!(intent.tif, core_types::OrderTimeInForce::Fok) {
                    let missing = intent.size - accepted_size;
                    if missing > 1e-9 {
                        accepted = false;
                        reject_code.get_or_insert_with(|| "fok_partial_fill".to_string());
                    }
                }

                Ok(OrderAckV2 {
                    order_id,
                    market_id: intent.market_id,
                    accepted,
                    accepted_size: accepted_size.max(0.0),
                    reject_code,
                    exchange_latency_ms,
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
        match self.mode {
            ExecutionMode::Paper => Ok(()),
            ExecutionMode::Live => {
                let base = self.clob_endpoint.trim_end_matches('/');
                let res = self.http.post(format!("{base}/flatten")).send().await?;
                if !res.status().is_success() {
                    bail!("flatten failed with status {}", res.status());
                }
                Ok(())
            }
        }
    }
}
