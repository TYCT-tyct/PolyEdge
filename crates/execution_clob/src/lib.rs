use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use core_types::{new_id, ExecutionVenue, OrderAck, OrderAckV2, OrderIntentV2, QuoteIntent};
use parking_lot::{Mutex, RwLock};
use reqwest::Client;

pub mod wss_user_feed;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Paper,
    Live,
}

pub struct ClobExecution {
    mode: ExecutionMode,
    http: Client,
    clob_endpoint: String,
    order_primary_endpoint: String,
    order_backup_endpoint: Option<String>,
    order_failover_timeout: Duration,
    open_orders: Arc<RwLock<HashMap<String, PaperOpenOrder>>>,
    last_prune: Mutex<Instant>,
    ack_probe: Option<Arc<AckProbe>>,
}

impl Clone for ClobExecution {
    fn clone(&self) -> Self {
        Self {
            mode: self.mode,
            http: self.http.clone(),
            clob_endpoint: self.clob_endpoint.clone(),
            order_primary_endpoint: self.order_primary_endpoint.clone(),
            order_backup_endpoint: self.order_backup_endpoint.clone(),
            order_failover_timeout: self.order_failover_timeout,
            open_orders: self.open_orders.clone(),
            last_prune: Mutex::new(Instant::now()),
            ack_probe: self.ack_probe.clone(),
        }
    }
}

/// Minimum interval between order pruning to reduce lock contention
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct AckProbe {
    url: String,
    every: u64,
    counter: AtomicU64,
}

#[derive(Debug, Clone)]
struct PaperOpenOrder {
    intent: QuoteIntent,
    created_at: Instant,
}

impl ClobExecution {
    pub fn new(mode: ExecutionMode, clob_endpoint: String) -> Self {
        Self::new_with_order_routing(
            mode,
            clob_endpoint,
            None,
            None,
            std::time::Duration::from_millis(3_000),
            std::time::Duration::from_millis(200),
        )
    }

    pub fn new_with_timeout(
        mode: ExecutionMode,
        clob_endpoint: String,
        timeout: std::time::Duration,
    ) -> Self {
        Self::new_with_order_routing(
            mode,
            clob_endpoint,
            None,
            None,
            timeout,
            std::time::Duration::from_millis(200),
        )
    }

    pub fn new_with_order_routing(
        mode: ExecutionMode,
        clob_endpoint: String,
        order_primary_endpoint: Option<String>,
        order_backup_endpoint: Option<String>,
        timeout: std::time::Duration,
        order_failover_timeout: std::time::Duration,
    ) -> Self {
        let http = Client::builder()
            // Keep the request budget bounded (engine must never hang on IO).
            .timeout(timeout)
            // Connection pooling + keepalive to reduce RTT tail spikes.
            .pool_max_idle_per_host(
                std::env::var("POLYEDGE_HTTP_POOL_IDLE_PER_HOST")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(16)
                    .max(1),
            )
            .pool_idle_timeout(Some(Duration::from_secs(
                std::env::var("POLYEDGE_HTTP_POOL_IDLE_TIMEOUT_SEC")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(90)
                    .max(5),
            )))
            .tcp_keepalive(Some(Duration::from_secs(
                std::env::var("POLYEDGE_HTTP_TCP_KEEPALIVE_SEC")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(30)
                    .max(5),
            )))
            .tcp_nodelay(true)
            // Force HTTP/2 without negotiation (Polymarket CLOB supports it)
            .http2_prior_knowledge()
            // If the peer supports it (ALPN), this can cut head-of-line blocking.
            .http2_keep_alive_interval(Some(Duration::from_secs(
                std::env::var("POLYEDGE_HTTP2_KEEPALIVE_INTERVAL_SEC")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(30)
                    .max(5),
            )))
            .http2_keep_alive_timeout(Duration::from_secs(
                std::env::var("POLYEDGE_HTTP2_KEEPALIVE_TIMEOUT_SEC")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(10)
                    .max(1),
            ))
            .http2_keep_alive_while_idle(true)
            .build()
            .unwrap_or_else(|_| Client::new());
        let ack_probe = std::env::var("POLYEDGE_ACK_ONLY_PROBE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|url| {
                let every = std::env::var("POLYEDGE_ACK_ONLY_PROBE_EVERY")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(20)
                    .max(1);
                Arc::new(AckProbe {
                    url,
                    every,
                    counter: AtomicU64::new(0),
                })
            });

        let primary = order_primary_endpoint
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| clob_endpoint.trim().trim_end_matches('/').to_string());
        let backup = order_backup_endpoint
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty() && v != &primary);

        Self {
            mode,
            http,
            clob_endpoint,
            order_primary_endpoint: primary,
            order_backup_endpoint: backup,
            order_failover_timeout,
            open_orders: Arc::new(RwLock::new(HashMap::new())),
            last_prune: Mutex::new(Instant::now()),
            ack_probe,
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

    /// Best-effort warmup for the internal HTTP client pool. Intended to run on startup so the
    /// first order/ack path doesn't pay DNS+TLS handshake cost.
    pub async fn prewarm_urls(&self, urls: &[String]) {
        for url in urls {
            let _ = self.http.get(url).send().await;
        }
    }

    pub fn order_endpoints(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(2);
        out.push(self.order_primary_endpoint.clone());
        if let Some(backup) = &self.order_backup_endpoint {
            out.push(backup.clone());
        }
        out
    }

    /// Prune expired orders with lazy cleanup (only every 60 seconds)
    /// This reduces lock contention from O(n) per call to O(n) per minute
    fn prune_expired_orders(&self) {
        // Check if enough time has passed since last prune
        let should_prune = {
            let mut last = self.last_prune.lock();
            let now = Instant::now();
            if now.duration_since(*last) >= PRUNE_INTERVAL {
                *last = now;
                true
            } else {
                false
            }
        };

        if should_prune {
            let mut orders = self.open_orders.write();
            let now = Instant::now();
            orders.retain(|_, o| {
                let ttl = Duration::from_millis(o.intent.ttl_ms.max(1));
                now.duration_since(o.created_at) < ttl
            });
        }
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
                // In paper mode, there is no real exchange RTT. Optionally probe a configured URL
                // at a low sampling rate to estimate ack_only_ms without placing orders.
                let mut exchange_latency_ms = 0.0;
                if let Some(probe) = &self.ack_probe {
                    let n = probe
                        .counter
                        .fetch_add(1, Ordering::Relaxed)
                        .wrapping_add(1);
                    if n % probe.every == 0 {
                        let t0 = Instant::now();
                        let _ = self.http.get(&probe.url).send().await;
                        exchange_latency_ms = t0.elapsed().as_secs_f64() * 1_000.0;
                    }
                }
                Ok(OrderAckV2 {
                    order_id,
                    market_id: intent.market_id,
                    accepted: true,
                    accepted_size: intent.size,
                    reject_code: None,
                    // Note: default is 0.0 unless probing is enabled.
                    exchange_latency_ms,
                    ts_ms: Utc::now().timestamp_millis(),
                })
            }
            ExecutionMode::Live => {
                // Validate price is finite and within valid range before sending
                if !intent.price.is_finite() || intent.price <= 0.0 || intent.price >= 1.0 {
                    return Ok(OrderAckV2 {
                        order_id: new_id(),
                        market_id: intent.market_id,
                        accepted: false,
                        accepted_size: 0.0,
                        reject_code: Some("invalid_price".to_string()),
                        exchange_latency_ms: 0.0,
                        ts_ms: Utc::now().timestamp_millis(),
                    });
                }

                // -----------------------------------------------------------------------
                // B: 预序列化快路径 — 跳过 serde_json 序列化开销
                // 若调用方已预构建 JSON payload，直接用；否则走原始序列化路径。
                // -----------------------------------------------------------------------
                let body_bytes: Vec<u8> = if let Some(ref prebuilt) = intent.prebuilt_payload {
                    prebuilt.clone()
                } else {
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
                    serde_json::to_vec(&payload).unwrap_or_default()
                };

                const MAX_RETRIES: u32 = 2;
                let mut last_network_error: Option<String> = None;
                for (idx, endpoint) in self.order_endpoints().iter().enumerate() {
                    let primary_leg = idx == 0;
                    for attempt in 0..MAX_RETRIES {
                        let mut req = self
                            .http
                            .post(format!("{endpoint}/orders"))
                            .header("content-type", "application/json")
                            .body(body_bytes.clone());
                        if primary_leg && self.order_failover_timeout > Duration::from_millis(0) {
                            req = req.timeout(self.order_failover_timeout);
                        }
                        let res = match req.send().await {
                            Ok(res) => res,
                            Err(err) => {
                                last_network_error = Some(err.to_string());
                                if attempt + 1 < MAX_RETRIES {
                                    continue;
                                }
                                break;
                            }
                        };

                        let status = res.status();
                        let raw = res.text().await.unwrap_or_default();
                        let payload_value = serde_json::from_str::<serde_json::Value>(&raw)
                            .unwrap_or_else(|_| {
                                serde_json::json!({
                                    "raw": raw,
                                })
                            });
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
                            .unwrap_or_else(|| {
                                if status.is_success() {
                                    intent.size
                                } else {
                                    0.0
                                }
                            })
                            .max(0.0);
                        let mut accepted = payload_value
                            .get("accepted")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(status.is_success());
                        let mut reject_code = payload_value
                            .get("reject_code")
                            .and_then(|v| v.as_str())
                            .or_else(|| payload_value.get("reason").and_then(|v| v.as_str()))
                            .or_else(|| payload_value.get("error").and_then(|v| v.as_str()))
                            .map(ToString::to_string);

                        if !status.is_success() {
                            accepted = false;
                            if reject_code.is_none() {
                                reject_code = Some(format!("http_{}", status.as_u16()));
                            }
                        }
                        if accepted_size <= 0.0 {
                            accepted = false;
                        }
                        if status.is_server_error() && attempt + 1 < MAX_RETRIES {
                            continue;
                        }
                        if accepted {
                            return Ok(OrderAckV2 {
                                order_id,
                                market_id: intent.market_id,
                                accepted: true,
                                accepted_size,
                                reject_code: None,
                                exchange_latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                                ts_ms: Utc::now().timestamp_millis(),
                            });
                        }
                        if primary_leg {
                            break;
                        }
                        return Ok(OrderAckV2 {
                            order_id,
                            market_id: intent.market_id,
                            accepted: false,
                            accepted_size: 0.0,
                            reject_code,
                            exchange_latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                            ts_ms: Utc::now().timestamp_millis(),
                        });
                    }
                }

                let reject_code = last_network_error
                    .map(|e| format!("network_error:{e}"))
                    .unwrap_or_else(|| "network_error_after_failover".to_string());
                Ok(OrderAckV2 {
                    order_id: new_id(),
                    market_id: intent.market_id,
                    accepted: false,
                    accepted_size: 0.0,
                    reject_code: Some(reject_code),
                    exchange_latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
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
                let mut last_status: Option<reqwest::StatusCode> = None;
                for endpoint in self.order_endpoints() {
                    let res = self
                        .http
                        .delete(format!("{endpoint}/orders/{order_id}"))
                        .send()
                        .await?;
                    if res.status().is_success() {
                        return Ok(());
                    }
                    last_status = Some(res.status());
                }
                bail!(
                    "cancel failed with status {}",
                    last_status
                        .map(|s| s.as_u16().to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                )
            }
        }
    }

    async fn flatten_all(&self) -> Result<()> {
        self.open_orders.write().clear();
        match self.mode {
            ExecutionMode::Paper => Ok(()),
            ExecutionMode::Live => {
                let mut last_status: Option<reqwest::StatusCode> = None;
                for endpoint in self.order_endpoints() {
                    let res = self.http.post(format!("{endpoint}/flatten")).send().await?;
                    if res.status().is_success() {
                        return Ok(());
                    }
                    last_status = Some(res.status());
                }
                bail!(
                    "flatten failed with status {}",
                    last_status
                        .map(|s| s.as_u16().to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_endpoints_include_backup_when_configured() {
        let exec = ClobExecution::new_with_order_routing(
            ExecutionMode::Paper,
            "https://clob.polymarket.com".to_string(),
            Some("http://127.0.0.1:9001".to_string()),
            Some("http://127.0.0.1:9002".to_string()),
            Duration::from_millis(1_000),
            Duration::from_millis(200),
        );
        let endpoints = exec.order_endpoints();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0], "http://127.0.0.1:9001");
        assert_eq!(endpoints[1], "http://127.0.0.1:9002");
    }

    #[test]
    fn order_endpoints_dedup_empty_backup() {
        let exec = ClobExecution::new_with_order_routing(
            ExecutionMode::Paper,
            "https://clob.polymarket.com".to_string(),
            Some("http://127.0.0.1:9001".to_string()),
            Some("http://127.0.0.1:9001".to_string()),
            Duration::from_millis(1_000),
            Duration::from_millis(200),
        );
        let endpoints = exec.order_endpoints();
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0], "http://127.0.0.1:9001");
    }
}
