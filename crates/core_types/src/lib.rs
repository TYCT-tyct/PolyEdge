use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RefTick {
    pub source: String,
    pub symbol: String,
    pub event_ts_ms: i64,
    pub recv_ts_ms: i64,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookTop {
    pub market_id: String,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub bid_yes: f64,
    pub ask_yes: f64,
    pub bid_no: f64,
    pub ask_no: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Signal {
    pub market_id: String,
    pub fair_yes: f64,
    pub edge_bps_bid: f64,
    pub edge_bps_ask: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderSide {
    BuyYes,
    SellYes,
    BuyNo,
    SellNo,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::BuyYes => "buy_yes",
            Self::SellYes => "sell_yes",
            Self::BuyNo => "buy_no",
            Self::SellNo => "sell_no",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuoteIntent {
    pub market_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderAck {
    pub order_id: String,
    pub market_id: String,
    pub accepted: bool,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FillEvent {
    pub order_id: String,
    pub market_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskDecision {
    pub allow: bool,
    pub reason: String,
    pub capped_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PnLSnapshot {
    pub ts: DateTime<Utc>,
    pub realized: f64,
    pub unrealized: f64,
    pub max_drawdown_pct: f64,
    pub daily_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InventoryState {
    pub market_id: String,
    pub net_yes: f64,
    pub net_no: f64,
    pub exposure_notional: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskContext {
    pub market_id: String,
    pub symbol: String,
    pub order_count: usize,
    pub proposed_size: f64,
    pub market_notional: f64,
    pub asset_notional: f64,
    pub drawdown_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ControlCommand {
    Pause,
    Resume,
    Flatten,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EngineEvent {
    RefTick(RefTick),
    BookTop(BookTop),
    Signal(Signal),
    QuoteIntent(QuoteIntent),
    OrderAck(OrderAck),
    Fill(FillEvent),
    Pnl(PnLSnapshot),
    Control(ControlCommand),
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("feed disconnected: {0}")]
    FeedDisconnected(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

pub type DynStream<T> = BoxStream<'static, Result<T>>;

#[async_trait]
pub trait MarketFeed: Send + Sync {
    async fn stream_books(&self) -> Result<DynStream<BookTop>>;
}

#[async_trait]
pub trait RefPriceFeed: Send + Sync {
    async fn stream_ticks(&self, symbols: Vec<String>) -> Result<DynStream<RefTick>>;
}

pub trait FairValueModel: Send + Sync {
    fn evaluate(&self, tick: &RefTick, book: &BookTop) -> Signal;
}

pub trait QuotePolicy: Send + Sync {
    fn build_quotes(&self, signal: &Signal, inventory: &InventoryState) -> Vec<QuoteIntent>;
}

#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    async fn place_order(&self, intent: QuoteIntent) -> Result<OrderAck>;
    async fn cancel_order(&self, order_id: &str, market_id: &str) -> Result<()>;
    async fn flatten_all(&self) -> Result<()>;
}

pub trait RiskManager: Send + Sync {
    fn evaluate(&self, ctx: &RiskContext) -> RiskDecision;
}

#[async_trait]
pub trait ReplaySource: Send + Sync {
    async fn next_event(&mut self) -> Result<Option<EngineEvent>>;
}

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_side_display() {
        assert_eq!(OrderSide::BuyYes.to_string(), "buy_yes");
        assert_eq!(OrderSide::SellNo.to_string(), "sell_no");
    }

    #[test]
    fn engine_event_json_roundtrip() {
        let tick = EngineEvent::RefTick(RefTick {
            source: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            event_ts_ms: 1,
            recv_ts_ms: 2,
            price: 50000.0,
        });

        let raw = serde_json::to_string(&tick).expect("serialize");
        let parsed: EngineEvent = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(parsed, tick);
    }
}
