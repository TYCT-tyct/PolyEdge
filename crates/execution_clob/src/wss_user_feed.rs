use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// -----------------------------------------------------------------------
// 公开类型
// -----------------------------------------------------------------------

/// 从 WSS user channel 解析出的 fill 事件（仅保留 exit lifecycle 关心的字段）
#[derive(Debug, Clone)]
pub struct WssFillEvent {
    /// 订单 ID（对应 OrderAckV2.order_id）
    pub order_id: String,
    /// 市场 ID
    pub market_id: String,
    /// 成交价格
    pub price: f64,
    /// 成交数量
    pub size: f64,
    /// 事件类型: "trade" | "order"
    pub event_type: &'static str,
    /// 服务端时间戳 ms
    pub ts_ms: i64,
}

// -----------------------------------------------------------------------
// 内部 JSON 结构（Polymarket WSS user channel 格式）
// -----------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WssEnvelope {
    event_type: Option<String>,
    #[serde(default)]
    data: Vec<WssEventData>,
}

#[derive(Debug, Deserialize)]
struct WssEventData {
    #[serde(default)]
    order_id: String,
    #[serde(default)]
    market: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    size: String,
    #[serde(default)]
    timestamp: String,
    // order 事件字段
    #[serde(default)]
    id: String,
    #[serde(default)]
    market_id: String,
}

// -----------------------------------------------------------------------
// 公开入口：app_runner 可用自己的 broadcast::Sender 启动 WSS 循环
// -----------------------------------------------------------------------

/// 启动 WSS user channel 循环（自动重连，永不退出）
///
/// `tx`: 广播发送端（app_runner 持有 Arc<Sender>，此处共享）
/// `wss_url`: Polymarket user channel URL
/// `api_key`: CLOB API key（用于 subscribe 消息认证）
pub async fn run_wss_loop_with_sender(
    tx: Arc<broadcast::Sender<WssFillEvent>>,
    wss_url: String,
    api_key: String,
) {
    let mut backoff_ms = 500_u64;
    loop {
        match connect_and_stream(&tx, &wss_url, &api_key).await {
            Ok(()) => {
                tracing::info!("wss_user_feed: connection closed, reconnecting");
                backoff_ms = 500;
            }
            Err(err) => {
                tracing::warn!(
                    ?err,
                    backoff_ms,
                    "wss_user_feed: connection error, retrying"
                );
                backoff_ms = (backoff_ms * 2).min(30_000);
            }
        }
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
}



async fn connect_and_stream(
    tx: &Arc<broadcast::Sender<WssFillEvent>>,
    wss_url: &str,
    api_key: &str,
) -> Result<()> {
    let (mut ws, _) = connect_async(wss_url).await?;
    tracing::info!(wss_url, "wss_user_feed: connected");

    // 发送 subscribe 消息（Polymarket user channel 认证格式）
    let subscribe_json = serde_json::json!({
        "auth": { "apiKey": api_key },
        "type": "subscribe",
        "channel": "user",
    })
    .to_string();
    ws.send(Message::Text(subscribe_json.into())).await?;

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        match msg {
            Message::Text(text) => {
                if let Ok(envelope) = serde_json::from_str::<WssEnvelope>(&text) {
                    parse_and_broadcast(tx, envelope);
                }
            }
            // tungstenite 0.24+ 自动响应 Ping，无需手动处理
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}



fn parse_and_broadcast(tx: &Arc<broadcast::Sender<WssFillEvent>>, envelope: WssEnvelope) {
    let event_type = match envelope.event_type.as_deref() {
        Some("trade") => "trade",
        Some("order") => "order",
        _ => return, // 忽略非 fill 事件（heartbeat 等）
    };

    for data in envelope.data {
        let order_id = if !data.order_id.is_empty() {
            data.order_id.clone()
        } else {
            data.id.clone()
        };
        if order_id.is_empty() {
            continue;
        }

        let market_id = if !data.market.is_empty() {
            data.market.clone()
        } else {
            data.market_id.clone()
        };

        let Ok(price) = data.price.parse::<f64>() else {
            record_parse_error("price");
            continue;
        };
        let Ok(size) = data.size.parse::<f64>() else {
            record_parse_error("size");
            continue;
        };
        let ts_ms = data
            .timestamp
            .parse::<i64>()
            .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());

        let event = WssFillEvent {
            order_id,
            market_id,
            price,
            size,
            event_type,
            ts_ms,
        };

        // 忽略 lagged receiver 错误（消费者太慢时丢弃旧事件）
        let _ = tx.send(event);
    }
}

static PARSE_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);

fn record_parse_error(field: &'static str) {
    let count = PARSE_ERROR_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if count.is_power_of_two() {
        tracing::warn!(
            field,
            count,
            "wss_user_feed: dropping malformed fill event field"
        );
    }
}
