use serde::{Deserialize, Serialize};

/* -----------------------------
 * 模块：东京跨区传输 schema
 * 职责：定义东京 relay 发往爱尔兰 recorder 的稳定 UDP 负载
 * 边界：只放 wire 对象，不放运行时逻辑
 * ----------------------------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokyoBinanceWire {
    pub ts_tokyo_recv_ms: i64,
    pub ts_tokyo_send_ms: i64,
    pub ts_exchange_ms: i64,
    pub symbol: String,
    pub binance_price: f64,
}
