use serde::{Deserialize, Serialize};

pub const HYPER_MESH_WIRE_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperMeshFrame {
    pub v: u8,
    pub stream_id: String,
    pub seq: u64,
    pub ts_exchange_ms: i64,
    pub ts_tokyo_recv_ms: i64,
    pub ts_tokyo_send_ms: i64,
    pub symbol: String,
    pub price: f64,
}

impl HyperMeshFrame {
    pub fn new(
        stream_id: impl Into<String>,
        seq: u64,
        ts_exchange_ms: i64,
        ts_tokyo_recv_ms: i64,
        ts_tokyo_send_ms: i64,
        symbol: impl Into<String>,
        price: f64,
    ) -> Self {
        Self {
            v: HYPER_MESH_WIRE_VERSION,
            stream_id: stream_id.into(),
            seq,
            ts_exchange_ms,
            ts_tokyo_recv_ms,
            ts_tokyo_send_ms,
            symbol: symbol.into(),
            price,
        }
    }
}
