#[derive(Debug, Clone)]
pub struct TrackerSnapshot {
    pub received: u64,
    pub gap_count: u64,
    pub lost_frames: u64,
    pub out_of_order_count: u64,
    pub duplicate_count: u64,
    pub mean_latency_ms: f64,
    pub last_seq: Option<u64>,
    pub last_recv_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct StreamTracker {
    received: u64,
    gap_count: u64,
    lost_frames: u64,
    out_of_order_count: u64,
    duplicate_count: u64,
    last_seq: Option<u64>,
    latency_sum_ms: f64,
    last_recv_ms: Option<i64>,
}

impl StreamTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, seq: u64, send_ts_ms: i64, recv_ts_ms: i64) {
        self.received = self.received.saturating_add(1);

        if recv_ts_ms >= send_ts_ms {
            self.latency_sum_ms += (recv_ts_ms - send_ts_ms) as f64;
        }
        self.last_recv_ms = Some(recv_ts_ms);

        if let Some(prev) = self.last_seq {
            if seq == prev {
                self.duplicate_count = self.duplicate_count.saturating_add(1);
            } else if seq < prev {
                self.out_of_order_count = self.out_of_order_count.saturating_add(1);
            } else if seq > prev.saturating_add(1) {
                let lost = seq.saturating_sub(prev.saturating_add(1));
                self.gap_count = self.gap_count.saturating_add(1);
                self.lost_frames = self.lost_frames.saturating_add(lost);
            }
        }
        self.last_seq = Some(seq);
    }

    pub fn snapshot(&self) -> TrackerSnapshot {
        let mean_latency_ms = if self.received > 0 {
            self.latency_sum_ms / self.received as f64
        } else {
            0.0
        };
        TrackerSnapshot {
            received: self.received,
            gap_count: self.gap_count,
            lost_frames: self.lost_frames,
            out_of_order_count: self.out_of_order_count,
            duplicate_count: self.duplicate_count,
            mean_latency_ms,
            last_seq: self.last_seq,
            last_recv_ms: self.last_recv_ms,
        }
    }
}
