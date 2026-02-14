use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use core_types::{EngineEvent, ReplaySource};
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRecord {
    pub ts_ms: i64,
    pub event: EngineEvent,
}

pub struct JsonlRecorder {
    file: File,
}

impl JsonlRecorder {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context("create recorder dir")?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .context("open recorder file")?;
        Ok(Self { file })
    }

    pub async fn write(&mut self, record: &ReplayRecord) -> Result<()> {
        let line = serde_json::to_string(record).context("encode record")?;
        self.file.write_all(line.as_bytes()).await?;
        self.file.write_all(b"\n").await?;
        Ok(())
    }
}

pub struct JsonlReplaySource {
    lines: Lines<BufReader<File>>,
    speed: f64,
    prev_ts_ms: Option<i64>,
}

impl JsonlReplaySource {
    pub async fn open(path: impl AsRef<Path>, speed: f64) -> Result<Self> {
        let file = File::open(path).await.context("open replay file")?;
        let reader = BufReader::new(file);
        Ok(Self {
            lines: reader.lines(),
            speed: speed.max(0.001),
            prev_ts_ms: None,
        })
    }
}

#[async_trait]
impl ReplaySource for JsonlReplaySource {
    async fn next_event(&mut self) -> Result<Option<EngineEvent>> {
        let Some(line) = self.lines.next_line().await? else {
            return Ok(None);
        };
        let record: ReplayRecord = serde_json::from_str(&line).context("decode record")?;

        if let Some(prev) = self.prev_ts_ms {
            let delta_ms = (record.ts_ms - prev).max(0) as f64;
            let sleep_ms = (delta_ms / self.speed).round() as u64;
            if sleep_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
        }
        self.prev_ts_ms = Some(record.ts_ms);

        Ok(Some(record.event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{EngineEvent, RefTick};

    #[tokio::test]
    async fn record_roundtrip() {
        let path = "datasets/raw/test_replay.jsonl";
        let mut recorder = JsonlRecorder::open(path).await.expect("open recorder");
        recorder
            .write(&ReplayRecord {
                ts_ms: 1,
                event: EngineEvent::RefTick(RefTick {
                    source: "x".to_string(),
                    symbol: "BTCUSDT".to_string(),
                    event_ts_ms: 1,
                    recv_ts_ms: 1,
                    event_ts_exchange_ms: 1,
                    recv_ts_local_ns: 1_000_000,
                    price: 1.0,
                }),
            })
            .await
            .expect("write");

        let mut source = JsonlReplaySource::open(path, 10.0)
            .await
            .expect("open replay");
        let e = source.next_event().await.expect("next");
        assert!(matches!(e, Some(EngineEvent::RefTick(_))));
    }
}
