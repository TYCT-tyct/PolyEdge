use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::common::ts_to_date_hour;
use crate::models::{IngestLogRow, PersistEvent};

pub fn persist_event(root: &Path, ev: PersistEvent) -> Result<()> {
    match ev {
        PersistEvent::Snapshot(row) => {
            let (date, hour) = ts_to_date_hour(row.ts_ireland_sample_ms)?;
            let dir = root
                .join("snapshot_100ms")
                .join(format!("dt={date}"))
                .join(format!("symbol={}", row.symbol))
                .join(format!("tf={}", row.timeframe));
            let path = dir.join(format!("hour={hour:02}.jsonl"));
            append_jsonl(&path, row.as_ref())?;
        }
        PersistEvent::Round(row) => {
            let (date, _hour) = ts_to_date_hour(row.ts_recorded_ms)?;
            let dir = root
                .join("rounds")
                .join(format!("dt={date}"))
                .join(format!("symbol={}", row.symbol))
                .join(format!("tf={}", row.timeframe));
            let path = dir.join("rounds.jsonl");
            append_jsonl(&path, &row)?;
        }
        PersistEvent::Log(row) => {
            let (date, hour) = ts_to_date_hour(row.ts_ms)?;
            let dir = root
                .join("ingest_log")
                .join(format!("dt={date}"))
                .join("app=ireland-recorder");
            let path = dir.join(format!("hour={hour:02}.jsonl"));
            append_jsonl(&path, &row)?;
        }
    }
    Ok(())
}

pub fn append_jsonl<T: Serialize>(path: &Path, row: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    let line = serde_json::to_string(row)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

pub fn log_ingest(
    tx: &mpsc::UnboundedSender<PersistEvent>,
    level: &str,
    component: &str,
    message: &str,
) {
    let _ = tx.send(PersistEvent::Log(IngestLogRow {
        ts_ms: Utc::now().timestamp_millis(),
        level: level.to_string(),
        component: component.to_string(),
        message: message.to_string(),
    }));
}
