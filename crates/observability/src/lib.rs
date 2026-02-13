use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

static PROM_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_tracing(service_name: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{service_name}=info,info")));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .try_init();
}

pub fn init_metrics() -> PrometheusHandle {
    if let Some(handle) = PROM_HANDLE.get() {
        return handle.clone();
    }

    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("install prometheus recorder");

    let _ = PROM_HANDLE.set(handle.clone());
    handle
}
