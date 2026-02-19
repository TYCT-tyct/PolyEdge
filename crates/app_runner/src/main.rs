use std::future::Future;
use std::panic::AssertUnwindSafe;

use anyhow::Result;
use core_types::EngineEvent;
use futures::FutureExt;
use infra_bus::RingBus;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod bootstrap;
mod config_loader;
mod control_api;
mod engine_core;
mod engine_loop;
mod execution_eval;
mod feed_runtime;
mod fusion_engine;
mod gate_eval;
mod orchestration;
mod report_io;
mod state;
mod stats_utils;
mod strategy_policy;
mod strategy_runtime;
mod toxicity_report;
mod toxicity_runtime;

use bootstrap::{async_main, install_rustls_provider};

fn main() -> Result<()> {
    install_rustls_provider();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}

#[inline]
fn publish_if_telemetry_subscribers(bus: &RingBus<EngineEvent>, event: EngineEvent) {
    if bus.receiver_count() > 1 {
        let _ = bus.publish(event);
    }
}

fn spawn_detached<F>(task_name: &'static str, report_normal_exit: bool, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(()) => {
                if report_normal_exit {
                    tracing::warn!(task = task_name, "detached task exited");
                }
            }
            Err(payload) => {
                let panic_msg = if let Some(msg) = payload.downcast_ref::<&str>() {
                    (*msg).to_string()
                } else if let Some(msg) = payload.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                tracing::error!(task = task_name, panic = %panic_msg, "detached task panicked");
                metrics::counter!("runtime.detached_task_panic").increment(1);
            }
        }
    });
}

pub(crate) use engine_loop::{
    estimate_time_to_expiry_ms, spawn_predator_exit_lifecycle, spawn_shadow_outcome_task,
    spawn_strategy_engine, PredatorExecResult,
};

#[cfg(test)]
mod tests;
