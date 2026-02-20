use std::future::Future;

use anyhow::Result;
use core_types::EngineEvent;
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
mod paper_runtime;
mod paper_sqlite;
mod report_io;
mod seat_persist;
mod seat_runtime;
mod seat_types;
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
        fut.await;
        if report_normal_exit {
            tracing::warn!(task = task_name, "detached task exited");
        }
    });
}

pub(crate) use engine_loop::{
    spawn_predator_exit_lifecycle, spawn_shadow_outcome_task, spawn_strategy_engine,
    PredatorExecResult,
};

#[cfg(test)]
mod tests;
