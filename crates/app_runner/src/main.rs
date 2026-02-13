use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use core_types::{
    ControlCommand, EngineEvent, ExecutionVenue, FairValueModel, InventoryState, MarketFeed,
    QuotePolicy, RefPriceFeed, RiskContext, RiskManager,
};
use execution_clob::{ClobExecution, ExecutionMode};
use fair_value::SimpleFairValue;
use feed_polymarket::PolymarketFeed;
use feed_reference::MultiSourceRefFeed;
use futures::StreamExt;
use infra_bus::RingBus;
use observability::{init_metrics, init_tracing};
use paper_executor::ShadowExecutor;
use portfolio::PortfolioBook;
use risk_engine::{DefaultRiskManager, RiskLimits};
use serde::Serialize;
use strategy_maker::{MakerConfig, MakerQuotePolicy};
use tokio::sync::RwLock;

#[derive(Clone)]
struct AppState {
    paused: Arc<RwLock<bool>>,
    bus: RingBus<EngineEvent>,
    portfolio: Arc<PortfolioBook>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
}

#[derive(Serialize)]
struct HealthResp {
    status: &'static str,
    paused: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("app_runner");
    let prometheus = init_metrics();

    let bus = RingBus::new(16_384);
    let portfolio = Arc::new(PortfolioBook::default());
    let execution = Arc::new(ClobExecution::new(
        ExecutionMode::Paper,
        "https://clob.polymarket.com".to_string(),
    ));
    let shadow = Arc::new(ShadowExecutor::default());

    let state = AppState {
        paused: Arc::new(RwLock::new(false)),
        bus: bus.clone(),
        portfolio: portfolio.clone(),
        execution: execution.clone(),
        shadow: shadow.clone(),
        prometheus,
    };

    spawn_reference_feed(bus.clone());
    spawn_market_feed(bus.clone());
    spawn_strategy_engine(bus.clone(), portfolio, execution, shadow);

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/state/positions", get(positions))
        .route("/state/pnl", get(pnl))
        .route("/control/pause", post(pause))
        .route("/control/resume", post(resume))
        .route("/control/flatten", post(flatten))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    tracing::info!(%addr, "control api started");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

fn spawn_reference_feed(bus: RingBus<EngineEvent>) {
    tokio::spawn(async move {
        let feed = MultiSourceRefFeed::new(Duration::from_millis(500));
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
        ];
        let Ok(mut stream) = feed.stream_ticks(symbols).await else {
            tracing::error!("reference feed failed to start");
            return;
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(tick) => {
                    let _ = bus.publish(EngineEvent::RefTick(tick));
                }
                Err(err) => {
                    tracing::warn!(?err, "reference feed event error");
                }
            }
        }
    });
}

fn spawn_market_feed(bus: RingBus<EngineEvent>) {
    tokio::spawn(async move {
        let feed = PolymarketFeed::new(Duration::from_millis(700));
        let Ok(mut stream) = feed.stream_books().await else {
            tracing::error!("market feed failed to start");
            return;
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(book) => {
                    let _ = bus.publish(EngineEvent::BookTop(book));
                }
                Err(err) => {
                    tracing::warn!(?err, "market feed event error");
                }
            }
        }
    });
}

fn spawn_strategy_engine(
    bus: RingBus<EngineEvent>,
    portfolio: Arc<PortfolioBook>,
    execution: Arc<ClobExecution>,
    shadow: Arc<ShadowExecutor>,
) {
    tokio::spawn(async move {
        let model = SimpleFairValue::default();
        let policy = MakerQuotePolicy::new(MakerConfig::default());
        let risk = DefaultRiskManager::new(RiskLimits::default());

        let mut rx = bus.subscribe();
        let mut latest_tick: Option<core_types::RefTick> = None;
        let mut paused = false;

        loop {
            let Ok(event) = rx.recv().await else {
                continue;
            };

            match event {
                EngineEvent::RefTick(tick) => latest_tick = Some(tick),
                EngineEvent::Control(ControlCommand::Pause) => paused = true,
                EngineEvent::Control(ControlCommand::Resume) => paused = false,
                EngineEvent::Control(ControlCommand::Flatten) => {
                    if let Err(err) = execution.flatten_all().await {
                        tracing::warn!(?err, "flatten_all failed");
                    }
                }
                EngineEvent::BookTop(book) => {
                    let fills = shadow.on_book(&book);
                    for fill in fills {
                        portfolio.apply_fill(&fill);
                        let _ = bus.publish(EngineEvent::Fill(fill));
                    }

                    let Some(tick) = latest_tick.clone() else {
                        continue;
                    };
                    if paused {
                        continue;
                    }

                    let signal = model.evaluate(&tick, &book);
                    let _ = bus.publish(EngineEvent::Signal(signal.clone()));

                    let inv = portfolio
                        .positions()
                        .get(&book.market_id)
                        .map(|p| InventoryState {
                            market_id: p.market_id.clone(),
                            net_yes: p.yes,
                            net_no: p.no,
                            exposure_notional: p.yes.abs() + p.no.abs(),
                        })
                        .unwrap_or(InventoryState {
                            market_id: book.market_id.clone(),
                            net_yes: 0.0,
                            net_no: 0.0,
                            exposure_notional: 0.0,
                        });

                    let intents = policy.build_quotes(&signal, &inv);
                    for intent in intents {
                        let snap = portfolio.snapshot();
                        let decision = risk.evaluate(&RiskContext {
                            market_id: intent.market_id.clone(),
                            symbol: tick.symbol.clone(),
                            order_count: execution.open_orders_count(),
                            proposed_size: intent.size,
                            market_notional: inv.exposure_notional,
                            asset_notional: inv.exposure_notional,
                            drawdown_pct: snap.max_drawdown_pct,
                        });

                        if !decision.allow {
                            tracing::debug!(reason = %decision.reason, "risk blocked quote");
                            continue;
                        }

                        let mut adjusted = intent.clone();
                        adjusted.size = adjusted.size.min(decision.capped_size.max(0.0));
                        if adjusted.size <= 0.0 {
                            continue;
                        }

                        let _ = bus.publish(EngineEvent::QuoteIntent(adjusted.clone()));

                        match execution.place_order(adjusted.clone()).await {
                            Ok(ack) => {
                                shadow.register_order(&ack, adjusted);
                                let _ = bus.publish(EngineEvent::OrderAck(ack));
                            }
                            Err(err) => {
                                tracing::warn!(?err, "place order failed");
                            }
                        }
                    }

                    let _ = bus.publish(EngineEvent::Pnl(portfolio.snapshot()));
                }
                _ => {}
            }
        }
    });
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let paused = *state.paused.read().await;
    Json(HealthResp {
        status: "ok",
        paused,
    })
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    state.prometheus.render()
}

async fn positions(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.portfolio.positions())
}

async fn pnl(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.portfolio.snapshot())
}

async fn pause(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = true;
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Pause));
    Json(serde_json::json!({"ok": true, "paused": true}))
}

async fn resume(State(state): State<AppState>) -> impl IntoResponse {
    *state.paused.write().await = false;
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Resume));
    Json(serde_json::json!({"ok": true, "paused": false}))
}

async fn flatten(State(state): State<AppState>) -> impl IntoResponse {
    let _ = state.execution.flatten_all().await;
    let _ = state
        .bus
        .publish(EngineEvent::Control(ControlCommand::Flatten));
    Json(serde_json::json!({"ok": true, "flattened": true}))
}
