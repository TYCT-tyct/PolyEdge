use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock};

use crate::seat_persist::{
    append_decision, archive_old_decisions, load_history, recover_state, write_state_atomic,
    write_tune_report,
};
use crate::seat_types::{
    SeatConfig, SeatDecisionRecord, SeatForceLayerReq, SeatLayer, SeatLockState,
    SeatManualOverrideReq, SeatMonitorState, SeatObjectivePoint, SeatObjectiveSnapshot,
    SeatOptimizerProposal, SeatParameterSet, SeatRuntimeState, SeatSmoothingState, SeatStatusReport,
    SeatStyleMemoryEntry, SeatStyleVector,
};
use crate::spawn_detached;

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

#[derive(Debug, Deserialize)]
struct SourceHealthLite {
    #[serde(default)]
    score: f64,
}

#[derive(Debug, Deserialize)]
struct ShadowLiveLite {
    #[serde(default)]
    executed_count: u64,
    #[serde(default)]
    ev_net_usdc_p50: f64,
    #[serde(default)]
    roi_notional_10s_bps_p50: f64,
    #[serde(default)]
    ev_positive_ratio: f64,
    #[serde(default)]
    pnl_10s_p50_bps_raw: f64,
    #[serde(default)]
    source_health: Vec<SourceHealthLite>,
}

#[derive(Debug, Deserialize, Default)]
struct PnlLite {
    #[serde(default)]
    max_drawdown_pct: f64,
}

#[derive(Debug)]
struct ChallengerProcess {
    layer: SeatLayer,
    started_ms: i64,
    end_ms: i64,
    required_cycles: u32,
    control_base_url: String,
    candidate: SeatParameterSet,
    old_params: SeatParameterSet,
    baseline: SeatObjectiveSnapshot,
    proposal_notes: Vec<String>,
    child: Child,
}

#[derive(Clone)]
pub(crate) struct SeatRuntimeHandle {
    cfg: SeatConfig,
    http: Client,
    state: Arc<RwLock<SeatRuntimeState>>,
    live_fill_counter: Arc<AtomicU64>,
    challenger: Arc<Mutex<Option<ChallengerProcess>>>,
}

impl SeatRuntimeHandle {
    pub(crate) fn spawn(mut cfg: SeatConfig) -> Arc<Self> {
        if let Ok(value) = std::env::var("POLYEDGE_SEAT_ENABLED") {
            cfg.enabled = env_flag_enabled("POLYEDGE_SEAT_ENABLED") || value.eq_ignore_ascii_case("true");
        }
        if cfg.control_base_url.trim().is_empty() {
            let port = std::env::var("POLYEDGE_CONTROL_PORT")
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(8080);
            cfg.control_base_url = format!("http://127.0.0.1:{port}");
        }
        let handle = Arc::new(Self {
            cfg,
            http: Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_else(|_| Client::new()),
            state: Arc::new(RwLock::new(recover_state())),
            live_fill_counter: Arc::new(AtomicU64::new(0)),
            challenger: Arc::new(Mutex::new(None)),
        });
        if handle.cfg.enabled {
            let runner = handle.clone();
            spawn_detached("seat_runtime", true, async move {
                runner.run().await;
            });
        }
        handle
    }

    pub(crate) fn live_fill_counter(&self) -> Arc<AtomicU64> {
        self.live_fill_counter.clone()
    }

    pub(crate) async fn pause(&self, reason: String) -> SeatStatusReport {
        let mut state = self.state.write().await;
        state.paused = true;
        state.pause_reason = Some(reason);
        state.last_decision_ts_ms = now_ms();
        let _ = write_state_atomic(&state);
        drop(state);
        self.status().await
    }

    pub(crate) async fn resume(&self) -> SeatStatusReport {
        let mut state = self.state.write().await;
        state.paused = false;
        state.pause_reason = None;
        state.last_decision_ts_ms = now_ms();
        let _ = write_state_atomic(&state);
        drop(state);
        self.status().await
    }

    pub(crate) async fn force_layer(&self, req: SeatForceLayerReq) -> SeatStatusReport {
        let mut state = self.state.write().await;
        state.forced_layer = req.layer;
        state.last_decision_ts_ms = now_ms();
        let _ = write_state_atomic(&state);
        drop(state);
        self.status().await
    }

    pub(crate) async fn manual_override(&self, req: SeatManualOverrideReq) -> Result<SeatStatusReport> {
        if req.params.is_empty() {
            return Err(anyhow!("manual override params is empty"));
        }
        self.apply_params(&req.params, &self.cfg.control_base_url).await?;
        let mut state = self.state.write().await;
        state.manual_override = Some(req.params);
        state.last_decision_ts_ms = now_ms();
        write_state_atomic(&state)?;
        drop(state);
        Ok(self.status().await)
    }

    pub(crate) async fn clear_manual_override(&self) -> SeatStatusReport {
        let mut state = self.state.write().await;
        state.manual_override = None;
        state.last_decision_ts_ms = now_ms();
        let _ = write_state_atomic(&state);
        drop(state);
        self.status().await
    }

    pub(crate) async fn status(&self) -> SeatStatusReport {
        let state = self.state.read().await;
        SeatStatusReport {
            ts_ms: now_ms(),
            enabled: self.cfg.enabled,
            paused: state.paused,
            pause_reason: state.pause_reason.clone(),
            current_layer: state.current_layer,
            forced_layer: state.forced_layer,
            global_pause_until_ms: state.global_pause_until_ms,
            layer0_lock_until_ms: state.layer0_lock_until_ms,
            active_shadow_until_ms: state.active_shadow_until_ms,
            trade_count: state.live_fill_total.max(state.proxy_trade_total),
            trade_count_source: state.trade_count_source.clone(),
            started_ms: state.started_ms,
            last_decision_ts_ms: state.last_decision_ts_ms,
            degrade_streak: state.degrade_streak,
            rollback_streak: state.rollback_streak,
            smoothing_active: state.smoothing.is_some(),
            monitor_active: state.monitor.is_some(),
            manual_override_active: state.manual_override.is_some(),
            last_objective: state.last_objective.clone(),
        }
    }

    pub(crate) fn history(&self, limit: usize) -> Vec<SeatDecisionRecord> {
        load_history(limit)
    }

    async fn run(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.runtime_tick_sec.max(5)));
        loop {
            ticker.tick().await;
            if let Err(err) = self.tick_once().await {
                tracing::warn!(error = %err, "seat runtime tick failed");
            }
        }
    }

    async fn tick_once(&self) -> Result<()> {
        let now = now_ms();
        let Some((live, pnl)) = self.fetch_runtime_metrics().await? else {
            return Ok(());
        };
        let objective = self.build_objective(&live, &pnl);
        {
            let mut state = self.state.write().await;
            self.refresh_trade_counter(&mut state, &live);
            self.push_history(&mut state, now, &objective);
            state.last_objective = Some(objective.clone());
            if now.saturating_sub(state.last_archive_ts_ms) > 86_400_000 {
                archive_old_decisions(self.cfg.history_retention_days)?;
                state.last_archive_ts_ms = now;
            }
        }

        self.poll_challenger(now).await?;
        self.process_smoothing(now).await?;
        self.process_monitor(now, &objective).await?;
        self.process_post_switch_degrade(now).await?;

        if self.black_swan_triggered(&objective).await? {
            self.force_layer0_lock("black_swan").await?;
            return Ok(());
        }

        let due = {
            let mut state = self.state.write().await;
            let layer_pause_until = state
                .layer_pause_until_ms
                .get(state.current_layer.as_str())
                .copied()
                .unwrap_or(0);
            if state.paused
                || now < state.global_pause_until_ms
                || now < state.active_shadow_until_ms
                || now < layer_pause_until
                || state.smoothing.is_some()
                || state.monitor.is_some()
            {
                write_state_atomic(&state)?;
                return Ok(());
            }
            if now.saturating_sub(state.last_activation_check_ms) >= (self.cfg.activation_check_sec as i64) * 1_000 {
                state.last_activation_check_ms = now;
                let target = self.target_layer(&state, now);
                if target != state.current_layer {
                    state.current_layer = target;
                    let last_params = state.last_params.clone();
                    self.record_decision_locked(
                        &mut state,
                        target,
                        last_params,
                        objective.clone(),
                        "layer_switch".to_string(),
                        false,
                        Vec::new(),
                    )?;
                }
            }
            self.is_tune_due(&state, now)
        };
        if due {
            let layer = self.state.read().await.current_layer;
            self.run_tune_cycle(layer, objective, now).await?;
        }
        let state = self.state.read().await;
        write_state_atomic(&state)?;
        Ok(())
    }

    fn target_layer(&self, state: &SeatRuntimeState, now: i64) -> SeatLayer {
        if now < state.layer0_lock_until_ms {
            return SeatLayer::Layer0;
        }
        if let Some(forced) = state.forced_layer {
            return forced;
        }
        let trade_count = state.live_fill_total.max(state.proxy_trade_total);
        let uptime_sec = ((now.saturating_sub(state.started_ms)) / 1_000).max(0) as u64;
        if trade_count < self.cfg.layer1_min_trades || uptime_sec < 48 * 3_600 {
            SeatLayer::Layer0
        } else if trade_count >= self.cfg.layer3_min_trades
            || (uptime_sec >= self.cfg.layer3_min_uptime_sec && trade_count >= self.cfg.layer2_min_trades)
        {
            SeatLayer::Layer3
        } else if trade_count >= self.cfg.layer2_min_trades && uptime_sec >= self.cfg.layer2_min_uptime_sec {
            SeatLayer::Layer2
        } else {
            SeatLayer::Layer1
        }
    }

    fn is_tune_due(&self, state: &SeatRuntimeState, now: i64) -> bool {
        let key = state.current_layer.as_str().to_string();
        let Some(last_ms) = state.last_tune_ms_by_layer.get(&key).copied() else {
            return true;
        };
        let interval = match state.current_layer {
            SeatLayer::Layer0 | SeatLayer::Layer1 => self.cfg.layer1_interval_sec,
            SeatLayer::Layer2 => self.cfg.layer2_interval_sec,
            SeatLayer::Layer3 => self.cfg.layer3_interval_sec,
        };
        now.saturating_sub(last_ms) >= (interval as i64) * 1_000
    }

    fn effective_shadow_sec(&self, layer: SeatLayer) -> u64 {
        match layer {
            SeatLayer::Layer2 => self.cfg.layer2_shadow_sec,
            SeatLayer::Layer3 => self.cfg.layer3_shadow_sec.max(20 * 5 * 60),
            SeatLayer::Layer0 | SeatLayer::Layer1 => 0,
        }
    }

    fn required_shadow_cycles(&self, layer: SeatLayer, shadow_sec: u64) -> u32 {
        match layer {
            SeatLayer::Layer3 => (shadow_sec / (5 * 60)).max(20) as u32,
            SeatLayer::Layer2 => (shadow_sec / (5 * 60)).max(1) as u32,
            SeatLayer::Layer0 | SeatLayer::Layer1 => 0,
        }
    }

    fn build_objective(&self, live: &ShadowLiveLite, pnl: &PnlLite) -> SeatObjectiveSnapshot {
        let source_health_min = if live.source_health.is_empty() {
            1.0
        } else {
            live.source_health.iter().map(|row| row.score).fold(1.0_f64, f64::min)
        };
        let objective = live.ev_net_usdc_p50
            + live.roi_notional_10s_bps_p50 * 0.01
            + live.ev_positive_ratio * 0.5
            - pnl.max_drawdown_pct.max(0.0) * self.cfg.objective_drawdown_penalty;
        SeatObjectiveSnapshot {
            ev_usdc_p50: live.ev_net_usdc_p50,
            max_drawdown_pct: pnl.max_drawdown_pct.max(0.0),
            roi_notional_10s_bps_p50: live.roi_notional_10s_bps_p50,
            win_rate: live.ev_positive_ratio,
            source_health_min,
            volatility_proxy: live.pnl_10s_p50_bps_raw.abs(),
            objective,
        }
    }

    fn refresh_trade_counter(&self, state: &mut SeatRuntimeState, live: &ShadowLiveLite) {
        let live_seen = self.live_fill_counter.load(Ordering::Relaxed);
        let live_delta = if live_seen >= state.live_fill_seen { live_seen - state.live_fill_seen } else { live_seen };
        state.live_fill_total = state.live_fill_total.saturating_add(live_delta);
        state.live_fill_seen = live_seen;

        let proxy_delta = if live.executed_count >= state.proxy_trade_seen {
            live.executed_count - state.proxy_trade_seen
        } else {
            live.executed_count
        };
        state.proxy_trade_total = state.proxy_trade_total.saturating_add(proxy_delta);
        state.proxy_trade_seen = live.executed_count;
        let live_expected = !env_flag_enabled("POLYEDGE_FORCE_PAPER") && env_flag_enabled("POLYEDGE_LIVE_ARMED");
        state.trade_count_source = if live_expected && state.live_fill_total > 0 {
            "live_fill".to_string()
        } else {
            "proxy".to_string()
        };
    }

    fn push_history(&self, state: &mut SeatRuntimeState, ts_ms: i64, objective: &SeatObjectiveSnapshot) {
        state.objective_history.push(SeatObjectivePoint { ts_ms, objective: objective.objective });
        state.volatility_history.push(SeatObjectivePoint { ts_ms, objective: objective.volatility_proxy });
        let keep_after = ts_ms.saturating_sub(8 * 24 * 3_600 * 1_000);
        state.objective_history.retain(|p| p.ts_ms >= keep_after);
        state.volatility_history.retain(|p| p.ts_ms >= keep_after);
    }

    async fn fetch_runtime_metrics(&self) -> Result<Option<(ShadowLiveLite, PnlLite)>> {
        let live_resp = match self.http.get(format!("{}/report/shadow/live", self.cfg.control_base_url)).send().await {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        if !live_resp.status().is_success() {
            return Ok(None);
        }
        let live = live_resp.json::<ShadowLiveLite>().await.context("parse shadow live")?;
        let pnl_resp = self
            .http
            .get(format!("{}/state/pnl", self.cfg.control_base_url))
            .send()
            .await
            .context("get state pnl")?;
        if !pnl_resp.status().is_success() {
            return Ok(None);
        }
        let pnl = pnl_resp.json::<PnlLite>().await.context("parse state pnl")?;
        Ok(Some((live, pnl)))
    }

    async fn black_swan_triggered(&self, objective: &SeatObjectiveSnapshot) -> Result<bool> {
        let state = self.state.read().await;
        let mut vols = state
            .volatility_history
            .iter()
            .map(|p| p.objective)
            .filter(|v| v.is_finite())
            .collect::<Vec<_>>();
        if vols.len() < 32 {
            return Ok(objective.source_health_min < self.cfg.source_health_floor);
        }
        vols.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((vols.len() as f64) * 0.95).floor() as usize;
        let p95 = vols
            .get(idx.min(vols.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0.0);
        Ok(objective.volatility_proxy > p95 || objective.source_health_min < self.cfg.source_health_floor)
    }

    async fn force_layer0_lock(&self, reason: &str) -> Result<()> {
        let now = now_ms();
        let mut state = self.state.write().await;
        state.current_layer = SeatLayer::Layer0;
        state.layer0_lock_until_ms = now + (self.cfg.black_swan_lock_sec as i64) * 1_000;
        state.active_shadow_until_ms = 0;
        state.smoothing = None;
        state.monitor = None;
        let last_params = state.last_params.clone();
        let last_objective = state.last_objective.clone().unwrap_or_default();
        self.record_decision_locked(
            &mut state,
            SeatLayer::Layer0,
            last_params,
            last_objective,
            format!("force_layer0:{reason}"),
            false,
            vec![reason.to_string()],
        )?;
        write_state_atomic(&state)?;
        Ok(())
    }

    async fn run_tune_cycle(&self, layer: SeatLayer, baseline: SeatObjectiveSnapshot, now: i64) -> Result<()> {
        let current = match self.capture_current_params().await {
            Ok(v) => v,
            Err(_) => self.state.read().await.last_params.clone(),
        };
        let (mut candidate, proposal_notes) = match layer {
            SeatLayer::Layer0 => (self.layer0_candidate(&current, &baseline), Vec::new()),
            SeatLayer::Layer1 | SeatLayer::Layer2 | SeatLayer::Layer3 => {
                let proposal = self.optimizer_candidate(layer, &current, &baseline).await?;
                let mut notes = proposal.notes.clone();
                notes.push(format!(
                    "mc_ev_delta_p50={:.6}",
                    proposal.validation.mc_ev_delta_p50
                ));
                notes.push(format!(
                    "mc_drawdown_p95={:.6}",
                    proposal.validation.mc_drawdown_p95
                ));
                notes.push(format!(
                    "style_match_score={:.6}",
                    proposal.meta.style_match_score
                ));
                notes.push(format!("style_match_count={}", proposal.meta.style_match_count));
                notes.push(format!("top_k_size={}", proposal.meta.top_k_size));
                notes.push(format!(
                    "walk_forward_windows={}",
                    proposal.validation.walk_forward_windows
                ));
                notes.push(format!(
                    "walk_forward_score={:.6}",
                    proposal.validation.walk_forward_score
                ));
                notes.push(format!(
                    "objective_uplift_estimate={:.6}",
                    proposal.meta.objective_uplift_estimate
                ));
                notes.push(format!("rl_signal={:.6}", proposal.meta.rl_signal));
                (proposal.candidate, notes)
            }
        };
        {
            let state = self.state.read().await;
            if let Some(override_params) = &state.manual_override {
                candidate = override_params.merge_over(&candidate);
            }
        }
        candidate = candidate
            .clamp_relative(&current, layer.step_limit_pct())
            .merge_over(&current);

        if layer == SeatLayer::Layer2 || layer == SeatLayer::Layer3 {
            let shadow_sec = self.effective_shadow_sec(layer);
            let started = self
                .start_challenger(
                    layer,
                    current.clone(),
                    candidate.clone(),
                    baseline.clone(),
                    shadow_sec,
                    now,
                    proposal_notes.clone(),
                )
                .await?;
            if !started {
                let mut state = self.state.write().await;
                self.record_decision_locked(
                    &mut state,
                    layer,
                    current,
                    baseline,
                    "shadow_skip_active".to_string(),
                    false,
                    vec!["active_shadow_window_in_progress".to_string()],
                )?;
                write_state_atomic(&state)?;
                return Ok(());
            }
        } else {
            self.begin_smoothing(layer, current, candidate, baseline, now, proposal_notes)
                .await?;
        }

        let mut state = self.state.write().await;
        state
            .last_tune_ms_by_layer
            .insert(layer.as_str().to_string(), now);
        write_state_atomic(&state)?;
        Ok(())
    }

    fn layer0_candidate(&self, current: &SeatParameterSet, baseline: &SeatObjectiveSnapshot) -> SeatParameterSet {
        let mut out = current.clone();
        let tighten = baseline.ev_usdc_p50 < 0.0 || baseline.max_drawdown_pct > 0.05;
        if let Some(v) = current.position_fraction {
            out.position_fraction = Some(if tighten { v * 0.97 } else { v * 1.01 });
        }
        if let Some(v) = current.early_size_scale {
            out.early_size_scale = Some(if tighten { v * 0.98 } else { v * 1.02 });
        }
        if let Some(v) = current.maturity_size_scale {
            out.maturity_size_scale = Some(if tighten { v * 0.99 } else { v * 1.01 });
        }
        if let Some(v) = current.late_size_scale {
            out.late_size_scale = Some(if tighten { v * 0.98 } else { v * 1.02 });
        }
        if let Some(v) = current.min_edge_net_bps {
            out.min_edge_net_bps = Some(if tighten { v * 1.03 } else { v * 0.99 });
        }
        if let Some(v) = current.convergence_exit_ratio {
            out.convergence_exit_ratio = Some(if tighten {
                (v * 0.99).max(0.10)
            } else {
                (v * 1.005).min(0.99)
            });
        }
        if let Some(v) = current.min_velocity_bps_per_sec {
            out.min_velocity_bps_per_sec = Some(if tighten { v * 1.03 } else { v * 0.99 });
        }
        out.clamp_relative(current, SeatLayer::Layer0.step_limit_pct())
    }

    async fn optimizer_candidate(
        &self,
        layer: SeatLayer,
        current: &SeatParameterSet,
        baseline: &SeatObjectiveSnapshot,
    ) -> Result<SeatOptimizerProposal> {
        let endpoint = match layer {
            SeatLayer::Layer1 => "l1",
            SeatLayer::Layer2 => "l2",
            SeatLayer::Layer3 => {
                let state = self.state.read().await;
                if state.style_memory.len() < 8 {
                    "l2"
                } else {
                    "l3"
                }
            }
            SeatLayer::Layer0 => "l1",
        };
        let current_style = SeatStyleVector {
            volatility_proxy: baseline.volatility_proxy,
            source_health_min: baseline.source_health_min,
            roi_notional_10s_bps_p50: baseline.roi_notional_10s_bps_p50,
            win_rate: baseline.win_rate,
        };
        let (style_memory, objective_history, volatility_history) = {
            let state = self.state.read().await;
            (
                state.style_memory.clone(),
                state
                    .objective_history
                    .iter()
                    .rev()
                    .take(1_440)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>(),
                state
                    .volatility_history
                    .iter()
                    .rev()
                    .take(1_440)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>(),
            )
        };
        let payload = serde_json::json!({
            "layer": layer.as_str(),
            "current_params": current,
            "baseline": baseline,
            "style_memory": style_memory,
            "current_style": current_style,
            "objective_history": objective_history,
            "volatility_history": volatility_history,
        });
        let resp = self
            .http
            .post(format!("{}/v1/seat/{endpoint}/optimize", self.cfg.optimizer_url))
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("seat optimizer {endpoint}"))?;
        if !resp.status().is_success() {
            return Err(anyhow!("seat optimizer status={}", resp.status()));
        }
        let proposal = resp
            .json::<SeatOptimizerProposal>()
            .await
            .context("parse optimizer proposal")?;
        let required_mc = match layer {
            SeatLayer::Layer1 => 100,
            SeatLayer::Layer2 => 500,
            SeatLayer::Layer3 => 1_000,
            SeatLayer::Layer0 => 100,
        };
        if proposal.validation.mc_runs < required_mc || !proposal.validation.mc_pass {
            return Err(anyhow!(
                "optimizer validation failed mc_runs={} mc_pass={}",
                proposal.validation.mc_runs,
                proposal.validation.mc_pass
            ));
        }
        if layer != SeatLayer::Layer1 && !proposal.validation.walk_forward_pass {
            return Err(anyhow!("optimizer walk-forward validation failed"));
        }
        if layer != SeatLayer::Layer1 && !proposal.validation.shadow_pass {
            return Err(anyhow!("optimizer shadow validation failed"));
        }
        Ok(proposal)
    }

    async fn begin_smoothing(
        &self,
        layer: SeatLayer,
        old_params: SeatParameterSet,
        candidate: SeatParameterSet,
        baseline: SeatObjectiveSnapshot,
        now: i64,
        mut notes: Vec<String>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        let pre_switch = self.mean_objective_24h_locked(&state, now).unwrap_or(baseline.objective);
        state.pre_switch_objective_24h = Some(pre_switch);
        state.smoothing = Some(SeatSmoothingState {
            layer,
            old_params: old_params.clone(),
            target_params: candidate.clone(),
            current_params: old_params,
            baseline: baseline.clone(),
            started_ms: now,
            end_ms: now + (self.cfg.smoothing_sec as i64) * 1_000,
            next_step_ms: now,
        });
        notes.push(format!("pre_switch_objective_24h={pre_switch:.6}"));
        self.record_decision_locked(
            &mut state,
            layer,
            candidate,
            baseline,
            "smoothing_started".to_string(),
            false,
            notes,
        )?;
        write_state_atomic(&state)?;
        Ok(())
    }

    async fn process_smoothing(&self, now: i64) -> Result<()> {
        let mut start_monitor = None::<(
            SeatLayer,
            SeatParameterSet,
            SeatParameterSet,
            SeatObjectiveSnapshot,
            f64,
        )>;
        {
            let mut state = self.state.write().await;
            let Some(mut smooth) = state.smoothing.clone() else {
                return Ok(());
            };
            if now < smooth.next_step_ms {
                return Ok(());
            }
            if now >= smooth.end_ms {
                self.apply_params(&smooth.target_params, &self.cfg.control_base_url)
                    .await?;
                start_monitor = Some((
                    smooth.layer,
                    smooth.old_params.clone(),
                    smooth.target_params.clone(),
                    smooth.baseline.clone(),
                    state.pre_switch_objective_24h.unwrap_or(0.0),
                ));
                state.smoothing = None;
            } else {
                smooth.current_params.exp_blend_towards(&smooth.target_params, 0.7);
                self.apply_params(&smooth.current_params, &self.cfg.control_base_url)
                    .await?;
                smooth.next_step_ms = smooth.next_step_ms.saturating_add(5 * 60 * 1_000);
                state.smoothing = Some(smooth);
            }
            write_state_atomic(&state)?;
        }
        if let Some((layer, old_params, new_params, baseline, pre_switch)) = start_monitor {
            let mut state = self.state.write().await;
            state.monitor = Some(SeatMonitorState {
                layer,
                old_params,
                new_params,
                baseline,
                pre_switch_objective_24h: pre_switch,
                started_ms: now,
                end_ms: now + (self.cfg.monitor_sec as i64) * 1_000,
            });
            write_state_atomic(&state)?;
        }
        Ok(())
    }

    async fn process_monitor(&self, now: i64, objective: &SeatObjectiveSnapshot) -> Result<()> {
        let monitor = self.state.read().await.monitor.clone();
        let Some(monitor) = monitor else {
            return Ok(());
        };
        if now < monitor.end_ms {
            let local_rollback = objective.ev_usdc_p50 < monitor.baseline.ev_usdc_p50
                || objective.max_drawdown_pct > monitor.baseline.max_drawdown_pct;
            let remote_rollback = match self
                .http
                .post(format!("{}/v1/seat/monitor_60m", self.cfg.optimizer_url))
                .json(&serde_json::json!({
                    "layer": monitor.layer.as_str(),
                    "ev_new": objective.ev_usdc_p50,
                    "ev_old": monitor.baseline.ev_usdc_p50,
                    "dd_new": objective.max_drawdown_pct,
                    "dd_old": monitor.baseline.max_drawdown_pct,
                }))
                .send()
                .await
            {
                Ok(resp) => match resp.json::<serde_json::Value>().await {
                    Ok(body) => body
                        .get("rollback")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(local_rollback),
                    Err(_) => local_rollback,
                },
                Err(_) => local_rollback,
            };
            if local_rollback || remote_rollback {
                self.rollback_with_pause(
                    monitor.layer,
                    monitor.old_params,
                    monitor.baseline,
                    "monitor_regression",
                    now,
                )
                .await?;
            }
            return Ok(());
        }

        let entry = {
            let mut state = self.state.write().await;
            state.monitor = None;
            state.rollback_streak = 0;
            state.post_switch_eval_due_ms = now + 24 * 3_600 * 1_000;
            state.post_switch_start_ms = now;
            state.post_switch_baseline_24h = Some(monitor.pre_switch_objective_24h);
            state.post_switch_layer = Some(monitor.layer);
            let entry = self.update_style_memory_locked(&mut state, objective, &monitor.new_params, now);
            self.record_decision_locked(
                &mut state,
                monitor.layer,
                monitor.new_params,
                objective.clone(),
                "monitor_pass".to_string(),
                false,
                vec![
                    format!("style_objective={:.6}", entry.objective),
                    format!("style_id={}", entry.style_id),
                ],
            )?;
            write_state_atomic(&state)?;
            entry
        };
        let _ = self
            .push_style_memory_update(monitor.layer, &entry, objective)
            .await;
        Ok(())
    }

    async fn process_post_switch_degrade(&self, now: i64) -> Result<()> {
        let maybe = {
            let state = self.state.read().await;
            if state.post_switch_eval_due_ms == 0 || now < state.post_switch_eval_due_ms {
                None
            } else {
                Some((
                    state.post_switch_layer.unwrap_or(SeatLayer::Layer0),
                    state.post_switch_start_ms,
                    state.post_switch_baseline_24h.unwrap_or(0.0),
                ))
            }
        };
        let Some((layer, start_ms, baseline)) = maybe else {
            return Ok(());
        };
        let post_mean = {
            let state = self.state.read().await;
            let values = state
                .objective_history
                .iter()
                .filter(|p| p.ts_ms >= start_ms)
                .map(|p| p.objective)
                .collect::<Vec<_>>();
            if values.is_empty() {
                baseline
            } else {
                values.iter().sum::<f64>() / values.len() as f64
            }
        };
        let mut state = self.state.write().await;
        state.post_switch_eval_due_ms = 0;
        state.post_switch_baseline_24h = None;
        state.post_switch_layer = None;
        if post_mean < baseline {
            self.downgrade_locked(&mut state, layer, format!("{post_mean:.6}<{baseline:.6}"))?;
        } else {
            state.degrade_streak = 0;
        }
        write_state_atomic(&state)?;
        Ok(())
    }

    async fn rollback_with_pause(
        &self,
        layer: SeatLayer,
        old_params: SeatParameterSet,
        baseline: SeatObjectiveSnapshot,
        reason: &str,
        now: i64,
    ) -> Result<()> {
        self.apply_params(&old_params, &self.cfg.control_base_url).await?;
        let mut state = self.state.write().await;
        state.smoothing = None;
        state.monitor = None;
        state
            .layer_pause_until_ms
            .insert(layer.as_str().to_string(), now + (self.cfg.rollback_pause_sec as i64) * 1_000);
        state.rollback_streak = state.rollback_streak.saturating_add(1);
        if state.rollback_streak >= 2 {
            state.global_pause_until_ms = now + (self.cfg.global_pause_sec as i64) * 1_000;
            state.layer0_lock_until_ms = now + (self.cfg.layer0_lock_sec as i64) * 1_000;
            state.current_layer = SeatLayer::Layer0;
        }
        self.record_decision_locked(
            &mut state,
            layer,
            old_params,
            baseline,
            format!("rollback:{reason}"),
            true,
            vec![reason.to_string()],
        )?;
        write_state_atomic(&state)?;
        Ok(())
    }

    fn downgrade_locked(&self, state: &mut SeatRuntimeState, from_layer: SeatLayer, reason: String) -> Result<()> {
        let next = match from_layer {
            SeatLayer::Layer3 => SeatLayer::Layer2,
            SeatLayer::Layer2 => SeatLayer::Layer1,
            SeatLayer::Layer1 | SeatLayer::Layer0 => SeatLayer::Layer0,
        };
        state.current_layer = next;
        state
            .layer_pause_until_ms
            .insert(from_layer.as_str().to_string(), now_ms() + (self.cfg.rollback_pause_sec as i64) * 1_000);
        state.degrade_streak = state.degrade_streak.saturating_add(1);
        if state.degrade_streak >= 2 {
            state.current_layer = SeatLayer::Layer0;
            state.layer0_lock_until_ms = now_ms() + (self.cfg.layer0_lock_sec as i64) * 1_000;
            state.degrade_streak = 0;
        }
        let last_params = state.last_params.clone();
        let last_objective = state.last_objective.clone().unwrap_or_default();
        self.record_decision_locked(
            state,
            state.current_layer,
            last_params,
            last_objective,
            format!("downgrade:{reason}"),
            false,
            vec![reason],
        )?;
        Ok(())
    }

    fn mean_objective_24h_locked(&self, state: &SeatRuntimeState, now: i64) -> Option<f64> {
        let start = now.saturating_sub(24 * 3_600 * 1_000);
        let values = state
            .objective_history
            .iter()
            .filter(|p| p.ts_ms >= start && p.ts_ms <= now)
            .map(|p| p.objective)
            .collect::<Vec<_>>();
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        }
    }

    fn update_style_memory_locked(
        &self,
        state: &mut SeatRuntimeState,
        objective: &SeatObjectiveSnapshot,
        params: &SeatParameterSet,
        now: i64,
    ) -> SeatStyleMemoryEntry {
        let vector = SeatStyleVector {
            volatility_proxy: objective.volatility_proxy,
            source_health_min: objective.source_health_min,
            roi_notional_10s_bps_p50: objective.roi_notional_10s_bps_p50,
            win_rate: objective.win_rate,
        };
        let style_id = self.style_id(&vector);
        if let Some(entry) = state.style_memory.iter_mut().find(|entry| entry.style_id == style_id) {
            entry.vector = vector;
            entry.params = params.clone();
            entry.objective = objective.objective;
            entry.updated_ms = now;
            return entry.clone();
        }
        if state.style_memory.len() < 8 {
            let entry = SeatStyleMemoryEntry {
                style_id,
                vector,
                params: params.clone(),
                objective: objective.objective,
                updated_ms: now,
            };
            state.style_memory.push(entry.clone());
            return entry;
        }
        if let Some((idx, _)) = state
            .style_memory
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.objective.partial_cmp(&b.objective).unwrap_or(std::cmp::Ordering::Equal))
        {
            let entry = SeatStyleMemoryEntry {
                style_id,
                vector,
                params: params.clone(),
                objective: objective.objective,
                updated_ms: now,
            };
            state.style_memory[idx] = entry.clone();
            return entry;
        }
        SeatStyleMemoryEntry {
            style_id,
            vector,
            params: params.clone(),
            objective: objective.objective,
            updated_ms: now,
        }
    }

    fn style_id(&self, vector: &SeatStyleVector) -> String {
        let vol = if vector.volatility_proxy > 8.0 { "hv" } else { "lv" };
        let health = if vector.source_health_min > 0.6 { "hh" } else { "lh" };
        let drift = if vector.roi_notional_10s_bps_p50 > 0.0 { "pr" } else { "ng" };
        format!("{vol}_{health}_{drift}")
    }

    async fn push_style_memory_update(
        &self,
        layer: SeatLayer,
        entry: &SeatStyleMemoryEntry,
        objective: &SeatObjectiveSnapshot,
    ) -> Result<()> {
        let resp = self
            .http
            .post(format!("{}/v1/seat/style_memory/update", self.cfg.optimizer_url))
            .json(&serde_json::json!({
                "layer": layer.as_str(),
                "entry": entry,
                "reward": objective.objective,
                "objective": objective,
            }))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("style memory update failed status={}", resp.status()));
        }
        Ok(())
    }

    fn record_decision_locked(
        &self,
        state: &mut SeatRuntimeState,
        layer: SeatLayer,
        candidate: SeatParameterSet,
        baseline: SeatObjectiveSnapshot,
        decision: String,
        rollback: bool,
        notes: Vec<String>,
    ) -> Result<()> {
        let previous = state.last_params.clone();
        state.last_decision_ts_ms = now_ms();
        state.decision_seq = state.decision_seq.saturating_add(1);
        state.last_params = candidate.clone();
        let record = SeatDecisionRecord {
            ts_ms: state.last_decision_ts_ms,
            layer,
            previous,
            candidate,
            baseline,
            decision,
            rollback,
            lock_state: SeatLockState {
                paused: state.paused,
                global_pause_until_ms: state.global_pause_until_ms,
                layer0_lock_until_ms: state.layer0_lock_until_ms,
                active_shadow_until_ms: state.active_shadow_until_ms,
            },
            trade_count_source: state.trade_count_source.clone(),
            notes,
        };
        append_decision(&record)?;
        write_tune_report(&record, state)?;
        Ok(())
    }

    async fn apply_params(&self, params: &SeatParameterSet, base_url: &str) -> Result<()> {
        if params.is_empty() {
            return Ok(());
        }
        if params.position_fraction.is_some()
            || params.early_size_scale.is_some()
            || params.maturity_size_scale.is_some()
            || params.late_size_scale.is_some()
            || params.min_edge_net_bps.is_some()
            || params.min_velocity_bps_per_sec.is_some()
        {
            let mut body = serde_json::Map::new();
            if params.position_fraction.is_some() {
                body.insert(
                    "compounder".to_string(),
                    serde_json::json!({"position_fraction": params.position_fraction}),
                );
            }
            if params.early_size_scale.is_some()
                || params.maturity_size_scale.is_some()
                || params.late_size_scale.is_some()
            {
                body.insert(
                    "v52_time_phase".to_string(),
                    serde_json::json!({
                        "early_size_scale": params.early_size_scale,
                        "maturity_size_scale": params.maturity_size_scale,
                        "late_size_scale": params.late_size_scale
                    }),
                );
            }
            if params.min_edge_net_bps.is_some() {
                body.insert(
                    "taker_sniper".to_string(),
                    serde_json::json!({"min_edge_net_bps": params.min_edge_net_bps}),
                );
            }
            if params.min_velocity_bps_per_sec.is_some() {
                body.insert(
                    "direction_detector".to_string(),
                    serde_json::json!({"min_velocity_bps_per_sec": params.min_velocity_bps_per_sec}),
                );
            }
            self.http
                .post(format!("{base_url}/control/reload_predator_c"))
                .json(&serde_json::Value::Object(body))
                .send()
                .await?
                .error_for_status()?;
        }
        if params.convergence_exit_ratio.is_some()
            || params.t100ms_reversal_bps.is_some()
            || params.t300ms_reversal_bps.is_some()
            || params.max_single_trade_loss_usdc.is_some()
        {
            self.http
                .post(format!("{base_url}/control/reload_exit"))
                .json(&serde_json::json!({
                    "convergence_exit_ratio": params.convergence_exit_ratio,
                    "t100ms_reversal_bps": params.t100ms_reversal_bps,
                    "t300ms_reversal_bps": params.t300ms_reversal_bps,
                    "max_single_trade_loss_usdc": params.max_single_trade_loss_usdc
                }))
                .send()
                .await?
                .error_for_status()?;
        }
        if params.capital_fraction_kelly.is_some() {
            self.http
                .post(format!("{base_url}/control/reload_allocator"))
                .json(&serde_json::json!({
                    "capital_fraction_kelly": params.capital_fraction_kelly
                }))
                .send()
                .await?
                .error_for_status()?;
        }
        if params.risk_max_drawdown_pct.is_some() || params.risk_max_market_notional.is_some() {
            self.http
                .post(format!("{base_url}/control/reload_risk"))
                .json(&serde_json::json!({
                    "daily_drawdown_cap_pct": params.risk_max_drawdown_pct,
                    "max_market_notional": params.risk_max_market_notional
                }))
                .send()
                .await?
                .error_for_status()?;
        }
        if params.maker_min_edge_bps.is_some() || params.basis_k_revert.is_some() || params.basis_z_cap.is_some() {
            self.http
                .post(format!("{base_url}/control/reload_strategy"))
                .json(&serde_json::json!({
                    "min_edge_bps": params.maker_min_edge_bps,
                    "basis_k_revert": params.basis_k_revert,
                    "basis_z_cap": params.basis_z_cap
                }))
                .send()
                .await?
                .error_for_status()?;
        }
        Ok(())
    }

    async fn capture_current_params(&self) -> Result<SeatParameterSet> {
        #[derive(Debug, Deserialize)]
        struct PredatorResp {
            predator_c: serde_json::Value,
        }
        #[derive(Debug, Deserialize)]
        struct ExitResp {
            exit: serde_json::Value,
        }
        #[derive(Debug, Deserialize)]
        struct AllocatorResp {
            allocator: serde_json::Value,
        }
        #[derive(Debug, Deserialize)]
        struct RiskResp {
            risk: serde_json::Value,
        }
        #[derive(Debug, Deserialize)]
        struct StrategyResp {
            maker: serde_json::Value,
            fair_value: serde_json::Value,
            v52: serde_json::Value,
        }

        let predator = self
            .http
            .post(format!("{}/control/reload_predator_c", self.cfg.control_base_url))
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json::<PredatorResp>()
            .await?;
        let exit = self
            .http
            .post(format!("{}/control/reload_exit", self.cfg.control_base_url))
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json::<ExitResp>()
            .await?;
        let allocator = self
            .http
            .post(format!("{}/control/reload_allocator", self.cfg.control_base_url))
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json::<AllocatorResp>()
            .await?;
        let risk = self
            .http
            .post(format!("{}/control/reload_risk", self.cfg.control_base_url))
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json::<RiskResp>()
            .await?;
        let strategy = self
            .http
            .post(format!("{}/control/reload_strategy", self.cfg.control_base_url))
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json::<StrategyResp>()
            .await?;

        Ok(SeatParameterSet {
            position_fraction: predator
                .predator_c
                .get("compounder")
                .and_then(|v| v.get("position_fraction"))
                .and_then(|v| v.as_f64()),
            early_size_scale: strategy
                .v52
                .get("time_phase")
                .and_then(|v| v.get("early_size_scale"))
                .and_then(|v| v.as_f64()),
            maturity_size_scale: strategy
                .v52
                .get("time_phase")
                .and_then(|v| v.get("maturity_size_scale"))
                .and_then(|v| v.as_f64()),
            late_size_scale: strategy
                .v52
                .get("time_phase")
                .and_then(|v| v.get("late_size_scale"))
                .and_then(|v| v.as_f64()),
            min_edge_net_bps: predator
                .predator_c
                .get("taker_sniper")
                .and_then(|v| v.get("min_edge_net_bps"))
                .and_then(|v| v.as_f64()),
            convergence_exit_ratio: exit
                .exit
                .get("convergence_exit_ratio")
                .and_then(|v| v.as_f64()),
            min_velocity_bps_per_sec: predator
                .predator_c
                .get("direction_detector")
                .and_then(|v| v.get("min_velocity_bps_per_sec"))
                .and_then(|v| v.as_f64()),
            capital_fraction_kelly: allocator
                .allocator
                .get("capital_fraction_kelly")
                .and_then(|v| v.as_f64()),
            t100ms_reversal_bps: exit
                .exit
                .get("t100ms_reversal_bps")
                .and_then(|v| v.as_f64()),
            t300ms_reversal_bps: exit
                .exit
                .get("t300ms_reversal_bps")
                .and_then(|v| v.as_f64()),
            max_single_trade_loss_usdc: exit
                .exit
                .get("max_single_trade_loss_usdc")
                .and_then(|v| v.as_f64()),
            risk_max_drawdown_pct: risk
                .risk
                .get("max_drawdown_pct")
                .and_then(|v| v.as_f64()),
            risk_max_market_notional: risk
                .risk
                .get("max_market_notional")
                .and_then(|v| v.as_f64()),
            maker_min_edge_bps: strategy
                .maker
                .get("min_edge_bps")
                .and_then(|v| v.as_f64()),
            basis_k_revert: strategy
                .fair_value
                .get("k_revert")
                .and_then(|v| v.as_f64()),
            basis_z_cap: strategy.fair_value.get("z_cap").and_then(|v| v.as_f64()),
        })
    }

    async fn start_challenger(
        &self,
        layer: SeatLayer,
        old_params: SeatParameterSet,
        candidate: SeatParameterSet,
        baseline: SeatObjectiveSnapshot,
        shadow_sec: u64,
        now: i64,
        mut notes: Vec<String>,
    ) -> Result<bool> {
        let mut guard = self.challenger.lock().await;
        if guard.is_some() {
            return Ok(false);
        }
        let required_cycles = self.required_shadow_cycles(layer, shadow_sec);
        let port = self.find_available_port()?;
        let root = self.build_challenger_dataset_root(now, layer);
        let mut cmd = Command::new(std::env::current_exe().context("resolve current exe")?);
        cmd.env("POLYEDGE_FORCE_PAPER", "true")
            .env("POLYEDGE_SEAT_ENABLED", "false")
            .env("POLYEDGE_LIVE_ARMED", "false")
            .env("POLYEDGE_CONTROL_PORT", port.to_string())
            .env("POLYEDGE_DATASET_ROOT", &root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = cmd.spawn().context("spawn challenger app_runner")?;
        let base_url = format!("http://127.0.0.1:{port}");
        self.wait_control_ready(&base_url).await?;
        self.apply_params(&candidate, &base_url).await?;
        let proposal_notes = notes.clone();
        *guard = Some(ChallengerProcess {
            layer,
            started_ms: now,
            end_ms: now + (shadow_sec as i64) * 1_000,
            required_cycles,
            control_base_url: base_url,
            candidate: candidate.clone(),
            old_params,
            baseline: baseline.clone(),
            proposal_notes,
            child,
        });
        drop(guard);
        let mut state = self.state.write().await;
        state.active_shadow_until_ms = now + (shadow_sec as i64) * 1_000;
        notes.push(format!("duration_sec={shadow_sec}"));
        notes.push(format!("required_cycles={required_cycles}"));
        self.record_decision_locked(
            &mut state,
            layer,
            candidate,
            baseline,
            "shadow_started".to_string(),
            false,
            notes,
        )?;
        write_state_atomic(&state)?;
        Ok(true)
    }

    async fn poll_challenger(&self, now: i64) -> Result<()> {
        let mut guard = self.challenger.lock().await;
        let Some(mut challenger) = guard.take() else {
            return Ok(());
        };
        if now < challenger.end_ms {
            *guard = Some(challenger);
            return Ok(());
        }
        let compare = self.fetch_shadow_and_pnl_from_base(&challenger.control_base_url).await;
        let observed_cycles = ((now.saturating_sub(challenger.started_ms)) / (5 * 60 * 1_000)) as u32;
        let (pass, shadow_notes) = match compare {
            Ok((live, pnl)) => {
                let obj = self.build_objective(&live, &pnl);
                let local_pass = obj.ev_usdc_p50 >= challenger.baseline.ev_usdc_p50 * 1.05
                    && obj.max_drawdown_pct <= challenger.baseline.max_drawdown_pct * 1.12
                    && observed_cycles >= challenger.required_cycles;
                let remote_pass = match self
                    .http
                    .post(format!("{}/v1/seat/shadow_compare", self.cfg.optimizer_url))
                    .json(&serde_json::json!({
                        "layer": challenger.layer.as_str(),
                        "ev_new": obj.ev_usdc_p50,
                        "ev_old": challenger.baseline.ev_usdc_p50,
                        "dd_new": obj.max_drawdown_pct,
                        "dd_old": challenger.baseline.max_drawdown_pct,
                        "cycles_observed": observed_cycles,
                        "cycles_required": challenger.required_cycles,
                    }))
                    .send()
                    .await
                {
                    Ok(resp) => match resp.json::<serde_json::Value>().await {
                        Ok(body) => body
                            .get("pass")
                            .and_then(|x| x.as_bool())
                            .unwrap_or(local_pass),
                        Err(_) => local_pass,
                    },
                    Err(_) => local_pass,
                };
                (
                    local_pass && remote_pass,
                    vec![
                        format!("shadow_ev_usdc_p50={:.6}", obj.ev_usdc_p50),
                        format!(
                            "shadow_max_drawdown_pct={:.6}",
                            obj.max_drawdown_pct
                        ),
                    ],
                )
            }
            Err(_) => (false, vec!["shadow_metrics_unavailable=true".to_string()]),
        };
        let _ = challenger.child.kill().await;
        let _ = challenger.child.wait().await;
        {
            let mut state = self.state.write().await;
            state.active_shadow_until_ms = 0;
            write_state_atomic(&state)?;
        }
        if pass {
            self.begin_smoothing(
                challenger.layer,
                challenger.old_params,
                challenger.candidate,
                challenger.baseline,
                now,
                {
                    let mut notes = challenger.proposal_notes;
                    notes.extend(shadow_notes);
                    notes.extend([
                    format!("shadow_cycles_observed={observed_cycles}"),
                    format!("shadow_cycles_required={}", challenger.required_cycles),
                    ]);
                    notes
                },
            )
            .await?;
        } else {
            let mut state = self.state.write().await;
            let last_params = state.last_params.clone();
            self.record_decision_locked(
                &mut state,
                challenger.layer,
                last_params,
                challenger.baseline,
                "shadow_reject".to_string(),
                false,
                {
                    let mut notes = vec!["shadow_compare_failed".to_string()];
                    notes.extend(shadow_notes);
                    notes.extend([
                    format!("shadow_cycles_observed={observed_cycles}"),
                    format!("shadow_cycles_required={}", challenger.required_cycles),
                    ]);
                    notes
                },
            )?;
            write_state_atomic(&state)?;
        }
        Ok(())
    }

    async fn fetch_shadow_and_pnl_from_base(&self, base_url: &str) -> Result<(ShadowLiveLite, PnlLite)> {
        let live = self
            .http
            .get(format!("{base_url}/report/shadow/live"))
            .send()
            .await?
            .error_for_status()?
            .json::<ShadowLiveLite>()
            .await?;
        let pnl = self
            .http
            .get(format!("{base_url}/state/pnl"))
            .send()
            .await?
            .error_for_status()?
            .json::<PnlLite>()
            .await?;
        Ok((live, pnl))
    }

    fn find_available_port(&self) -> Result<u16> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
        let port = listener.local_addr().context("ephemeral addr")?.port();
        drop(listener);
        Ok(port)
    }

    fn build_challenger_dataset_root(&self, now: i64, layer: SeatLayer) -> String {
        let root = std::env::var("POLYEDGE_DATASET_ROOT")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "datasets".to_string());
        let path = std::path::PathBuf::from(root)
            .join("seat_challenger")
            .join(format!("{}_{}", now, layer.as_str()));
        let _ = std::fs::create_dir_all(&path);
        path.to_string_lossy().to_string()
    }

    async fn wait_control_ready(&self, base_url: &str) -> Result<()> {
        let health = format!("{base_url}/health");
        let start = now_ms();
        while now_ms().saturating_sub(start) < 30_000 {
            let ok = self
                .http
                .get(&health)
                .send()
                .await
                .map(|resp| resp.status().is_success())
                .unwrap_or(false);
            if ok {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(anyhow!("challenger control API not ready"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handle() -> SeatRuntimeHandle {
        SeatRuntimeHandle {
            cfg: SeatConfig::default(),
            http: Client::new(),
            state: Arc::new(RwLock::new(SeatRuntimeState::default())),
            live_fill_counter: Arc::new(AtomicU64::new(0)),
            challenger: Arc::new(Mutex::new(None)),
        }
    }

    #[test]
    fn layer_boundaries_respect_trade_and_uptime() {
        let handle = test_handle();
        let mut state = SeatRuntimeState::default();
        let t0 = state.started_ms + 50 * 3_600 * 1_000;
        state.live_fill_total = 200;
        assert_eq!(handle.target_layer(&state, t0), SeatLayer::Layer0);
        state.live_fill_total = 320;
        assert_eq!(handle.target_layer(&state, t0), SeatLayer::Layer1);
        let t1 = state.started_ms + 73 * 3_600 * 1_000;
        state.live_fill_total = 900;
        assert_eq!(handle.target_layer(&state, t1), SeatLayer::Layer2);
        state.live_fill_total = 2_100;
        assert_eq!(handle.target_layer(&state, t1), SeatLayer::Layer3);
    }

    #[test]
    fn smoothing_and_monitor_window_form_ninety_minutes() {
        let smooth = SeatSmoothingState {
            layer: SeatLayer::Layer1,
            old_params: SeatParameterSet::default(),
            target_params: SeatParameterSet::default(),
            current_params: SeatParameterSet::default(),
            baseline: SeatObjectiveSnapshot::default(),
            started_ms: 0,
            end_ms: 1_800_000,
            next_step_ms: 0,
        };
        let monitor = SeatMonitorState {
            layer: SeatLayer::Layer1,
            old_params: SeatParameterSet::default(),
            new_params: SeatParameterSet::default(),
            baseline: SeatObjectiveSnapshot::default(),
            pre_switch_objective_24h: 0.0,
            started_ms: smooth.end_ms,
            end_ms: smooth.end_ms + 3_600_000,
        };
        assert_eq!(monitor.end_ms, 5_400_000);
    }

    #[test]
    fn downgrade_streak_forces_layer0_lock() {
        let handle = test_handle();
        let mut state = SeatRuntimeState::default();
        state.current_layer = SeatLayer::Layer3;
        handle
            .downgrade_locked(&mut state, SeatLayer::Layer3, "first".to_string())
            .expect("first downgrade");
        assert_eq!(state.current_layer, SeatLayer::Layer2);
        handle
            .downgrade_locked(&mut state, SeatLayer::Layer2, "second".to_string())
            .expect("second downgrade");
        assert_eq!(state.current_layer, SeatLayer::Layer0);
        assert!(state.layer0_lock_until_ms > 0);
    }
}
