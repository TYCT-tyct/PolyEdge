use std::fs;
use std::path::{Path, PathBuf};

use fair_value::BasisMrConfig;
use risk_engine::RiskLimits;
use serde::{Deserialize, Serialize};
use strategy_maker::MakerConfig;

use crate::seat_types::SeatConfig;
use crate::state::{
    EdgeModelConfig, ExecutionConfig, ExitConfig, FusionConfig, PerfProfile, PredatorCConfig,
    PredatorCPriority, SettlementConfig, SourceHealthConfig,
};

fn strategy_config_path() -> PathBuf {
    std::env::var("POLYEDGE_STRATEGY_CONFIG_PATH")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("configs/strategy.toml"))
}

pub(super) fn load_fair_value_config() -> BasisMrConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        return BasisMrConfig::default();
    };

    let mut cfg = BasisMrConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[fair_value.basis_mr]";
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "enabled" => cfg.enabled = val.parse().unwrap(),
            "alpha_mean" => cfg.alpha_mean = val.parse::<f64>().unwrap().max(0.0),
            "alpha_var" => cfg.alpha_var = val.parse::<f64>().unwrap().max(0.0),
            "alpha_ret" => cfg.alpha_ret = val.parse::<f64>().unwrap().max(0.0),
            "alpha_vol" => cfg.alpha_vol = val.parse::<f64>().unwrap().max(0.0),
            "k_revert" => cfg.k_revert = val.parse::<f64>().unwrap().max(0.0),
            "z_cap" => cfg.z_cap = val.parse::<f64>().unwrap().max(0.5),
            "min_confidence" => cfg.min_confidence = val.parse::<f64>().unwrap().max(0.0),
            "warmup_ticks" => cfg.warmup_ticks = val.parse::<usize>().unwrap().max(1),
            _ => {}
        }
    }
    cfg
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct UniverseConfig {
    pub(super) assets: Vec<String>,
    pub(super) market_types: Vec<String>,
    pub(super) timeframes: Vec<String>,
    pub(super) tier_whitelist: Vec<String>,
    pub(super) tier_blacklist: Vec<String>,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            assets: vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "SOLUSDT".to_string(),
                "XRPUSDT".to_string(),
            ],
            market_types: vec![
                "updown".to_string(),
                "above_below".to_string(),
                "range".to_string(),
            ],
            timeframes: vec![
                "5m".to_string(),
                "15m".to_string(),
                "1h".to_string(),
                "1d".to_string(),
            ],
            tier_whitelist: Vec::new(),
            tier_blacklist: Vec::new(),
        }
    }
}

fn parse_toml_array_of_strings(val: &str) -> Vec<String> {
    let trimmed = val.trim();
    if !(trimmed.starts_with('[') && trimmed.ends_with(']')) {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    inner
        .split(',')
        .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
}

#[cfg(test)]
pub(crate) fn parse_toml_array_for_key(raw: &str, key: &str) -> Option<Vec<String>> {
    let Ok(value) = toml::from_str::<toml::Value>(raw) else {
        return None;
    };
    let arr = value.get(key)?.as_array()?;
    let parsed = arr
        .iter()
        .filter_map(|v| v.as_str().map(ToString::to_string))
        .collect::<Vec<_>>();
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

fn parse_gatling_symbol_section(section: &str) -> Option<String> {
    const PREFIX: &str = "[predator_c.gatling_symbols.";
    if !(section.starts_with(PREFIX) && section.ends_with(']')) {
        return None;
    }
    let inner = &section[PREFIX.len()..section.len() - 1];
    let symbol = inner.trim();
    if symbol.is_empty() {
        None
    } else {
        Some(symbol.to_string())
    }
}

pub(super) fn load_strategy_config() -> MakerConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        return MakerConfig::default();
    };
    let mut cfg = MakerConfig::default();
    let mut in_maker = false;
    let mut in_taker = false;
    let mut in_online = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_maker = line == "[maker]";
            in_taker = line == "[taker]";
            in_online = line == "[online_calibration]";
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        if in_maker {
            match key {
                "base_quote_size" => cfg.base_quote_size = val.parse::<f64>().unwrap().max(0.01),
                "min_edge_bps" => cfg.min_edge_bps = val.parse::<f64>().unwrap().max(0.0),
                "inventory_skew" => cfg.inventory_skew = val.parse::<f64>().unwrap().clamp(0.0, 1.0),
                "max_spread" => cfg.max_spread = val.parse::<f64>().unwrap().max(0.0001),
                "ttl_ms" => cfg.ttl_ms = val.parse::<u64>().unwrap().max(50),
                _ => {}
            }
        } else if in_taker {
            match key {
                "trigger_bps" => cfg.taker_trigger_bps = val.parse::<f64>().unwrap().max(0.0),
                "max_slippage_bps" => cfg.taker_max_slippage_bps = val.parse::<f64>().unwrap().max(0.0),
                "stale_tick_filter_ms" => cfg.stale_tick_filter_ms = val.parse::<f64>().unwrap().clamp(50.0, 5_000.0),
                "market_tier_profile" => cfg.market_tier_profile = val.to_string(),
                _ => {}
            }
        } else if in_online {
            match key {
                "capital_fraction_kelly" => cfg.capital_fraction_kelly = val.parse::<f64>().unwrap().clamp(0.01, 1.0),
                "variance_penalty_lambda" => cfg.variance_penalty_lambda = val.parse::<f64>().unwrap().clamp(0.0, 5.0),
                "min_eval_notional_usdc" => cfg.min_eval_notional_usdc = val.parse::<f64>().unwrap().max(0.0),
                "min_expected_edge_usdc" => cfg.min_expected_edge_usdc = val.parse::<f64>().unwrap().max(0.0),
                _ => {}
            }
        }
    }
    cfg
}

pub(super) fn load_fusion_config() -> FusionConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        let cfg = FusionConfig::default();
        std::env::set_var(
            "POLYEDGE_UDP_LOCAL_ONLY",
            if cfg.udp_local_only { "true" } else { "false" },
        );
        return cfg;
    };

    #[derive(Default)]
    struct FusionPatch {
        enable_udp: Option<bool>,
        mode: Option<String>,
        udp_port: Option<u16>,
        dedupe_window_ms: Option<i64>,
        dedupe_price_bps: Option<f64>,
        udp_share_cap: Option<f64>,
        jitter_threshold_ms: Option<f64>,
        fallback_arm_duration_ms: Option<u64>,
        fallback_cooldown_sec: Option<u64>,
        udp_local_only: Option<bool>,
    }

    impl FusionPatch {
        fn apply_to(self, cfg: &mut FusionConfig) {
            if let Some(v) = self.enable_udp {
                cfg.enable_udp = v;
            }
            if let Some(v) = self.mode {
                cfg.mode = v;
            }
            if let Some(v) = self.udp_port {
                cfg.udp_port = v.max(1);
            }
            if let Some(v) = self.dedupe_window_ms {
                cfg.dedupe_window_ms = v.clamp(0, 2_000);
            }
            if let Some(v) = self.dedupe_price_bps {
                cfg.dedupe_price_bps = v.clamp(0.0, 50.0);
            }
            if let Some(v) = self.udp_share_cap {
                cfg.udp_share_cap = v.clamp(0.05, 0.95);
            }
            if let Some(v) = self.jitter_threshold_ms {
                cfg.jitter_threshold_ms = v.clamp(1.0, 2_000.0);
            }
            if let Some(v) = self.fallback_arm_duration_ms {
                cfg.fallback_arm_duration_ms = v.clamp(200, 15_000);
            }
            if let Some(v) = self.fallback_cooldown_sec {
                cfg.fallback_cooldown_sec = v.clamp(0, 3_600);
            }
            if let Some(v) = self.udp_local_only {
                cfg.udp_local_only = v;
            }
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Section {
        None,
        Fusion,
        Transport,
    }

    let mut cfg = FusionConfig::default();
    let mut fusion_patch = FusionPatch::default();
    let mut transport_patch = FusionPatch::default();
    let mut section = Section::None;
    let mut saw_fusion = false;
    let mut saw_transport = false;

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = match line {
                "[fusion]" => {
                    saw_fusion = true;
                    Section::Fusion
                }
                "[transport]" => {
                    saw_transport = true;
                    Section::Transport
                }
                _ => Section::None,
            };
            continue;
        }
        if section == Section::None {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        let target = if section == Section::Transport {
            &mut transport_patch
        } else {
            &mut fusion_patch
        };
        match key {
            "enable_udp" => {
                if let Ok(parsed) = val.parse::<bool>() {
                    target.enable_udp = Some(parsed);
                }
            }
            "mode" => {
                let norm = val.to_ascii_lowercase();
                if matches!(
                    norm.as_str(),
                    "active_active" | "direct_only" | "hyper_mesh"
                ) {
                    target.mode = Some(norm);
                }
            }
            "udp_port" => {
                if let Ok(parsed) = val.parse::<u16>() {
                    target.udp_port = Some(parsed.max(1));
                }
            }
            "dedupe_window_ms" => {
                if let Ok(parsed) = val.parse::<i64>() {
                    target.dedupe_window_ms = Some(parsed.clamp(0, 2_000));
                }
            }
            "dedupe_price_bps" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    target.dedupe_price_bps = Some(parsed.clamp(0.0, 50.0));
                }
            }
            "udp_share_cap" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    target.udp_share_cap = Some(parsed.clamp(0.05, 0.95));
                }
            }
            "jitter_threshold_ms" => {
                if let Ok(parsed) = val.parse::<f64>() {
                    target.jitter_threshold_ms = Some(parsed.clamp(1.0, 2_000.0));
                }
            }
            "fallback_arm_duration_ms" => {
                if let Ok(parsed) = val.parse::<u64>() {
                    target.fallback_arm_duration_ms = Some(parsed.clamp(200, 15_000));
                }
            }
            "fallback_cooldown_sec" => {
                if let Ok(parsed) = val.parse::<u64>() {
                    target.fallback_cooldown_sec = Some(parsed.clamp(0, 3_600));
                }
            }
            "udp_local_only" => {
                if let Ok(parsed) = val.parse::<bool>() {
                    target.udp_local_only = Some(parsed);
                }
            }
            _ => {}
        }
    }

    fusion_patch.apply_to(&mut cfg);
    if saw_transport {
        transport_patch.apply_to(&mut cfg);
    }

    if let Ok(raw) = std::env::var("POLYEDGE_UDP_PORT") {
        if let Ok(parsed) = raw.parse::<u16>() {
            cfg.udp_port = parsed.max(1);
        }
    }
    if saw_fusion && saw_transport {
        tracing::warn!(
            "both [fusion] and [transport] are present; [transport] now takes precedence"
        );
    }
    std::env::set_var(
        "POLYEDGE_UDP_LOCAL_ONLY",
        if cfg.udp_local_only { "true" } else { "false" },
    );
    cfg
}

pub(super) fn load_source_health_config() -> SourceHealthConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        return SourceHealthConfig::default();
    };
    let mut cfg = SourceHealthConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[source_health]";
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "min_samples" => cfg.min_samples = val.parse::<u64>().unwrap().max(1),
            "gap_window_ms" => cfg.gap_window_ms = val.parse::<i64>().unwrap().clamp(50, 60_000),
            "jitter_limit_ms" => cfg.jitter_limit_ms = val.parse::<f64>().unwrap().clamp(0.1, 2_000.0),
            "deviation_limit_bps" => cfg.deviation_limit_bps = val.parse::<f64>().unwrap().clamp(0.1, 10_000.0),
            "freshness_limit_ms" => cfg.freshness_limit_ms = val.parse::<f64>().unwrap().clamp(50.0, 60_000.0),
            "min_score_for_trading" => cfg.min_score_for_trading = val.parse::<f64>().unwrap().clamp(0.0, 1.0),
            _ => {}
        }
    }
    cfg
}

pub(super) fn load_edge_model_config() -> EdgeModelConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        return EdgeModelConfig::default();
    };
    let mut cfg = EdgeModelConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[edge_model]";
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "model" => cfg.model = val.to_string(),
            "gate_mode" => cfg.gate_mode = val.to_string(),
            "version" => cfg.version = val.to_string(),
            "base_gate_bps" => cfg.base_gate_bps = val.parse::<f64>().unwrap().max(0.0),
            "congestion_penalty_bps" => cfg.congestion_penalty_bps = val.parse::<f64>().unwrap().max(0.0),
            "latency_penalty_bps" => cfg.latency_penalty_bps = val.parse::<f64>().unwrap().max(0.0),
            "fail_cost_bps" => cfg.fail_cost_bps = val.parse::<f64>().unwrap().max(0.0),
            _ => {}
        }
    }
    cfg
}

pub(super) fn load_exit_config() -> ExitConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        return ExitConfig::default();
    };
    let mut cfg = ExitConfig::default();
    let mut in_section = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line == "[exit]" || line == "[predator_c.exit]";
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');
        match key {
            "enabled" => cfg.enabled = val.parse::<bool>().unwrap(),
            "t300ms_reversal_bps" => cfg.t300ms_reversal_bps = val.parse::<f64>().unwrap(),
            "t100ms_reversal_bps" => cfg.t100ms_reversal_bps = val.parse::<f64>().unwrap(),
            "convergence_exit_ratio" => cfg.convergence_exit_ratio = val.parse::<f64>().unwrap().clamp(0.0, 1.0),
            "time_stop_ms" => cfg.time_stop_ms = val.parse::<u64>().unwrap().clamp(50, 600_000),
            "edge_decay_bps" => cfg.edge_decay_bps = val.parse::<f64>().unwrap(),
            "adverse_move_bps" => cfg.adverse_move_bps = val.parse::<f64>().unwrap(),
            "flatten_on_trigger" => cfg.flatten_on_trigger = val.parse::<bool>().unwrap(),
            "t3_take_ratio" => cfg.t3_take_ratio = val.parse::<f64>().unwrap().clamp(0.0, 5.0),
            "t15_min_unrealized_usdc" => cfg.t15_min_unrealized_usdc = val.parse::<f64>().unwrap(),
            "t60_true_prob_floor" => cfg.t60_true_prob_floor = val.parse::<f64>().unwrap().clamp(0.0, 1.0),
            "t300_force_exit_ms" => cfg.t300_force_exit_ms = val.parse::<u64>().unwrap().clamp(1_000, 1_800_000),
            "t300_hold_prob_threshold" => cfg.t300_hold_prob_threshold = val.parse::<f64>().unwrap().clamp(0.0, 1.0),
            "t300_hold_time_to_expiry_ms" => cfg.t300_hold_time_to_expiry_ms = val.parse::<u64>().unwrap().clamp(1_000, 1_800_000),
            "max_single_trade_loss_usdc" => cfg.max_single_trade_loss_usdc = val.parse::<f64>().unwrap().max(0.0),
            _ => {}
        }
    }
    cfg
}

pub(super) fn load_predator_c_config() -> PredatorCConfig {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        return PredatorCConfig::default();
    };

    let mut cfg = PredatorCConfig::default();

    let mut in_root = false;
    let mut in_dir = false;
    let mut in_prob = false;
    let mut in_sniper = false;
    let mut in_gatling = false;
    let mut in_strategy_d = false;
    let mut in_regime = false;
    let mut in_cross = false;
    let mut in_router = false;
    let mut in_compounder = false;
    let mut in_v52_time_phase = false;
    let mut in_v52_execution = false;
    let mut in_v52_dual_arb = false;
    let mut in_v52_reversal = false;
    let mut gatling_symbol_section: Option<String> = None;

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            gatling_symbol_section = parse_gatling_symbol_section(line);
            in_root = line == "[predator_c]";
            in_dir = line == "[predator_c.direction_detector]";
            in_prob = line == "[predator_c.probability_engine]";
            in_sniper = line == "[predator_c.taker_sniper]";
            in_gatling = line == "[predator_c.gatling]";
            in_strategy_d = line == "[predator_c.strategy_d]";
            in_regime = line == "[predator_c.regime]";
            in_cross = line == "[predator_c.cross_symbol]";
            in_router = line == "[predator_c.router]";
            in_compounder = line == "[predator_c.compounder]";
            in_v52_time_phase = line == "[v52.time_phase]" || line == "[predator_c.v52.time_phase]";
            in_v52_execution = line == "[v52.execution]" || line == "[predator_c.v52.execution]";
            in_v52_dual_arb = line == "[v52.dual_arb]" || line == "[predator_c.v52.dual_arb]";
            in_v52_reversal = line == "[v52.reversal]" || line == "[predator_c.v52.reversal]";
            continue;
        }
        if !(in_root
            || in_dir
            || in_prob
            || in_sniper
            || in_gatling
            || in_strategy_d
            || in_regime
            || in_cross
            || in_router
            || in_compounder
            || in_v52_time_phase
            || in_v52_execution
            || in_v52_dual_arb
            || in_v52_reversal
            || gatling_symbol_section.is_some())
        {
            continue;
        }

        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');

        if in_root {
            match key {
                "enabled" => cfg.enabled = val.parse::<bool>().unwrap(),
                "priority" => {
                    let norm = val.trim().to_ascii_lowercase();
                    cfg.priority = match norm.as_str() {
                        "maker_first" => PredatorCPriority::MakerFirst,
                        "taker_first" => PredatorCPriority::TakerFirst,
                        "taker_only" => PredatorCPriority::TakerOnly,
                        _ => cfg.priority.clone(),
                    };
                }
                _ => {}
            }
            continue;
        }

        if in_dir {
            match key {
                "window_max_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.direction_detector.window_max_sec = parsed.max(10);
                    }
                }
                "threshold_5m_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_5m_pct = parsed.max(0.0);
                    }
                }
                "threshold_15m_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_15m_pct = parsed.max(0.0);
                    }
                }
                "threshold_1h_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_1h_pct = parsed.max(0.0);
                    }
                }
                "threshold_1d_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.threshold_1d_pct = parsed.max(0.0);
                    }
                }
                "lookback_short_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.direction_detector.lookback_short_sec = parsed.max(1);
                    }
                }
                "lookback_long_sec" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.direction_detector.lookback_long_sec = parsed.max(1);
                    }
                }
                "min_sources_for_high_confidence" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.direction_detector.min_sources_for_high_confidence = parsed.max(1);
                    }
                }
                "min_ticks_for_signal" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.direction_detector.min_ticks_for_signal = parsed.max(1);
                    }
                }
                "min_consecutive_ticks" => {
                    if let Ok(parsed) = val.parse::<u8>() {
                        cfg.direction_detector.min_consecutive_ticks = parsed.max(1);
                    }
                }
                "min_velocity_bps_per_sec" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.min_velocity_bps_per_sec = parsed.max(0.0);
                    }
                }
                "min_acceleration" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.min_acceleration = parsed.max(0.0);
                    }
                }
                "momentum_spike_multiplier" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.momentum_spike_multiplier = parsed.max(1.0);
                    }
                }
                "min_tick_rate_spike_ratio" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.direction_detector.min_tick_rate_spike_ratio = parsed.max(1.0);
                    }
                }
                "tick_rate_short_ms" => {
                    if let Ok(parsed) = val.parse::<i64>() {
                        cfg.direction_detector.tick_rate_short_ms = parsed.clamp(50, 10_000);
                    }
                }
                "tick_rate_long_ms" => {
                    if let Ok(parsed) = val.parse::<i64>() {
                        cfg.direction_detector.tick_rate_long_ms = parsed.clamp(100, 60_000);
                    }
                }
                "enable_source_vote_gate" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.direction_detector.enable_source_vote_gate = parsed;
                    }
                }
                "require_secondary_confirmation" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.direction_detector.require_secondary_confirmation = parsed;
                    }
                }
                "source_vote_max_age_ms" => {
                    if let Ok(parsed) = val.parse::<i64>() {
                        cfg.direction_detector.source_vote_max_age_ms = parsed.clamp(50, 60_000);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_prob {
            match key {
                "momentum_gain" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.momentum_gain = parsed.clamp(0.0, 20.0);
                    }
                }
                "lag_penalty_per_ms" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.lag_penalty_per_ms = parsed.clamp(0.0, 0.1);
                    }
                }
                "confidence_floor" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.confidence_floor = parsed.clamp(0.0, 1.0);
                    }
                }
                "sigma_annual" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.sigma_annual = parsed.clamp(0.05, 5.0);
                    }
                }
                "horizon_sec" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.horizon_sec = parsed.clamp(1.0, 900.0);
                    }
                }
                "drift_annual" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.drift_annual = parsed.clamp(-10.0, 10.0);
                    }
                }
                "velocity_drift_gain" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.velocity_drift_gain = parsed.clamp(0.0, 5.0);
                    }
                }
                "acceleration_drift_gain" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.acceleration_drift_gain = parsed.clamp(0.0, 5.0);
                    }
                }
                "fair_blend_weight" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.probability_engine.fair_blend_weight = parsed.clamp(0.0, 1.0);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_sniper || in_gatling {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.taker_sniper.gatling_enabled = parsed;
                    }
                }
                "min_direction_confidence" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.min_direction_confidence = parsed.clamp(0.0, 1.0);
                    }
                }
                "min_edge_net_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.min_edge_net_bps = parsed.max(0.0);
                    }
                }
                "max_spread" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.max_spread = parsed.max(0.0001);
                    }
                }
                "cooldown_ms_per_market" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.taker_sniper.cooldown_ms_per_market = parsed;
                    }
                }
                "gatling_enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.taker_sniper.gatling_enabled = parsed;
                    }
                }
                "chunk_notional_usdc" | "gatling_chunk_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.taker_sniper.gatling_chunk_notional_usdc = parsed.max(0.1);
                    }
                }
                "min_chunks" | "gatling_min_chunks" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.taker_sniper.gatling_min_chunks = parsed.max(1);
                    }
                }
                "max_chunks" | "gatling_max_chunks" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.taker_sniper.gatling_max_chunks = parsed.max(1);
                    }
                }
                "spacing_ms" | "gatling_spacing_ms" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.taker_sniper.gatling_spacing_ms = parsed.min(250);
                    }
                }
                "stop_on_reject" | "gatling_stop_on_reject" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.taker_sniper.gatling_stop_on_reject = parsed;
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_strategy_d {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.strategy_d.enabled = parsed;
                    }
                }
                "min_gap_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.strategy_d.min_gap_bps = parsed.max(0.0);
                    }
                }
                "min_edge_net_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.strategy_d.min_edge_net_bps = parsed.max(0.0);
                    }
                }
                "min_confidence" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.strategy_d.min_confidence = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.strategy_d.max_notional_usdc = parsed.max(0.0);
                    }
                }
                "cooldown_ms_per_market" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.strategy_d.cooldown_ms_per_market = parsed;
                    }
                }
                _ => {}
            }
            continue;
        }

        if let Some(symbol) = gatling_symbol_section.as_deref() {
            let entry = cfg
                .taker_sniper
                .gatling_by_symbol
                .entry(symbol.to_ascii_uppercase())
                .or_default();
            match key {
                "enabled" | "gatling_enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        entry.enabled = Some(parsed);
                    }
                }
                "chunk_notional_usdc" | "gatling_chunk_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        entry.chunk_notional_usdc = Some(parsed.max(0.01));
                    }
                }
                "min_chunks" | "gatling_min_chunks" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        entry.min_chunks = Some(parsed.max(1));
                    }
                }
                "max_chunks" | "gatling_max_chunks" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        entry.max_chunks = Some(parsed.max(1));
                    }
                }
                "spacing_ms" | "gatling_spacing_ms" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        entry.spacing_ms = Some(parsed.min(1_000));
                    }
                }
                "stop_on_reject" | "gatling_stop_on_reject" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        entry.stop_on_reject = Some(parsed);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_regime {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.regime.enabled = parsed;
                    }
                }
                "active_min_confidence" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.regime.active_min_confidence = parsed.clamp(0.0, 1.0);
                    }
                }
                "active_min_magnitude_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.regime.active_min_magnitude_pct = parsed.max(0.0);
                    }
                }
                "defend_tox_score" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.regime.defend_tox_score = parsed.clamp(0.0, 1.0);
                    }
                }
                "defend_on_toxic_danger" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.regime.defend_on_toxic_danger = parsed;
                    }
                }
                "defend_min_source_health" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.regime.defend_min_source_health = parsed.clamp(0.0, 1.0);
                    }
                }
                "quiet_min_edge_multiplier" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.regime.quiet_min_edge_multiplier = parsed.clamp(1.0, 10.0);
                    }
                }
                "quiet_chunk_scale" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.regime.quiet_chunk_scale = parsed.clamp(0.05, 1.0);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_cross {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.cross_symbol.enabled = parsed;
                    }
                }
                "leader_symbol" => {
                    cfg.cross_symbol.leader_symbol = val.trim().to_string();
                }
                "follower_symbols" => {
                    let parsed = parse_toml_array_of_strings(v.trim());
                    if !parsed.is_empty() {
                        cfg.cross_symbol.follower_symbols = parsed;
                    }
                }
                "min_leader_confidence" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.cross_symbol.min_leader_confidence = parsed.clamp(0.0, 1.0);
                    }
                }
                "min_leader_magnitude_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.cross_symbol.min_leader_magnitude_pct = parsed.max(0.0);
                    }
                }
                "follower_stale_confidence_max" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.cross_symbol.follower_stale_confidence_max = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_correlated_positions" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.cross_symbol.max_correlated_positions = parsed.max(1);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_router {
            match key {
                "max_locked_pct_5m" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_5m = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_locked_pct_15m" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_15m = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_locked_pct_1h" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_1h = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_locked_pct_1d" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_locked_pct_1d = parsed.clamp(0.0, 1.0);
                    }
                }
                "max_concurrent_positions" => {
                    if let Ok(parsed) = val.parse::<usize>() {
                        cfg.router.max_concurrent_positions = parsed.max(1);
                    }
                }
                "liquidity_reserve_pct" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.liquidity_reserve_pct = parsed.clamp(0.0, 0.95);
                    }
                }
                "max_order_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_order_notional_usdc = parsed.max(0.0);
                    }
                }
                "max_total_notional_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.router.max_total_notional_usdc = parsed.max(0.0);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_compounder {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.compounder.enabled = parsed;
                    }
                }
                "initial_capital_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.initial_capital_usdc = parsed.max(0.0);
                    }
                }
                "compound_ratio" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.compound_ratio = parsed.clamp(0.0, 1.0);
                    }
                }
                "position_fraction" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.position_fraction = parsed.clamp(0.0, 1.0);
                    }
                }
                "min_quote_size" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.min_quote_size = parsed.max(0.0);
                    }
                }
                "daily_loss_cap_usdc" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.compounder.daily_loss_cap_usdc = parsed.max(0.0);
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_v52_time_phase {
            match key {
                "early_min_ratio" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.time_phase.early_min_ratio = parsed;
                    }
                }
                "late_max_ratio" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.time_phase.late_max_ratio = parsed;
                    }
                }
                "early_size_scale" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.time_phase.early_size_scale = parsed;
                    }
                }
                "maturity_size_scale" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.time_phase.maturity_size_scale = parsed;
                    }
                }
                "late_size_scale" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.time_phase.late_size_scale = parsed;
                    }
                }
                "allow_timeframes" => {
                    let parsed = parse_toml_array_of_strings(v.trim())
                        .into_iter()
                        .map(|s| s.to_ascii_lowercase())
                        .collect::<Vec<_>>();
                    if !parsed.is_empty() {
                        cfg.v52.time_phase.allow_timeframes = parsed;
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_v52_execution {
            match key {
                "late_force_taker_remaining_ms" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.v52.execution.late_force_taker_remaining_ms = parsed;
                    }
                }
                "maker_wait_ms_before_force" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.v52.execution.maker_wait_ms_before_force = parsed;
                    }
                }
                "apply_force_taker_in_maturity" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.v52.execution.apply_force_taker_in_maturity = parsed;
                    }
                }
                "apply_force_taker_in_late" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.v52.execution.apply_force_taker_in_late = parsed;
                    }
                }
                "alpha_window_enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.v52.execution.alpha_window_enabled = parsed;
                    }
                }
                "alpha_window_move_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.execution.alpha_window_move_bps = parsed;
                    }
                }
                "alpha_window_poll_ms" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.v52.execution.alpha_window_poll_ms = parsed;
                    }
                }
                "alpha_window_max_wait_ms" => {
                    if let Ok(parsed) = val.parse::<u64>() {
                        cfg.v52.execution.alpha_window_max_wait_ms = parsed;
                    }
                }
                "require_compounder_when_live" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.v52.execution.require_compounder_when_live = parsed;
                    }
                }
                _ => {}
            }
            continue;
        }

        if in_v52_dual_arb {
            match key {
                "enabled" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.v52.dual_arb.enabled = parsed;
                    }
                }
                "safety_margin_bps" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.dual_arb.safety_margin_bps = parsed;
                    }
                }
                "threshold" => {
                    if let Ok(parsed) = val.parse::<f64>() {
                        cfg.v52.dual_arb.threshold = parsed;
                    }
                }
                "fee_buffer_mode" => {
                    cfg.v52.dual_arb.fee_buffer_mode = val.trim().to_ascii_lowercase();
                }
                _ => {}
            }
            continue;
        }

        if in_v52_reversal {
            match key {
                "same_market_opposite_first" => {
                    if let Ok(parsed) = val.parse::<bool>() {
                        cfg.v52.reversal.same_market_opposite_first = parsed;
                    }
                }
                _ => {}
            }
            continue;
        }
    }

    cfg.v52.time_phase.early_min_ratio = cfg.v52.time_phase.early_min_ratio.clamp(0.11, 0.99);
    cfg.v52.time_phase.late_max_ratio = cfg.v52.time_phase.late_max_ratio.clamp(0.01, 0.54);
    cfg.v52.time_phase.early_size_scale = cfg.v52.time_phase.early_size_scale.clamp(0.10, 5.0);
    cfg.v52.time_phase.maturity_size_scale =
        cfg.v52.time_phase.maturity_size_scale.clamp(0.10, 5.0);
    cfg.v52.time_phase.late_size_scale = cfg.v52.time_phase.late_size_scale.clamp(0.10, 5.0);
    if cfg.v52.time_phase.late_max_ratio >= cfg.v52.time_phase.early_min_ratio {
        cfg.v52.time_phase.late_max_ratio = 0.10;
        cfg.v52.time_phase.early_min_ratio = 0.55;
    }
    cfg.v52.time_phase.allow_timeframes = cfg
        .v52
        .time_phase
        .allow_timeframes
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .filter(|s| s == "5m" || s == "15m")
        .collect::<Vec<_>>();
    if cfg.v52.time_phase.allow_timeframes.is_empty() {
        cfg.v52.time_phase.allow_timeframes = vec!["5m".to_string(), "15m".to_string()];
    }
    cfg.v52.execution.late_force_taker_remaining_ms = cfg
        .v52
        .execution
        .late_force_taker_remaining_ms
        .clamp(1_000, 60_000);
    cfg.v52.execution.maker_wait_ms_before_force = cfg
        .v52
        .execution
        .maker_wait_ms_before_force
        .clamp(50, 10_000);
    cfg.v52.execution.alpha_window_move_bps =
        cfg.v52.execution.alpha_window_move_bps.clamp(0.1, 50.0);
    cfg.v52.execution.alpha_window_poll_ms = cfg.v52.execution.alpha_window_poll_ms.clamp(1, 200);
    cfg.v52.execution.alpha_window_max_wait_ms =
        cfg.v52.execution.alpha_window_max_wait_ms.clamp(50, 5_000);
    cfg.v52.dual_arb.safety_margin_bps = cfg.v52.dual_arb.safety_margin_bps.clamp(0.0, 100.0);
    cfg.v52.dual_arb.threshold = cfg.v52.dual_arb.threshold.clamp(0.50, 1.10);
    if cfg.v52.dual_arb.fee_buffer_mode != "conservative_taker" {
        cfg.v52.dual_arb.fee_buffer_mode = "conservative_taker".to_string();
    }

    cfg
}

pub(super) fn load_risk_limits_config() -> RiskLimits {
    let path = strategy_config_path();
    let Ok(raw) = fs::read_to_string(path) else {
        println!("Warn: strategy.toml not found for risk config, using defaults");
        return RiskLimits::default();
    };

    // Conservative fallback values when parsing is partial or malformed.
    // Keep defaults via struct update syntax to avoid piecemeal re-assignment.
    let mut cfg = RiskLimits {
        max_drawdown_pct: 0.20,
        max_asset_notional: 50.0,
        max_market_notional: 10.0,
        ..RiskLimits::default()
    };

    let mut section = "";
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line;
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let val = v.trim().trim_matches('"');

        match section {
            "[risk_controls.exposure_limits]" => match key {
                "max_total_exposure_usdc" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.max_asset_notional = p.max(0.0);
                    }
                }
                "max_per_market_exposure_usdc" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.max_market_notional = p.max(0.0);
                    }
                }
                "max_concurrent_positions" => {
                    if let Ok(p) = val.parse::<usize>() {
                        cfg.max_open_orders = p.max(1);
                    }
                }
                _ => {}
            },
            "[risk_controls.kill_switch]" => match key {
                "max_drawdown_pct" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.max_drawdown_pct = p.clamp(0.001, 1.0);
                    }
                }
                "max_loss_streak" => {
                    if let Ok(p) = val.parse::<u32>() {
                        cfg.max_loss_streak = p.max(1);
                    }
                }
                "cooldown_sec" => {
                    if let Ok(p) = val.parse::<u64>() {
                        cfg.cooldown_sec = p.max(1);
                    }
                }
                _ => {}
            },
            "[risk_controls.progressive_limits]" => match key {
                "enabled" => {
                    if let Ok(p) = val.parse::<bool>() {
                        cfg.progressive_enabled = p;
                    }
                }
                "drawdown_tier1_ratio" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.drawdown_tier1_ratio = p.clamp(0.05, 0.99);
                    }
                }
                "drawdown_tier2_ratio" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.drawdown_tier2_ratio = p.clamp(cfg.drawdown_tier1_ratio, 0.999);
                    }
                }
                "tier1_size_scale" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.tier1_size_scale = p.clamp(0.01, 1.0);
                    }
                }
                "tier2_size_scale" => {
                    if let Ok(p) = val.parse::<f64>() {
                        cfg.tier2_size_scale = p.clamp(0.01, 1.0);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    cfg
}

pub(super) fn load_execution_config() -> ExecutionConfig {
    let path = Path::new("configs/execution.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return ExecutionConfig::default();
    };
    #[derive(Debug, Deserialize, Default)]
    struct ExecutionFile {
        execution: Option<ExecutionSection>,
    }

    #[derive(Debug, Deserialize, Default)]
    struct ExecutionSection {
        mode: Option<String>,
        rate_limit_rps: Option<f64>,
        http_timeout_ms: Option<u64>,
        clob_endpoint: Option<String>,
        order_endpoint: Option<String>,
        order_backup_endpoint: Option<String>,
        order_failover_timeout_ms: Option<u64>,
    }

    let Ok(parsed) = toml::from_str::<ExecutionFile>(&raw) else {
        return ExecutionConfig::default();
    };
    let Some(section) = parsed.execution else {
        return ExecutionConfig::default();
    };
    let mut cfg = ExecutionConfig::default();
    if let Some(v) = section.mode {
        cfg.mode = v;
    }
    if let Some(v) = section.rate_limit_rps {
        cfg.rate_limit_rps = v.max(0.1);
    }
    if let Some(v) = section.http_timeout_ms {
        cfg.http_timeout_ms = v.max(100);
    }
    if let Some(v) = section.clob_endpoint {
        cfg.clob_endpoint = v;
    }
    if let Some(v) = section.order_endpoint {
        cfg.order_endpoint = (!v.trim().is_empty()).then_some(v);
    }
    if let Some(v) = section.order_backup_endpoint {
        cfg.order_backup_endpoint = (!v.trim().is_empty()).then_some(v);
    }
    if let Some(v) = section.order_failover_timeout_ms {
        cfg.order_failover_timeout_ms = v.clamp(10, 5_000);
    }
    cfg
}

pub(super) fn load_settlement_config() -> SettlementConfig {
    let path = Path::new("configs/settlement.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return SettlementConfig::default();
    };
    #[derive(Debug, Deserialize, Default)]
    struct SettlementFile {
        settlement: Option<SettlementSection>,
    }

    #[derive(Debug, Deserialize, Default)]
    struct SettlementSection {
        enabled: Option<bool>,
        endpoint: Option<String>,
        required_for_live: Option<bool>,
        poll_interval_ms: Option<u64>,
        timeout_ms: Option<u64>,
        symbols: Option<Vec<String>>,
    }

    let Ok(parsed) = toml::from_str::<SettlementFile>(&raw) else {
        return SettlementConfig::default();
    };
    let Some(section) = parsed.settlement else {
        return SettlementConfig::default();
    };
    let mut cfg = SettlementConfig::default();
    if let Some(v) = section.enabled {
        cfg.enabled = v;
    }
    if let Some(v) = section.endpoint {
        cfg.endpoint = v;
    }
    if let Some(v) = section.required_for_live {
        cfg.required_for_live = v;
    }
    if let Some(v) = section.poll_interval_ms {
        cfg.poll_interval_ms = v.clamp(250, 10_000);
    }
    if let Some(v) = section.timeout_ms {
        cfg.timeout_ms = v.clamp(100, 5_000);
    }
    if let Some(v) = section.symbols {
        cfg.symbols = v;
    }
    cfg
}

pub(super) fn load_universe_config() -> UniverseConfig {
    let path = Path::new("configs/universe.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return UniverseConfig::default();
    };
    #[derive(Debug, Deserialize, Default)]
    struct UniverseFile {
        assets: Option<Vec<String>>,
        market_types: Option<Vec<String>>,
        timeframes: Option<Vec<String>>,
        tier_whitelist: Option<Vec<String>>,
        tier_blacklist: Option<Vec<String>>,
    }

    let Ok(parsed) = toml::from_str::<UniverseFile>(&raw) else {
        return UniverseConfig::default();
    };
    let mut cfg = UniverseConfig::default();
    if let Some(v) = parsed.assets {
        cfg.assets = v;
    }
    if let Some(v) = parsed.market_types {
        cfg.market_types = v;
    }
    if let Some(v) = parsed.timeframes {
        cfg.timeframes = v;
    }
    if let Some(v) = parsed.tier_whitelist {
        cfg.tier_whitelist = v;
    }
    if let Some(v) = parsed.tier_blacklist {
        cfg.tier_blacklist = v;
    }
    cfg
}

pub(super) fn load_perf_profile_config() -> PerfProfile {
    let path = Path::new("configs/latency.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        return PerfProfile::default();
    };
    #[derive(Debug, Deserialize, Default)]
    struct LatencyFile {
        runtime: Option<RuntimeSection>,
    }

    #[derive(Debug, Deserialize, Default)]
    struct RuntimeSection {
        tail_guard: Option<f64>,
        io_flush_batch: Option<usize>,
        io_queue_capacity: Option<usize>,
        json_mode: Option<String>,
        io_drop_on_full: Option<bool>,
    }

    let Ok(parsed) = toml::from_str::<LatencyFile>(&raw) else {
        return PerfProfile::default();
    };
    let Some(section) = parsed.runtime else {
        return PerfProfile::default();
    };
    let mut cfg = PerfProfile::default();
    if let Some(v) = section.tail_guard {
        cfg.tail_guard = v.clamp(0.50, 0.9999);
    }
    if let Some(v) = section.io_flush_batch {
        cfg.io_flush_batch = v.clamp(1, 4096);
    }
    if let Some(v) = section.io_queue_capacity {
        cfg.io_queue_capacity = v.clamp(256, 262_144);
    }
    if let Some(v) = section.json_mode {
        cfg.json_mode = v;
    }
    if let Some(v) = section.io_drop_on_full {
        cfg.io_drop_on_full = v;
    }
    cfg
}

pub(super) fn load_seat_config() -> SeatConfig {
    let path = Path::new("configs/seat.toml");
    let Ok(raw) = fs::read_to_string(path) else {
        let mut cfg = SeatConfig::default();
        if let Ok(url) = std::env::var("POLYEDGE_SEAT_OPTIMIZER_URL") {
            if !url.trim().is_empty() {
                cfg.optimizer_url = url;
            }
        }
        return cfg;
    };

    #[derive(Debug, Deserialize, Default)]
    struct SeatFile {
        seat: Option<SeatSection>,
    }

    #[derive(Debug, Deserialize, Default)]
    struct SeatSection {
        enabled: Option<bool>,
        control_base_url: Option<String>,
        optimizer_url: Option<String>,
        runtime_tick_sec: Option<u64>,
        activation_check_sec: Option<u64>,
        layer1_interval_sec: Option<u64>,
        layer2_interval_sec: Option<u64>,
        layer3_interval_sec: Option<u64>,
        layer2_shadow_sec: Option<u64>,
        layer3_shadow_sec: Option<u64>,
        smoothing_sec: Option<u64>,
        monitor_sec: Option<u64>,
        rollback_pause_sec: Option<u64>,
        global_pause_sec: Option<u64>,
        layer0_lock_sec: Option<u64>,
        layer1_min_trades: Option<u64>,
        layer2_min_trades: Option<u64>,
        layer2_min_uptime_sec: Option<u64>,
        layer3_min_trades: Option<u64>,
        layer3_min_uptime_sec: Option<u64>,
        black_swan_lock_sec: Option<u64>,
        source_health_floor: Option<f64>,
        history_retention_days: Option<u32>,
        objective_drawdown_penalty: Option<f64>,
    }

    let Ok(parsed) = toml::from_str::<SeatFile>(&raw) else {
        return SeatConfig::default();
    };
    let Some(section) = parsed.seat else {
        return SeatConfig::default();
    };
    let mut cfg = SeatConfig::default();
    if let Some(v) = section.enabled {
        cfg.enabled = v;
    }
    if let Some(v) = section.control_base_url {
        cfg.control_base_url = v;
    }
    if let Some(v) = section.optimizer_url {
        cfg.optimizer_url = v;
    }
    if let Some(v) = section.runtime_tick_sec {
        cfg.runtime_tick_sec = v.clamp(5, 300);
    }
    if let Some(v) = section.activation_check_sec {
        cfg.activation_check_sec = v.clamp(60, 86_400);
    }
    if let Some(v) = section.layer1_interval_sec {
        cfg.layer1_interval_sec = v.clamp(60, 86_400);
    }
    if let Some(v) = section.layer2_interval_sec {
        cfg.layer2_interval_sec = v.clamp(300, 86_400);
    }
    if let Some(v) = section.layer3_interval_sec {
        cfg.layer3_interval_sec = v.clamp(900, 7 * 86_400);
    }
    if let Some(v) = section.layer2_shadow_sec {
        cfg.layer2_shadow_sec = v.clamp(60, 8 * 3_600);
    }
    if let Some(v) = section.layer3_shadow_sec {
        cfg.layer3_shadow_sec = v.clamp(6_000, 12 * 3_600);
    }
    if let Some(v) = section.smoothing_sec {
        cfg.smoothing_sec = v.clamp(60, 12 * 3_600);
    }
    if let Some(v) = section.monitor_sec {
        cfg.monitor_sec = v.clamp(60, 24 * 3_600);
    }
    if let Some(v) = section.rollback_pause_sec {
        cfg.rollback_pause_sec = v.clamp(60, 7 * 24 * 3_600);
    }
    if let Some(v) = section.global_pause_sec {
        cfg.global_pause_sec = v.clamp(60, 14 * 24 * 3_600);
    }
    if let Some(v) = section.layer0_lock_sec {
        cfg.layer0_lock_sec = v.clamp(60, 14 * 24 * 3_600);
    }
    if let Some(v) = section.layer1_min_trades {
        cfg.layer1_min_trades = v.max(1);
    }
    if let Some(v) = section.layer2_min_trades {
        cfg.layer2_min_trades = v.max(cfg.layer1_min_trades);
    }
    if let Some(v) = section.layer2_min_uptime_sec {
        cfg.layer2_min_uptime_sec = v.max(3_600);
    }
    if let Some(v) = section.layer3_min_trades {
        cfg.layer3_min_trades = v.max(cfg.layer2_min_trades);
    }
    if let Some(v) = section.layer3_min_uptime_sec {
        cfg.layer3_min_uptime_sec = v.max(24 * 3_600);
    }
    if let Some(v) = section.black_swan_lock_sec {
        cfg.black_swan_lock_sec = v.clamp(60, 14 * 24 * 3_600);
    }
    if let Some(v) = section.source_health_floor {
        cfg.source_health_floor = v.clamp(0.0, 1.0);
    }
    if let Some(v) = section.history_retention_days {
        cfg.history_retention_days = v.clamp(7, 720);
    }
    if let Some(v) = section.objective_drawdown_penalty {
        cfg.objective_drawdown_penalty = v.clamp(0.1, 100.0);
    }
    if let Ok(url) = std::env::var("POLYEDGE_SEAT_OPTIMIZER_URL") {
        if !url.trim().is_empty() {
            cfg.optimizer_url = url;
        }
    }
    cfg
}
