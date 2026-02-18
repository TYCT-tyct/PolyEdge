use std::collections::HashMap;

use core_types::{Direction, DirectionSignal, TimeframeClass, TimeframeOpp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SymbolGatlingConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub chunk_notional_usdc: Option<f64>,
    #[serde(default)]
    pub min_chunks: Option<usize>,
    #[serde(default)]
    pub max_chunks: Option<usize>,
    #[serde(default)]
    pub spacing_ms: Option<u64>,
    #[serde(default)]
    pub stop_on_reject: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
struct GatlingResolved {
    enabled: bool,
    chunk_notional_usdc: f64,
    min_chunks: usize,
    max_chunks: usize,
    spacing_ms: u64,
    stop_on_reject: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TakerSniperConfig {
    pub min_direction_confidence: f64,
    pub min_edge_net_bps: f64,
    pub max_spread: f64,
    pub cooldown_ms_per_market: u64,
    pub gatling_enabled: bool,
    pub gatling_chunk_notional_usdc: f64,
    pub gatling_min_chunks: usize,
    pub gatling_max_chunks: usize,
    pub gatling_spacing_ms: u64,
    pub gatling_stop_on_reject: bool,
    #[serde(default)]
    pub gatling_by_symbol: HashMap<String, SymbolGatlingConfig>,
    /// Minimum quality score (0..100) required to fire.
    /// Score = signal (0..40) + market (0..35) + timing (0..25).
    #[serde(default = "default_min_win_rate_score")]
    pub min_win_rate_score: f64,
}

fn default_min_win_rate_score() -> f64 {
    55.0
}

impl Default for TakerSniperConfig {
    fn default() -> Self {
        Self {
            min_direction_confidence: 0.60,
            // Conservative default for taker path; config can override this.
            min_edge_net_bps: 200.0,
            max_spread: 0.08,
            cooldown_ms_per_market: 800,
            gatling_enabled: true,
            gatling_chunk_notional_usdc: 5.0,
            gatling_min_chunks: 1,
            gatling_max_chunks: 4,
            gatling_spacing_ms: 12,
            gatling_stop_on_reject: true,
            gatling_by_symbol: HashMap::new(),
            min_win_rate_score: 55.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TakerAction {
    Fire,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FireChunk {
    pub size: f64,
    pub send_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FirePlan {
    pub opportunity: TimeframeOpp,
    pub chunks: Vec<FireChunk>,
    pub stop_on_reject: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TakerDecision {
    pub action: TakerAction,
    pub fire_plan: Option<FirePlan>,
    pub reason: String,
}

// Inputs for a single taker decision.
#[derive(Debug, Clone)]
pub struct EvaluateCtx<'a> {
    pub market_id: &'a str,
    pub symbol: &'a str,
    pub timeframe: TimeframeClass,
    pub direction_signal: &'a DirectionSignal,
    pub entry_price: f64,
    pub spread: f64,
    pub fee_bps: f64,
    pub edge_gross_bps: f64,
    pub edge_net_bps: f64,
    pub size: f64,
    pub now_ms: i64,
}

#[derive(Debug)]
pub struct TakerSniper {
    cfg: TakerSniperConfig,
    last_fire_ms_by_market: HashMap<String, i64>,
}

impl TakerSniper {
    pub fn new(cfg: TakerSniperConfig) -> Self {
        Self {
            cfg,
            last_fire_ms_by_market: HashMap::new(),
        }
    }

    pub fn cfg(&self) -> &TakerSniperConfig {
        &self.cfg
    }

    pub fn set_cfg(&mut self, cfg: TakerSniperConfig) {
        self.cfg = cfg;
    }

    pub fn evaluate(&mut self, ctx: &EvaluateCtx<'_>) -> TakerDecision {
        if matches!(ctx.direction_signal.direction, Direction::Neutral) {
            return skip_static("neutral_direction");
        }
        if ctx.direction_signal.confidence < self.cfg.min_direction_confidence {
            return skip_static("low_confidence");
        }
        if ctx.entry_price <= 0.0 {
            return skip_static("bad_price");
        }
        if ctx.spread > self.cfg.max_spread {
            return skip_static("spread_too_wide");
        }
        let dynamic_min_edge =
            dynamic_fee_gate_min_edge_bps(ctx.entry_price, ctx.direction_signal.confidence);
        let min_edge_required = self.cfg.min_edge_net_bps.max(dynamic_min_edge);
        if ctx.edge_net_bps < min_edge_required {
            return skip_static("fee_gate_too_expensive");
        }
        if ctx.size <= 0.0 {
            return skip_static("size_zero");
        }
        if self.cfg.cooldown_ms_per_market > 0 {
            if let Some(last) = self.last_fire_ms_by_market.get(ctx.market_id) {
                let age = ctx.now_ms.saturating_sub(*last);
                if (age as u64) < self.cfg.cooldown_ms_per_market {
                    return skip_static("cooldown_active");
                }
            }
        }

        // Quality gate: skip weak opportunities.
        if self.cfg.min_win_rate_score > 0.0 {
            let score = compute_win_rate_score(ctx);
            if score < self.cfg.min_win_rate_score {
                return skip_dynamic(format!("win_rate_score_too_low:{score:.1}"));
            }
        }

        let lock_minutes = lock_minutes_for_timeframe(&ctx.timeframe);
        let notional_usdc = (ctx.entry_price.max(0.0) * ctx.size.max(0.0)).max(0.0);
        let edge_net_usdc = (ctx.edge_net_bps / 10_000.0) * notional_usdc;
        let density = if lock_minutes <= 0.0 { 0.0 } else { edge_net_usdc / lock_minutes };
        let opp = TimeframeOpp {
            timeframe: ctx.timeframe.clone(),
            market_id: ctx.market_id.to_string(),
            symbol: ctx.symbol.to_string(),
            direction: ctx.direction_signal.direction.clone(),
            side: direction_to_side(&ctx.direction_signal.direction),
            entry_price: ctx.entry_price,
            size: ctx.size,
            edge_gross_bps: ctx.edge_gross_bps,
            edge_net_bps: ctx.edge_net_bps,
            edge_net_usdc,
            fee_bps: ctx.fee_bps,
            lock_minutes,
            density,
            confidence: ctx.direction_signal.confidence,
            ts_ms: ctx.now_ms,
        };
        self.last_fire_ms_by_market
            .insert(ctx.market_id.to_string(), ctx.now_ms);
        let gatling = self.cfg.gatling_for_symbol(ctx.symbol);
        let fire_plan = build_fire_plan(&gatling, opp);
        TakerDecision {
            action: TakerAction::Fire,
            fire_plan: Some(fire_plan),
            reason: "fire".to_string(),
        }
    }
}

// ============================================================
// skip 辅助函数 — 两个版本消除不必要的堆分配
//   skip_static: 固定原因，零分配（热路径专用）
//   skip_dynamic: 动态原因，只在必要时分配
// ============================================================
#[inline]
fn skip_static(reason: &'static str) -> TakerDecision {
    TakerDecision {
        action: TakerAction::Skip,
        fire_plan: None,
        reason: reason.to_string(),
    }
}

#[inline]
fn skip_dynamic(reason: String) -> TakerDecision {
    TakerDecision {
        action: TakerAction::Skip,
        fire_plan: None,
        reason,
    }
}

// ============================================================
// 胜率评分系统 (0-100分)
// 三个维度分别评估信号、市场、时序质量
// 只有总分 ≥ min_win_rate_score 才触发，其余跳过
// ============================================================
fn compute_win_rate_score(ctx: &EvaluateCtx<'_>) -> f64 {
    let sig = ctx.direction_signal;

    // --- 信号质量 (0-40分) ---
    // velocity: 动量越强，信号越可靠
    let velocity_score = match sig.velocity_bps_per_sec.abs() {
        v if v >= 100.0 => 20.0,
        v if v >= 50.0  => 14.0,
        v if v >= 20.0  => 8.0,
        v if v >= 5.0   => 3.0,
        _               => 0.0,
    };
    // acceleration: 趋势加强中，不是减速
    let accel_score = if sig.acceleration > 0.0 { 10.0 } else { 0.0 };
    // tick_consistency: 连续同向 Tick 越多，方向越确定
    let tick_score = match sig.tick_consistency {
        t if t >= 3 => 10.0,
        2           => 5.0,
        1           => 2.0,
        _           => 0.0,
    };
    let signal_quality = velocity_score + accel_score + tick_score;

    // --- 市场质量 (0-35分) ---
    // price_zone: 极端价格区 Gamma 最高，费率最低，最容易盈利
    let p = ctx.entry_price.clamp(0.0, 1.0);
    let dist = (p - 0.5).abs(); // 0=中间, 0.5=极端
    let zone_score = match dist {
        d if d >= 0.42 => 20.0, // >0.92 或 <0.08: 最高 Gamma
        d if d >= 0.35 => 13.0, // 0.85-0.92
        d if d >= 0.25 => 6.0,  // 0.75-0.85
        _              => 0.0,  // 中间区间: 费率太高
    };
    // spread: 盘口越紧，滑点越小
    let spread_score = match ctx.spread {
        s if s < 0.01 => 10.0,
        s if s < 0.03 => 5.0,
        s if s < 0.05 => 2.0,
        _             => 0.0,
    };
    // triple_confirm: 三重确认通过是高质量信号的标志
    let confirm_score = if sig.triple_confirm { 5.0 } else { 0.0 };
    let market_quality = zone_score + spread_score + confirm_score;

    // --- 时序质量 (0-25分) ---
    // momentum_spike: 动量突刺是最强的入场信号
    let spike_score = if sig.momentum_spike { 15.0 } else { 0.0 };
    // edge: 预期盈利越高，时序价值越大
    let edge_score = match ctx.edge_net_bps {
        e if e >= 200.0 => 10.0,
        e if e >= 100.0 => 7.0,
        e if e >= 50.0  => 4.0,
        e if e >= 30.0  => 2.0,
        _               => 0.0,
    };
    let timing_quality = spike_score + edge_score;

    signal_quality + market_quality + timing_quality
}

fn build_fire_plan(gatling: &GatlingResolved, opportunity: TimeframeOpp) -> FirePlan {
    let total_size = opportunity.size.max(0.0);
    let notional = (opportunity.entry_price.max(0.0) * total_size).max(0.0);
    let min_chunks = gatling.min_chunks.max(1);
    let max_chunks = gatling.max_chunks.max(min_chunks);
    let desired_chunks = if gatling.enabled && gatling.chunk_notional_usdc > 0.0 {
        ((notional / gatling.chunk_notional_usdc).ceil() as usize).clamp(min_chunks, max_chunks)
    } else {
        1
    };

    let mut chunks = Vec::with_capacity(desired_chunks);
    if desired_chunks == 1 {
        chunks.push(FireChunk {
            size: total_size,
            send_delay_ms: 0,
        });
    } else {
        let mut remain = total_size;
        let base = total_size / desired_chunks as f64;
        for idx in 0..desired_chunks {
            let mut size = if idx + 1 == desired_chunks {
                remain
            } else {
                base
            };
            if idx + 1 != desired_chunks {
                size = size.max(0.01);
                remain = (remain - size).max(0.0);
            }
            chunks.push(FireChunk {
                size,
                send_delay_ms: if idx == 0 { 0 } else { gatling.spacing_ms },
            });
        }
    }

    FirePlan {
        opportunity,
        chunks,
        stop_on_reject: gatling.stop_on_reject,
    }
}

impl TakerSniperConfig {
    fn gatling_for_symbol(&self, symbol: &str) -> GatlingResolved {
        let mut resolved = GatlingResolved {
            enabled: self.gatling_enabled,
            chunk_notional_usdc: self.gatling_chunk_notional_usdc,
            min_chunks: self.gatling_min_chunks.max(1),
            max_chunks: self.gatling_max_chunks.max(self.gatling_min_chunks.max(1)),
            spacing_ms: self.gatling_spacing_ms,
            stop_on_reject: self.gatling_stop_on_reject,
        };
        let key = symbol.to_ascii_uppercase();
        if let Some(override_cfg) = self.gatling_by_symbol.get(&key) {
            if let Some(v) = override_cfg.enabled {
                resolved.enabled = v;
            }
            if let Some(v) = override_cfg.chunk_notional_usdc {
                resolved.chunk_notional_usdc = v.max(0.01);
            }
            if let Some(v) = override_cfg.min_chunks {
                resolved.min_chunks = v.max(1);
            }
            if let Some(v) = override_cfg.max_chunks {
                resolved.max_chunks = v.max(resolved.min_chunks);
            }
            if let Some(v) = override_cfg.spacing_ms {
                resolved.spacing_ms = v.min(1_000);
            }
            if let Some(v) = override_cfg.stop_on_reject {
                resolved.stop_on_reject = v;
            }
        }
        resolved
    }
}

fn lock_minutes_for_timeframe(tf: &TimeframeClass) -> f64 {
    match tf {
        TimeframeClass::Tf5m => 5.0,
        TimeframeClass::Tf15m => 15.0,
        TimeframeClass::Tf1h => 60.0,
        TimeframeClass::Tf1d => 1440.0,
    }
}

fn direction_to_side(dir: &Direction) -> core_types::OrderSide {
    match dir {
        Direction::Up => core_types::OrderSide::BuyYes,
        Direction::Down => core_types::OrderSide::BuyNo,
        Direction::Neutral => core_types::OrderSide::BuyYes,
    }
}

/// Dynamic edge gate by entry price bucket.
/// Near 0.50 prices require much larger edge due fee drag and toxicity.
#[inline]
fn dynamic_fee_gate_min_edge_bps(entry_price: f64, confidence: f64) -> f64 {
    let p = entry_price.clamp(0.0, 1.0);
    // Fee behavior is approximately symmetric around 0.50.
    let base_gate = if p >= 0.92 || p <= 0.08 {
        80.0
    } else if p >= 0.85 || p <= 0.15 {
        150.0
    } else if p >= 0.75 || p <= 0.25 {
        300.0
    } else if p >= 0.60 || p <= 0.40 {
        600.0
    } else {
        // Around 0.50, require a much larger edge.
        1200.0
    };
    // High confidence can relax at most 25%.
    let confidence_relax =
        (1.0 - (confidence.clamp(0.0, 1.0) - 0.5).max(0.0) * 0.4).clamp(0.75, 1.0);
    base_gate * confidence_relax
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{Direction, DirectionSignal, TimeframeClass};

    fn up_signal(confidence: f64) -> DirectionSignal {
        DirectionSignal {
            symbol: "BTCUSDT".to_string(),
            direction: Direction::Up,
            magnitude_pct: 0.20,
            confidence,
            recommended_tf: TimeframeClass::Tf15m,
            velocity_bps_per_sec: 7.5,
            acceleration: 0.8,
            tick_consistency: 3,
            triple_confirm: true,
            momentum_spike: false,
            ts_ns: 1,
        }
    }

    #[test]
    fn fires_on_strong_signal() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_direction_confidence: 0.7,
            min_edge_net_bps: 5.0,
            max_spread: 0.08,
            cooldown_ms_per_market: 0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.95,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 80.0,
            edge_net_bps: 80.0, // 0.95 区间需要 > 67.2 bps (80*0.84)
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Fire));
        assert!(d.fire_plan.is_some());
    }

    #[test]
    fn skips_low_confidence() {
        let mut sniper = TakerSniper::new(TakerSniperConfig::default());
        let sig = up_signal(0.5);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.52,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 30.0,
            edge_net_bps: 32.0,
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "low_confidence");
    }

    #[test]
    fn skips_edge_below_threshold() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 25.0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.52,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 30.0,
            edge_net_bps: 24.0,
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "fee_gate_too_expensive");
    }

    #[test]
    fn cooldown_blocks_repeated_fire() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 5.0,
            cooldown_ms_per_market: 1_000,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d1 = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.95,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 80.0,
            edge_net_bps: 80.0, // 0.95 区间需要 > 67.2 bps
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d1.action, TakerAction::Fire));
        let d2 = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.95,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 80.0,
            edge_net_bps: 80.0,
            size: 10.0,
            now_ms: 1_000_500,
        });
        assert!(matches!(d2.action, TakerAction::Skip));
        assert_eq!(d2.reason, "cooldown_active");
    }

    #[test]
    fn dynamic_fee_gate_blocks_mid_price_without_large_edge() {
        // 50¢ 区间需要 800 bps (置信度 0.9 放宽 25% → 600 bps)
        // 传入 edge_net_bps=95 远低于 600 bps, 应该被拦截
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.50,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 60.0,
            edge_net_bps: 95.0,
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "fee_gate_too_expensive");
    }

    #[test]
    fn dynamic_fee_gate_blocks_mid_price_even_with_moderate_edge() {
        // 即使 edge=500 bps, 50¢ 区间 (需要 ~600 bps) 也应被拦截
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.50,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 500.0,
            edge_net_bps: 500.0,
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Skip));
        assert_eq!(d.reason, "fee_gate_too_expensive");
    }

    #[test]
    fn dynamic_fee_gate_allows_extreme_price_with_small_edge() {
        // 0.95 区间: 80 bps base * 0.84 (confidence=0.9 放宽) = 67.2 bps
        // edge_net_bps=80 > 67.2, 应该 Fire
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            min_win_rate_score: 0.0, // 只测试 fee gate 行为
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.95,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 80.0,
            edge_net_bps: 80.0, // 80 > 67.2 bps → Fire
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Fire));
    }

    #[test]
    fn dynamic_fee_gate_relaxes_with_higher_confidence() {
        // 验证高置信度确实能放宽门槛 (在极端价格区间)
        // 0.85 区间: 150 bps base
        //   confidence=0.55: relax = 1 - (0.55-0.5)*0.4 = 0.98 → 需要 147 bps
        //   confidence=0.95: relax = 1 - (0.95-0.5)*0.4 = 0.82 → 需要 123 bps
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 10.0,
            min_win_rate_score: 0.0, // 只测试 fee gate 行为，禁用胜率过滤
            ..TakerSniperConfig::default()
        });
        let low = up_signal(0.55);
        let high = up_signal(0.95);
        // 低置信度: edge=130 bps < 147 bps → Skip
        let d_low = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &low,
            entry_price: 0.85,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 130.0,
            edge_net_bps: 130.0,
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d_low.action, TakerAction::Skip));
        // 高置信度: edge=130 bps > 123 bps → Fire
        let d_high = sniper.evaluate(&EvaluateCtx {
            market_id: "m2",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &high,
            entry_price: 0.85,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 130.0,
            edge_net_bps: 130.0,
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d_high.action, TakerAction::Fire));
    }

    #[test]
    fn gatling_plan_splits_into_chunks() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            gatling_enabled: true,
            gatling_chunk_notional_usdc: 2.0,
            gatling_min_chunks: 2,
            gatling_max_chunks: 5,
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.90,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 200.0,
            edge_net_bps: 200.0, // 0.90 区间需要 > 150 bps
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Fire));
        let Some(plan) = d.fire_plan else {
            panic!("expected fire plan");
        };
        assert!(plan.chunks.len() >= 2);
        assert!(plan.stop_on_reject);
        let total: f64 = plan.chunks.iter().map(|c| c.size).sum();
        assert!((total - 10.0).abs() < 1e-6);
    }

    #[test]
    fn symbol_level_gatling_override_applied() {
        let mut sniper = TakerSniper::new(TakerSniperConfig {
            min_edge_net_bps: 5.0, // 测试 gatling 行为，不测试 edge 门槛
            gatling_enabled: false,
            gatling_chunk_notional_usdc: 10.0,
            gatling_min_chunks: 1,
            gatling_max_chunks: 2,
            gatling_by_symbol: HashMap::from([(
                "BTCUSDT".to_string(),
                SymbolGatlingConfig {
                    enabled: Some(true),
                    chunk_notional_usdc: Some(2.0),
                    min_chunks: Some(2),
                    max_chunks: Some(4),
                    spacing_ms: Some(7),
                    stop_on_reject: Some(false),
                },
            )]),
            ..TakerSniperConfig::default()
        });
        let sig = up_signal(0.9);
        let d = sniper.evaluate(&EvaluateCtx {
            market_id: "m1",
            symbol: "BTCUSDT",
            timeframe: TimeframeClass::Tf15m,
            direction_signal: &sig,
            entry_price: 0.90,
            spread: 0.01,
            fee_bps: 2.0,
            edge_gross_bps: 200.0,
            edge_net_bps: 200.0, // 0.90 区间需要 > 150 bps
            size: 10.0,
            now_ms: 1_000_000,
        });
        assert!(matches!(d.action, TakerAction::Fire));
        let Some(plan) = d.fire_plan else {
            panic!("expected fire plan");
        };
        assert!(plan.chunks.len() >= 2);
        assert_eq!(plan.chunks[1].send_delay_ms, 7);
        assert!(!plan.stop_on_reject);
    }
}
