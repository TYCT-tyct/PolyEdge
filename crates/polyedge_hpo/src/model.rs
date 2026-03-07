use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyParams {
    pub entry_threshold_base: f64,
    pub entry_threshold_cap: f64,
    pub spread_limit_prob: f64,
    pub entry_edge_prob: f64,
    pub entry_min_potential_cents: f64,
    pub entry_max_price_cents: f64,
    pub min_hold_ms: i64,
    pub stop_loss_cents: f64,
    pub reverse_signal_threshold: f64,
    pub reverse_signal_ticks: i64,
    pub trail_activate_profit_cents: f64,
    pub trail_drawdown_cents: f64,
    pub take_profit_near_max_cents: f64,
    pub endgame_take_profit_cents: f64,
    pub endgame_remaining_ms: i64,
    pub liquidity_widen_prob: f64,
    pub cooldown_ms: i64,
    pub max_entries_per_round: i64,
    pub max_exec_spread_cents: f64,
    pub slippage_cents_per_side: f64,
    pub fee_cents_per_side: f64,
    pub emergency_wide_spread_penalty_ratio: f64,
    pub stop_loss_grace_ticks: i64,
    pub stop_loss_hard_mult: f64,
    pub stop_loss_reverse_extra_ticks: i64,
    pub loss_cluster_limit: i64,
    pub loss_cluster_cooldown_ms: i64,
    pub noise_gate_enabled: bool,
    pub noise_gate_threshold_add: f64,
    pub noise_gate_edge_add: f64,
    pub noise_gate_spread_scale: f64,
    pub vic_enabled: bool,
    pub vic_target_entries_per_hour: f64,
    pub vic_deadband_ratio: f64,
    pub vic_threshold_relax_max: f64,
    pub vic_edge_relax_max: f64,
    pub vic_spread_relax_max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraints {
    pub min_trades: u32,
    pub min_win_rate_pct: f64,
    pub max_drawdown_cents: f64,
    pub require_non_negative_worst_window_net: bool,
}

impl Default for Constraints {
    fn default() -> Self {
        Self {
            min_trades: 160,
            min_win_rate_pct: 68.0,
            max_drawdown_cents: 900.0,
            require_non_negative_worst_window_net: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    pub iterations: usize,
    pub population: usize,
    pub elite: usize,
    pub random_injection: usize,
    pub mutation_ratio: f64,
    pub worker_concurrency: usize,
    pub seed: Option<u64>,
    pub max_top_candidates: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            iterations: 20,
            population: 48,
            elite: 8,
            random_injection: 8,
            mutation_ratio: 0.18,
            worker_concurrency: 8,
            seed: None,
            max_top_candidates: 12,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub forge_base_url: Option<String>,
    pub symbol: String,
    pub market_type: String,
    pub lookbacks: Option<Vec<u32>>,
    pub full_history: Option<bool>,
    pub max_trades: Option<u32>,
    pub max_samples: Option<u32>,
    pub timeout_secs: Option<u64>,
    pub seed_params: Option<Vec<StrategyParams>>,
    pub search: Option<SearchConfig>,
    pub constraints: Option<Constraints>,
}

#[derive(Debug, Clone)]
pub struct ResolvedSearchRequest {
    pub forge_base_url: String,
    pub symbol: String,
    pub market_type: String,
    pub lookbacks: Vec<u32>,
    pub full_history: bool,
    pub max_trades: u32,
    pub max_samples: u32,
    pub timeout_secs: u64,
    pub seed_params: Vec<StrategyParams>,
    pub search: SearchConfig,
    pub constraints: Constraints,
}

impl SearchRequest {
    pub fn resolve(self) -> anyhow::Result<ResolvedSearchRequest> {
        let search = self.search.unwrap_or_default();
        if search.population < 8 {
            anyhow::bail!("search.population must be >= 8");
        }
        if search.elite == 0 || search.elite >= search.population {
            anyhow::bail!("search.elite must be > 0 and < search.population");
        }
        if !(0.0..=0.6).contains(&search.mutation_ratio) {
            anyhow::bail!("search.mutation_ratio must be in [0.0, 0.6]");
        }
        let constraints = self.constraints.unwrap_or_default();
        let lookbacks = self.lookbacks.unwrap_or_else(|| vec![720, 1440, 2880]);
        if lookbacks.is_empty() {
            anyhow::bail!("lookbacks must not be empty");
        }
        let seed_params = self.seed_params.unwrap_or_default();
        Ok(ResolvedSearchRequest {
            forge_base_url: self
                .forge_base_url
                .unwrap_or_else(|| "http://127.0.0.1:9810".to_string()),
            symbol: self.symbol.trim().to_ascii_uppercase(),
            market_type: self.market_type.trim().to_string(),
            lookbacks,
            full_history: self.full_history.unwrap_or(false),
            max_trades: self.max_trades.unwrap_or(1500),
            max_samples: self.max_samples.unwrap_or(260_000),
            timeout_secs: self.timeout_secs.unwrap_or(180),
            seed_params,
            search,
            constraints,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowMetrics {
    pub lookback_minutes: u32,
    pub trade_count: u32,
    pub win_rate_pct: f64,
    pub avg_pnl_cents: f64,
    pub net_pnl_cents: f64,
    pub max_drawdown_cents: f64,
    pub last80_win_rate_pct: f64,
    pub last80_avg_pnl_cents: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateResult {
    pub score: f64,
    pub robust_score: f64,
    pub avg_net_pnl_cents: f64,
    pub worst_window_net_pnl_cents: f64,
    pub avg_win_rate_pct: f64,
    pub worst_window_win_rate_pct: f64,
    pub max_drawdown_cents: f64,
    pub avg_trade_count: f64,
    pub windows: Vec<WindowMetrics>,
    pub hard_fail_reasons: Vec<String>,
    pub params: StrategyParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterationSummary {
    pub iteration: usize,
    pub best_score: f64,
    pub best_avg_net_pnl_cents: f64,
    pub best_win_rate_pct: f64,
    pub best_drawdown_cents: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub started_at: String,
    pub finished_at: String,
    pub symbol: String,
    pub market_type: String,
    pub lookbacks: Vec<u32>,
    pub total_iterations: usize,
    pub population: usize,
    pub best: CandidateResult,
    pub top_candidates: Vec<CandidateResult>,
    pub iteration_summaries: Vec<IterationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    pub stage: String,
    pub iteration: usize,
    pub total_iterations: usize,
    pub evaluated_candidates: usize,
    pub best_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub progress: JobProgress,
    pub request: SearchRequest,
    pub result: Option<SearchResult>,
    pub error: Option<String>,
}

impl JobRecord {
    pub fn new(job_id: String, request: SearchRequest) -> Self {
        let now = now_utc();
        Self {
            job_id,
            status: "queued".to_string(),
            created_at: now.clone(),
            updated_at: now,
            progress: JobProgress {
                stage: "queued".to_string(),
                iteration: 0,
                total_iterations: 0,
                evaluated_candidates: 0,
                best_score: None,
            },
            request,
            result: None,
            error: None,
        }
    }
}

pub fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
