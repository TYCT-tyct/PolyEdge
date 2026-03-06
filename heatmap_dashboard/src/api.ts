import type {
    AccuracySeriesResponse,
    AvailableRoundsResponse,
    ChartPoint,
    ChartResponse,
    CollectorMetricsResponse,
    CollectorStatusResponse,
    HeatmapResponse,
    MarketSymbol,
    MarketType,
    RoundChartResponse,
    RoundsResponse,
    StatsResponse,
    StrategyPaperResponse,
    WindowType,
    WsLivePayload
} from "./types";

const API_BASE = (import.meta.env.VITE_FORGE_API_BASE as string | undefined)?.trim() ?? "";
const REQUEST_TIMEOUT_MS = 10_000;
const RESOLVED_UP_PRICE_CENTS = 99.0;
const RESOLVED_DOWN_PRICE_CENTS = 0.0;
const inflightRequests = new Map<string, Promise<unknown>>();
const responseCache = new Map<string, { expiresAt: number; value: unknown }>();
const apiSupport: {
  stats: boolean | null;
  collectorStatus: boolean | null;
  collectorMetrics: boolean | null;
  chart: boolean | null;
  rounds: boolean | null;
  roundsAvailable: boolean | null;
  roundChart: boolean | null;
  heatmap: boolean | null;
  accuracy: boolean | null;
} = {
  stats: null,
  collectorStatus: null,
  collectorMetrics: null,
  chart: null,
  rounds: null,
  roundsAvailable: null,
  roundChart: null,
  heatmap: null,
  accuracy: null
};

function responseCacheTtlMs(path: string): number {
  if (path.startsWith("/api/chart?")) {
    return 1_200;
  }
  if (path === "/api/latest/all") {
    return 700;
  }
  if (path === "/api/stats") {
    return 2_000;
  }
  if (path.startsWith("/api/collector/status")) {
    return 1_500;
  }
  if (path.startsWith("/api/collector/metrics")) {
    return 1_500;
  }
  return 0;
}

interface HistoryRaw {
  sample_count: number;
  round_count: number;
  samples: Array<{
    ts_ireland_sample_ms: number;
    delta_pct: number | null;
    delta_pct_smooth?: number | null;
    bid_yes: number | null;
    ask_yes: number | null;
    bid_no: number | null;
    ask_no: number | null;
    round_id: string;
    remaining_ms: number;
    binance_price: number | null;
    target_price: number | null;
    mid_yes: number | null;
    mid_yes_smooth?: number | null;
    mid_no: number | null;
    mid_no_smooth?: number | null;
    symbol: string;
    timeframe: string;
  }>;
  rounds: Array<{
    round_id: string;
    market_id: string;
    symbol: string;
    timeframe: string;
    start_ts_ms: number;
    end_ts_ms: number;
    target_price: number;
    settle_price: number;
    label_up: number | boolean;
  }>;
}

function normalizeSymbol(symbol: string): MarketSymbol {
  const s = symbol.trim().toUpperCase();
  if (s === "ETHUSDT" || s === "SOLUSDT" || s === "XRPUSDT") {
    return s;
  }
  return "BTCUSDT";
}

class HttpError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

function buildHttpUrl(path: string): string {
  if (!API_BASE) {
    return path;
  }
  return `${API_BASE.replace(/\/+$/, "")}${path}`;
}

function buildWsUrl(path: string): string {
  if (API_BASE) {
    const base = API_BASE.replace(/\/+$/, "");
    if (base.startsWith("https://")) {
      return `wss://${base.slice("https://".length)}${path}`;
    }
    if (base.startsWith("http://")) {
      return `ws://${base.slice("http://".length)}${path}`;
    }
    return `${base}${path}`;
  }
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.host}${path}`;
}

async function requestJson<T>(path: string, timeoutMs: number = REQUEST_TIMEOUT_MS): Promise<T> {
  const key = buildHttpUrl(path);
  const ttlMs = responseCacheTtlMs(path);
  if (ttlMs > 0) {
    const cached = responseCache.get(key);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.value as T;
    }
  }
  const existing = inflightRequests.get(key);
  if (existing) {
    return (await existing) as T;
  }
  const req = (async () => {
    const controller = new AbortController();
    let timeoutTriggered = false;
    const timer = window.setTimeout(() => {
      timeoutTriggered = true;
      controller.abort();
    }, timeoutMs);
    let resp: Response;
    try {
      resp = await fetch(key, { cache: "no-store", signal: controller.signal });
    } catch (err) {
      const aborted =
        err instanceof DOMException
          ? err.name === "AbortError"
          : typeof err === "object" && err != null && (err as { name?: string }).name === "AbortError";
      if (aborted && timeoutTriggered) {
        throw new HttpError(408, `Request timeout after ${timeoutMs}ms: ${path}`);
      }
      throw err;
    } finally {
      window.clearTimeout(timer);
    }
    if (!resp.ok) {
      const text = await resp.text();
      throw new HttpError(resp.status, `HTTP ${resp.status}: ${text}`);
    }
    const value = (await resp.json()) as T;
    if (ttlMs > 0) {
      if (responseCache.size >= 256) {
        const now = Date.now();
        for (const [cacheKey, entry] of responseCache.entries()) {
          if (entry.expiresAt <= now) {
            responseCache.delete(cacheKey);
          }
        }
      }
      responseCache.set(key, { expiresAt: Date.now() + ttlMs, value });
    }
    return value;
  })();
  inflightRequests.set(key, req as Promise<unknown>);
  try {
    return await req;
  } finally {
    inflightRequests.delete(key);
  }
}

import { windowToMinutes } from "./utils";

const CHART_LOOKBACK_MAX_MINUTES = 360;

function computeChartLookbackMinutes(view: WindowType): number {
  if (view === "all") {
    return CHART_LOOKBACK_MAX_MINUTES;
  }
  const viewMinutes = windowToMinutes(view);
  if (viewMinutes <= 0) {
    return 120;
  }
  // Keep modest context for bucket/indicator stability, but avoid loading
  // significantly more than the selected display window.
  const withContext = Math.round(viewMinutes * 2);
  return Math.min(CHART_LOOKBACK_MAX_MINUTES, Math.max(30, withContext));
}

function maxPointsForView(view: WindowType): number {
  switch (view) {
    case "5m":
      return 1_200;
    case "15m":
      return 1_800;
    case "30m":
      return 2_400;
    case "1h":
      return 3_200;
    case "2h":
      return 4_500;
    case "4h":
      return 6_000;
    default:
      return 8_000;
  }
}

function downsampleRows<T>(rows: T[], max: number): { rows: T[]; step: number } {
  if (rows.length <= max) {
    return { rows, step: 1 };
  }
  const step = Math.ceil(rows.length / max);
  const out: T[] = [];
  for (let i = 0; i < rows.length; i += step) {
    out.push(rows[i]!);
  }
  const last = rows[rows.length - 1];
  if (out[out.length - 1] !== last) {
    out.push(last!);
  }
  return { rows: out, step };
}

function finiteOrNull(v: number | null | undefined): number | null {
  return v != null && Number.isFinite(v) ? v : null;
}

function isFiniteNumber(v: unknown): v is number {
  return typeof v === "number" && Number.isFinite(v);
}

function isObjectRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function isStrategyPaperSummary(v: unknown): v is StrategyPaperResponse["summary"] {
  if (!isObjectRecord(v)) {
    return false;
  }
  return (
    isFiniteNumber(v.trade_count) &&
    isFiniteNumber(v.win_rate_pct) &&
    isFiniteNumber(v.avg_pnl_cents) &&
    isFiniteNumber(v.total_pnl_cents) &&
    isFiniteNumber(v.net_pnl_cents) &&
    isFiniteNumber(v.gross_pnl_cents) &&
    isFiniteNumber(v.total_cost_cents) &&
    isFiniteNumber(v.total_entry_fee_cents) &&
    isFiniteNumber(v.total_exit_fee_cents) &&
    isFiniteNumber(v.total_slippage_cents) &&
    isFiniteNumber(v.net_margin_pct) &&
    isFiniteNumber(v.max_drawdown_cents)
  );
}

function isStrategyPaperConfig(v: unknown): boolean {
  if (!isObjectRecord(v)) {
    return false;
  }
  return (
    isFiniteNumber(v.entry_threshold_base) &&
    isFiniteNumber(v.entry_threshold_cap) &&
    isFiniteNumber(v.spread_limit_prob) &&
    isFiniteNumber(v.stop_loss_cents) &&
    isFiniteNumber(v.trail_drawdown_cents) &&
    isFiniteNumber(v.reverse_signal_threshold) &&
    isFiniteNumber(v.reverse_signal_ticks)
  );
}

function isStrategyPaperResponsePayload(v: Record<string, unknown>): v is StrategyPaperResponse {
  return (
    typeof v.market_type === "string" &&
    isFiniteNumber(v.lookback_minutes) &&
    isFiniteNumber(v.samples) &&
    Array.isArray(v.trades) &&
    isStrategyPaperSummary(v.summary) &&
    isStrategyPaperConfig(v.config)
  );
}

function roundOutcome(
  settlePrice: number | null | undefined,
  targetPrice: number | null | undefined,
  fallbackLabel: number | boolean | null | undefined
): number {
  const settle = finiteOrNull(settlePrice);
  const target = finiteOrNull(targetPrice);
  if (settle != null && target != null && target > 0) {
    return settle > target ? 1 : 0;
  }
  return Number(fallbackLabel) === 1 ? 1 : 0;
}

function midpointProb(
  bid: number | null | undefined,
  ask: number | null | undefined
): number | null {
  const b = finiteOrNull(bid);
  const a = finiteOrNull(ask);
  if (b != null && a != null) {
    return Math.max(0, Math.min(1, (b + a) * 0.5));
  }
  if (b != null) {
    return Math.max(0, Math.min(1, b));
  }
  if (a != null) {
    return Math.max(0, Math.min(1, a));
  }
  return null;
}

function computeDeltaPct(
  deltaPct: number | null | undefined,
  btc: number | null | undefined,
  target: number | null | undefined
): number | null {
  const d = finiteOrNull(deltaPct);
  if (d != null) {
    return d;
  }
  const px = finiteOrNull(btc);
  const tp = finiteOrNull(target);
  if (px == null || tp == null || tp <= 0) {
    return null;
  }
  return ((px - tp) / tp) * 100;
}

function normalizeChartPoint(p: ChartPoint): ChartPoint {
  const bestBidUp = finiteOrNull(p.best_bid_up);
  const bestAskUp = finiteOrNull(p.best_ask_up);
  const bestBidDown = finiteOrNull(p.best_bid_down);
  const bestAskDown = finiteOrNull(p.best_ask_down);
  return {
    ...p,
    timestamp_ms: Number.isFinite(p.timestamp_ms) ? p.timestamp_ms : 0,
    delta_pct: computeDeltaPct(
      finiteOrNull(p.delta_pct_smooth) ?? p.delta_pct,
      p.btc_price,
      p.target_price
    ),
    mid_yes:
      finiteOrNull(p.mid_yes_smooth) ??
      finiteOrNull(p.mid_yes) ??
      midpointProb(bestBidUp, bestAskUp),
    mid_no:
      finiteOrNull(p.mid_no_smooth) ??
      finiteOrNull(p.mid_no) ??
      midpointProb(bestBidDown, bestAskDown),
    best_bid_up: bestBidUp,
    best_ask_up: bestAskUp,
    best_bid_down: bestBidDown,
    best_ask_down: bestAskDown,
    time_remaining_s: finiteOrNull(p.time_remaining_s),
    btc_price: finiteOrNull(p.btc_price),
    target_price: finiteOrNull(p.target_price)
  };
}

function roundEndMs(round: {
  end_ts_ms?: number | null;
  end_time_ms?: number | null;
}): number | null {
  return finiteOrNull(round.end_ts_ms) ?? finiteOrNull(round.end_time_ms) ?? null;
}

function appendRoundSettlementAnchors(
  points: ChartPoint[],
  rounds: Array<{
    round_id: string;
    outcome?: number | null;
    end_ts_ms?: number | null;
    end_time_ms?: number | null;
    target_price?: number | null;
  }>
): ChartPoint[] {
  if (!rounds.length) {
    return points;
  }
  const out = [...points];
  const resolvedUp = RESOLVED_UP_PRICE_CENTS / 100;
  const resolvedDown = RESOLVED_DOWN_PRICE_CENTS / 100;
  for (const round of rounds) {
    const roundId = typeof round.round_id === "string" ? round.round_id : "";
    const outcome = finiteOrNull(round.outcome);
    const endMs = roundEndMs(round);
    if (!roundId || outcome == null || endMs == null || endMs <= 0) {
      continue;
    }
    const up = outcome >= 0.5 ? resolvedUp : resolvedDown;
    const down = 1 - up;
    const hasNearby = out.some(
      (p) => p.round_id === roundId && Math.abs(p.timestamp_ms - endMs) <= 350
    );
    if (hasNearby) {
      continue;
    }
    out.push(
      normalizeChartPoint({
        timestamp_ms: Math.round(endMs),
        delta_pct: null,
        delta_pct_smooth: null,
        mid_yes: up,
        mid_no: down,
        mid_yes_smooth: up,
        mid_no_smooth: down,
        best_bid_up: up,
        best_ask_up: up,
        best_bid_down: down,
        best_ask_down: down,
        round_id: roundId,
        time_remaining_s: 0,
        btc_price: null,
        target_price: finiteOrNull(round.target_price)
      })
    );
  }
  return out;
}

function normalizeChartResponse(data: ChartResponse): ChartResponse {
  const sorted = appendRoundSettlementAnchors(data.points, data.rounds)
    .map((p) => normalizeChartPoint(p))
    .filter((p) => p.timestamp_ms > 0)
    .sort((a, b) => {
      if (a.timestamp_ms !== b.timestamp_ms) {
        return a.timestamp_ms - b.timestamp_ms;
      }
      const ar = a.round_id ?? "";
      const br = b.round_id ?? "";
      return ar.localeCompare(br);
    });
  const deduped: ChartPoint[] = [];
  for (const p of sorted) {
    const prev = deduped[deduped.length - 1];
    if (prev && prev.timestamp_ms === p.timestamp_ms && (prev.round_id ?? "") === (p.round_id ?? "")) {
      deduped[deduped.length - 1] = p;
    } else {
      deduped.push(p);
    }
  }
  return {
    ...data,
    points: deduped
  };
}

function normalizeRoundChartResponse(data: RoundChartResponse): RoundChartResponse {
  return {
    ...data,
    points: normalizeChartResponse({
      points: data.points,
      rounds: [],
      total_samples: data.total_samples,
      downsampled: false,
      step: data.step
    }).points
  };
}

async function getHistoryRaw(
  symbol: MarketSymbol,
  marketType: MarketType,
  lookbackMinutes: number,
  limit: number
): Promise<HistoryRaw> {
  const qs = new URLSearchParams({
    lookback_minutes: String(lookbackMinutes),
    limit: String(limit)
  });
  return requestJson<HistoryRaw>(`/api/history/${symbol}/${marketType}?${qs.toString()}`);
}

export async function getStats(symbol: MarketSymbol = "BTCUSDT"): Promise<StatsResponse> {
  const qs = new URLSearchParams({ symbol });
  if (apiSupport.stats !== false) {
    try {
      const data = await requestJson<StatsResponse>(`/api/stats?${qs.toString()}`);
      apiSupport.stats = true;
      return data;
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.stats = false;
    }
  }
  const [h5, h15] = await Promise.all([
    getHistoryRaw(symbol, "5m", 6 * 60, 30_000),
    getHistoryRaw(symbol, "15m", 12 * 60, 30_000)
  ]);
  const totalSamples = h5.sample_count + h15.sample_count;
  const rounds = [...h5.rounds, ...h15.rounds];
  const upCount = rounds.filter((r) => roundOutcome(r.settle_price, r.target_price, r.label_up) === 1).length;
  const downCount = rounds.length - upCount;
  const allSamples = [...h5.samples, ...h15.samples].sort(
    (a, b) => a.ts_ireland_sample_ms - b.ts_ireland_sample_ms
  );
  const first = allSamples[0]?.ts_ireland_sample_ms ?? null;
  const last = allSamples[allSamples.length - 1]?.ts_ireland_sample_ms ?? null;
  const uptimeHours = first != null && last != null && last >= first ? (last - first) / 3_600_000 : 0;
  return {
    total_rounds: rounds.length,
    total_samples: totalSamples,
    up_count: upCount,
    down_count: downCount,
    first_sample_ms: first,
    last_sample_ms: last,
    uptime_hours: uptimeHours,
    market_accuracy: null,
    market_accuracy_n: 0
  };
}

export async function getCollectorStatus(symbol: MarketSymbol = "BTCUSDT"): Promise<CollectorStatusResponse> {
  const qs = new URLSearchParams({ symbol });
  if (apiSupport.collectorStatus !== false) {
    try {
      const data = await requestJson<CollectorStatusResponse>(`/api/collector/status?${qs.toString()}`);
      apiSupport.collectorStatus = true;
      return data;
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.collectorStatus = false;
    }
  }
  const nowMs = Date.now();
  const rows = await getLatestAllRaw();
  const makeRow = (tf: MarketType) => {
    const latest = rows
      .filter((r) => r.timeframe === tf && normalizeSymbol(r.symbol) === symbol)
      .sort((a, b) => b.ts_ireland_sample_ms - a.ts_ireland_sample_ms)[0];
    if (!latest) {
      return {
        status: "missing",
        timestamp_ms: null,
        age_ms: null,
        remaining_ms: null,
        round_id: ""
      } as const;
    }
    const age = Math.max(0, nowMs - latest.ts_ireland_sample_ms);
    return {
      status: age <= 4_000 ? "ok" : age <= 12_000 ? "lagging" : "stalled",
      timestamp_ms: latest.ts_ireland_sample_ms,
      age_ms: age,
      remaining_ms: latest.remaining_ms,
      round_id: latest.round_id
    } as const;
  };
  const r5 = makeRow("5m");
  const r15 = makeRow("15m");
  return {
    ok: r5.status === "ok" && r15.status === "ok",
    ts_ms: nowMs,
    timeframes: {
      "5m": r5,
      "15m": r15
    }
  };
}

export async function getCollectorMetrics(
  symbol: MarketSymbol = "BTCUSDT"
): Promise<CollectorMetricsResponse> {
  const qs = new URLSearchParams({ symbol });
  if (apiSupport.collectorMetrics !== false) {
    try {
      const data = await requestJson<CollectorMetricsResponse>(`/api/collector/metrics?${qs.toString()}`);
      apiSupport.collectorMetrics = true;
      return data;
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.collectorMetrics = false;
    }
  }
  const status = await getCollectorStatus(symbol);
  return {
    ...status,
    symbol,
    window_ms: 180_000
  };
}

export interface LatestRawRow {
  ts_ireland_sample_ms: number;
  symbol: string;
  timeframe: string;
  market_id: string;
  round_id: string;
  target_price: number | null;
  binance_price: number | null;
  pm_live_btc_price?: number | null;
  chainlink_price?: number | null;
  mid_yes: number | null;
  mid_no: number | null;
  mid_yes_smooth?: number | null;
  mid_no_smooth?: number | null;
  bid_yes?: number | null;
  ask_yes?: number | null;
  bid_no?: number | null;
  ask_no?: number | null;
  delta_price: number | null;
  delta_pct: number | null;
  delta_pct_smooth?: number | null;
  remaining_ms: number;
  velocity_bps_per_sec?: number | null;
  acceleration?: number | null;
}

export async function getLatestAllRaw(): Promise<LatestRawRow[]> {
  return requestJson<LatestRawRow[]>("/api/latest/all");
}

export async function getChart(
  marketType: MarketType,
  view: WindowType,
  symbol: MarketSymbol = "BTCUSDT"
): Promise<ChartResponse> {
  const minutes = computeChartLookbackMinutes(view);
  const maxPoints = maxPointsForView(view);
  const qs = new URLSearchParams({
    market_type: marketType,
    minutes: String(minutes),
    max_points: String(maxPoints),
    symbol
  });
  if (apiSupport.chart !== false) {
    try {
      const data = await requestJson<ChartResponse>(`/api/chart?${qs.toString()}`);
      apiSupport.chart = true;
      return normalizeChartResponse(data);
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.chart = false;
    }
  }
  try {
    const lookback = minutes;
    const estimatedLimit = Math.min(50_000, Math.max(6_000, lookback * 600));
    const history = await getHistoryRaw(symbol, marketType, lookback, estimatedLimit);
    const points = history.samples
      .map((s) =>
        normalizeChartPoint({
          timestamp_ms: s.ts_ireland_sample_ms,
          delta_pct: s.delta_pct,
          delta_pct_smooth: s.delta_pct_smooth ?? null,
          mid_yes: s.mid_yes,
          mid_yes_smooth: s.mid_yes_smooth ?? null,
          mid_no: s.mid_no,
          mid_no_smooth: s.mid_no_smooth ?? null,
          best_bid_up: s.bid_yes,
          best_ask_up: s.ask_yes,
          best_bid_down: s.bid_no,
          best_ask_down: s.ask_no,
          round_id: s.round_id,
          time_remaining_s: s.remaining_ms / 1000,
          btc_price: s.binance_price,
          target_price: s.target_price
        })
      )
      .sort((a, b) => a.timestamp_ms - b.timestamp_ms);
    const sampled = downsampleRows(points, maxPoints);
    return {
      points: sampled.rows,
      rounds: history.rounds
        .map((r) => ({
          round_id: r.round_id,
          start_ts_ms: r.start_ts_ms,
          end_ts_ms: r.end_ts_ms,
          target_price: r.target_price,
          outcome: roundOutcome(r.settle_price, r.target_price, r.label_up)
        })),
      total_samples: points.length,
      downsampled: sampled.step > 1,
      step: sampled.step
    };
  } catch (err) {
    throw err;
  }
}

export async function getRoundHistory(
  marketType: MarketType,
  limit = 200,
  symbol: MarketSymbol = "BTCUSDT"
): Promise<RoundsResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    limit: String(limit),
    symbol
  });
  if (apiSupport.rounds !== false) {
    try {
      const data = await requestJson<RoundsResponse>(`/api/rounds?${qs.toString()}`);
      apiSupport.rounds = true;
      return data;
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.rounds = false;
    }
  }
  try {
    const history = await getHistoryRaw(symbol, marketType, 12 * 60, 50_000);
    const rows = history.rounds
      .map((r) => {
        const target = r.target_price;
        const final = r.settle_price;
        const outcome = roundOutcome(final, target, r.label_up);
        return {
          round_id: r.round_id,
          market_id: r.market_id,
          market_type: marketType,
          start_time_ms: r.start_ts_ms,
          end_time_ms: r.end_ts_ms,
          target_price: target,
          final_btc_price: final,
          outcome,
          delta_pct: target > 0 ? ((final - target) / target) * 100 : null,
          mkt_price_cents: outcome === 1 ? RESOLVED_UP_PRICE_CENTS : RESOLVED_DOWN_PRICE_CENTS
        };
      })
      .sort((a, b) => b.end_time_ms - a.end_time_ms)
      .slice(0, limit);
    return {
      market_type: marketType,
      count: rows.length,
      rounds: rows
    };
  } catch (err) {
    throw err;
  }
}

export async function getAvailableRounds(
  marketType: MarketType,
  symbol: MarketSymbol = "BTCUSDT"
): Promise<AvailableRoundsResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    symbol
  });
  if (apiSupport.roundsAvailable !== false) {
    try {
      const data = await requestJson<AvailableRoundsResponse>(`/api/rounds/available?${qs.toString()}`);
      apiSupport.roundsAvailable = true;
      return data;
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.roundsAvailable = false;
    }
  }
  try {
    const history = await getRoundHistory(marketType, 600, symbol);
    const rounds = history.rounds.map((r) => {
      const date = new Date(r.start_time_ms).toISOString().slice(0, 10);
      return {
        round_id: r.round_id,
        market_id: r.market_id,
        start_time_ms: r.start_time_ms,
        end_time_ms: r.end_time_ms,
        date
      };
    });
    const countByDate = new Map<string, number>();
    for (const r of rounds) {
      countByDate.set(r.date, (countByDate.get(r.date) ?? 0) + 1);
    }
    const days = [...countByDate.entries()]
      .map(([date, round_count]) => ({ date, round_count }))
      .sort((a, b) => b.date.localeCompare(a.date));
    return {
      market_type: marketType,
      days,
      rounds
    };
  } catch (err) {
    throw err;
  }
}

export async function getRoundChart(
  roundId: string,
  marketType: MarketType,
  symbol: MarketSymbol = "BTCUSDT"
): Promise<RoundChartResponse> {
  const qs = new URLSearchParams({
    round_id: roundId,
    market_type: marketType,
    max_points: "6000",
    symbol
  });
  if (apiSupport.roundChart !== false) {
    try {
      const data = await requestJson<RoundChartResponse>(`/api/chart/round?${qs.toString()}`);
      apiSupport.roundChart = true;
      return normalizeRoundChartResponse(data);
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 404) {
        throw err;
      }
      apiSupport.roundChart = false;
    }
  }
  try {
    const history = await getHistoryRaw(symbol, marketType, 12 * 60, 80_000);
    const points = history.samples
      .filter((s) => s.round_id === roundId)
      .map((s) =>
        normalizeChartPoint({
          timestamp_ms: s.ts_ireland_sample_ms,
          delta_pct: s.delta_pct,
          delta_pct_smooth: s.delta_pct_smooth ?? null,
          mid_yes: s.mid_yes,
          mid_yes_smooth: s.mid_yes_smooth ?? null,
          mid_no: s.mid_no,
          mid_no_smooth: s.mid_no_smooth ?? null,
          best_bid_up: s.bid_yes,
          best_ask_up: s.ask_yes,
          best_bid_down: s.bid_no,
          best_ask_down: s.ask_no,
          round_id: s.round_id,
          time_remaining_s: s.remaining_ms / 1000,
          btc_price: s.binance_price,
          target_price: s.target_price
        })
      );
    const round = history.rounds.find((r) => r.round_id === roundId);
    return {
      points,
      round: round
        ? {
            round_id: round.round_id,
            market_type: marketType,
            start_time_ms: round.start_ts_ms,
            end_time_ms: round.end_ts_ms,
            target_price: round.target_price,
            outcome: roundOutcome(round.settle_price, round.target_price, round.label_up),
            slug: `${marketType}-${Math.floor(round.start_ts_ms / 1000)}`
          }
        : null,
      total_samples: points.length,
      step: 1
    };
  } catch (err) {
    throw err;
  }
}

export async function getHeatmap(
  marketType: MarketType,
  lookbackHours = 72,
  symbol: MarketSymbol = "BTCUSDT"
): Promise<HeatmapResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    lookback_hours: String(lookbackHours),
    symbol
  });
  if (apiSupport.heatmap !== false) {
    const data = await requestJson<HeatmapResponse>(`/api/heatmap?${qs.toString()}`);
    apiSupport.heatmap = true;
    return data;
  }
  const durationMs = marketType === "5m" ? 5 * 60_000 : 15 * 60_000;
  const history = await getHistoryRaw(
    symbol,
    marketType,
    Math.max(60, Math.floor(lookbackHours * 60)),
    120_000
  );
  const bin = new Map<string, { count: number; sum: number }>();
  for (const s of history.samples) {
    const delta = computeDeltaPct(s.delta_pct_smooth ?? s.delta_pct, s.binance_price, s.target_price);
    const midYes = midpointProb(s.bid_yes, s.ask_yes);
    if (delta == null || midYes == null) {
      continue;
    }
    const remainingMs = Math.max(0, Math.min(durationMs, s.remaining_ms));
    const deltaBin = Math.round((delta / 0.05)) * 0.05;
    const timeBin = Math.floor(remainingMs / 30_000) * 30;
    const k = `${deltaBin.toFixed(2)}|${timeBin}`;
    const prev = bin.get(k) ?? { count: 0, sum: 0 };
    prev.count += 1;
    prev.sum += midYes * 100;
    bin.set(k, prev);
  }
  const maxSampleCount = Math.max(0, ...Array.from(bin.values()).map((v) => v.count));
  const cells = Array.from(bin.entries())
    .map(([k, v]) => {
      const [deltaBin, timeBin] = k.split("|");
      return {
        delta_bin_pct: Number(deltaBin),
        time_left_s_bin: Number(timeBin),
        avg_up_price_cents: v.sum / Math.max(1, v.count),
        sample_count: v.count,
        opacity: maxSampleCount > 0 ? v.count / maxSampleCount : 0
      };
    })
    .sort((a, b) => b.time_left_s_bin - a.time_left_s_bin || a.delta_bin_pct - b.delta_bin_pct);
  return {
    market_type: marketType,
    lookback_hours: lookbackHours,
    max_sample_count: maxSampleCount,
    cells
  };
}

export async function getAccuracySeries(
  marketType: MarketType,
  lookbackHours = 24,
  symbol: MarketSymbol = "BTCUSDT"
): Promise<AccuracySeriesResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    lookback_hours: String(lookbackHours),
    symbol,
  });
  if (apiSupport.accuracy !== false) {
    const data = await requestJson<AccuracySeriesResponse>(`/api/accuracy_series?${qs.toString()}`);
    apiSupport.accuracy = true;
    return data;
  }
  const history = await getHistoryRaw(
    symbol,
    marketType,
    Math.max(60, Math.floor(lookbackHours * 60)),
    120_000
  );
  const rounds = [...history.rounds].sort((a, b) => a.end_ts_ms - b.end_ts_ms);
  const rollingWindow = marketType === "5m" ? 40 : 20;
  const rolling: number[] = [];
  const points: Array<{ timestamp_ms: number; round_id: string; accuracy_pct: number; sample_count: number }> = [];
  let sum = 0;
  for (const r of rounds) {
    const outcome = roundOutcome(r.settle_price, r.target_price, r.label_up);
    const targetEval = r.end_ts_ms - (marketType === "5m" ? 60_000 : 180_000);
    let bestDiff = Number.POSITIVE_INFINITY;
    let evalMid: number | null = null;
    for (const s of history.samples) {
      if (s.round_id !== r.round_id) {
        continue;
      }
      const mid = midpointProb(s.bid_yes, s.ask_yes);
      if (mid == null) {
        continue;
      }
      const diff = Math.abs(s.ts_ireland_sample_ms - targetEval);
      if (diff < bestDiff) {
        bestDiff = diff;
        evalMid = mid;
      }
    }
    if (evalMid == null) {
      continue;
    }
    const correct = (evalMid >= 0.5) === (outcome === 1) ? 1 : 0;
    rolling.push(correct);
    sum += correct;
    if (rolling.length > rollingWindow) {
      sum -= rolling.shift() ?? 0;
    }
    points.push({
      timestamp_ms: r.end_ts_ms,
      round_id: r.round_id,
      accuracy_pct: (sum / Math.max(1, rolling.length)) * 100,
      sample_count: rolling.length
    });
  }
  const latest = points[points.length - 1]?.accuracy_pct ?? null;
  return {
    market_type: marketType,
    window: rollingWindow,
    lookback_hours: lookbackHours,
    bucket_minutes: 30,
    processed_rounds: points.length,
    series: points,
    latest_accuracy_pct: latest
  };
}

export interface StrategyPaperQueryOptions {
  source?: "replay" | "live" | "auto";
  profile?: string;
  lookbackMinutes?: number;
  maxTrades?: number;
  fullHistory?: boolean;
  entryThresholdBase?: number;
  entryThresholdCap?: number;
  entryEdgeProb?: number;
  entryMinPotentialCents?: number;
  minHoldMs?: number;
  maxEntriesPerRound?: number;
  cooldownMs?: number;
  liveExecute?: boolean;
  liveQuoteUsdc?: number;
  liveMaxOrders?: number;
  liveEntryOnly?: boolean;
  timeoutMs?: number;
}

export async function getStrategyPaper(
  marketType: MarketType = "5m",
  options: StrategyPaperQueryOptions = {},
  symbol: MarketSymbol = "BTCUSDT"
): Promise<StrategyPaperResponse> {
  const qs = new URLSearchParams({
    source: options.source ?? "replay",
    profile: options.profile ?? "",
    symbol,
    market_type: marketType,
    full_history: options.fullHistory ? "true" : "false"
  });
  if (!options.profile) {
    qs.delete("profile");
  }
  if (options.lookbackMinutes != null) {
    qs.set("lookback_minutes", String(options.lookbackMinutes));
  }
  if (options.maxTrades != null) {
    qs.set("max_trades", String(options.maxTrades));
  }
  if (options.entryThresholdBase != null) {
    qs.set("entry_threshold_base", String(options.entryThresholdBase));
  }
  if (options.entryThresholdCap != null) {
    qs.set("entry_threshold_cap", String(options.entryThresholdCap));
  }
  if (options.entryEdgeProb != null) {
    qs.set("entry_edge_prob", String(options.entryEdgeProb));
  }
  if (options.entryMinPotentialCents != null) {
    qs.set("entry_min_potential_cents", String(options.entryMinPotentialCents));
  }
  if (options.minHoldMs != null) {
    qs.set("min_hold_ms", String(options.minHoldMs));
  }
  if (options.maxEntriesPerRound != null) {
    qs.set("max_entries_per_round", String(options.maxEntriesPerRound));
  }
  if (options.cooldownMs != null) {
    qs.set("cooldown_ms", String(options.cooldownMs));
  }
  if (options.liveExecute != null) {
    qs.set("live_execute", options.liveExecute ? "true" : "false");
  }
  if (options.liveQuoteUsdc != null) {
    qs.set("live_quote_usdc", String(options.liveQuoteUsdc));
  }
  if (options.liveMaxOrders != null) {
    qs.set("live_max_orders", String(options.liveMaxOrders));
  }
  if (options.liveEntryOnly != null) {
    qs.set("live_entry_only", options.liveEntryOnly ? "true" : "false");
  }
  const payload = await requestJson<Record<string, unknown>>(
    `/api/strategy/paper?${qs.toString()}`,
    options.timeoutMs ?? REQUEST_TIMEOUT_MS
  );
  if (typeof payload.error === "string" && payload.error.trim()) {
    throw new Error(payload.error);
  }
  if (!isStrategyPaperResponsePayload(payload)) {
    throw new Error("strategy paper response schema mismatch");
  }
  return payload;
}

export function connectLiveWs(
  symbol: MarketSymbol,
  onData: (payload: WsLivePayload) => void,
  onStatus?: (status: "connecting" | "open" | "closed" | "error") => void
): () => void {
  let closed = false;
  let ws: WebSocket | null = null;
  let retryTimer: number | null = null;
  let retryMs = 1200;

  const connect = () => {
    if (closed) {
      return;
    }
    onStatus?.("connecting");
    ws = new WebSocket(buildWsUrl(`/ws/live?symbol=${encodeURIComponent(symbol)}`));
    ws.onopen = () => {
      retryMs = 1200;
      onStatus?.("open");
    };
    ws.onclose = () => {
      onStatus?.("closed");
      if (!closed) {
        retryTimer = window.setTimeout(connect, retryMs);
        retryMs = Math.min(5000, Math.round(retryMs * 1.7));
      }
    };
    ws.onerror = () => onStatus?.("error");
    ws.onmessage = (ev) => {
      try {
        onData(JSON.parse(ev.data) as WsLivePayload);
      } catch {
        // ignore malformed payload
      }
    };
  };

  connect();

  return () => {
    closed = true;
    if (retryTimer != null) {
      window.clearTimeout(retryTimer);
    }
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close(1000, "client closed");
    } else if (ws) {
      ws.close();
    }
  };
}
