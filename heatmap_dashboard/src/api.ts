import type {
  AccuracySeriesResponse,
  AvailableRoundsResponse,
  ChartPoint,
  ChartResponse,
  HeatmapResponse,
  MarketType,
  RoundChartResponse,
  RoundsResponse,
  StatsResponse,
  WindowType,
  WsLivePayload
} from "./types";

const API_BASE = (import.meta.env.VITE_FORGE_API_BASE as string | undefined)?.trim() ?? "";
const REQUEST_TIMEOUT_MS = 10_000;
const apiSupport: {
  stats: boolean | null;
  chart: boolean | null;
  rounds: boolean | null;
  roundsAvailable: boolean | null;
  roundChart: boolean | null;
  heatmap: boolean | null;
  accuracy: boolean | null;
} = {
  stats: null,
  chart: null,
  rounds: null,
  roundsAvailable: null,
  roundChart: null,
  heatmap: null,
  accuracy: null
};

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

async function requestJson<T>(path: string): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const resp = await fetch(buildHttpUrl(path), { cache: "no-store", signal: controller.signal }).finally(() =>
    window.clearTimeout(timer)
  );
  if (!resp.ok) {
    const text = await resp.text();
    throw new HttpError(resp.status, `HTTP ${resp.status}: ${text}`);
  }
  return (await resp.json()) as T;
}

function windowToMinutes(view: WindowType): number {
  switch (view) {
    case "5m":
      return 5;
    case "15m":
      return 15;
    case "30m":
      return 30;
    case "1h":
      return 60;
    case "2h":
      return 120;
    case "4h":
      return 240;
    default:
      return 0;
  }
}

function maxPointsForView(view: WindowType): number {
  switch (view) {
    case "5m":
      return 5_000;
    case "15m":
      return 12_000;
    case "30m":
      return 24_000;
    case "1h":
      return 48_000;
    case "2h":
      return 50_000;
    case "4h":
      return 50_000;
    default:
      return 12_000;
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
      finiteOrNull(p.mid_yes) ??
      finiteOrNull(p.mid_yes_smooth) ??
      midpointProb(bestBidUp, bestAskUp),
    mid_no:
      finiteOrNull(p.mid_no) ??
      finiteOrNull(p.mid_no_smooth) ??
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

function normalizeChartResponse(data: ChartResponse): ChartResponse {
  const sorted = [...data.points]
    .map((p) => normalizeChartPoint(p))
    .filter((p) => p.timestamp_ms > 0)
    .sort((a, b) => a.timestamp_ms - b.timestamp_ms);
  const deduped: ChartPoint[] = [];
  for (const p of sorted) {
    const prev = deduped[deduped.length - 1];
    if (prev && prev.timestamp_ms === p.timestamp_ms) {
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

async function getHistoryRaw(marketType: MarketType, lookbackMinutes: number, limit: number): Promise<HistoryRaw> {
  const qs = new URLSearchParams({
    lookback_minutes: String(lookbackMinutes),
    limit: String(limit)
  });
  return requestJson<HistoryRaw>(`/api/history/BTCUSDT/${marketType}?${qs.toString()}`);
}

export async function getStats(): Promise<StatsResponse> {
  if (apiSupport.stats !== false) {
    try {
      const data = await requestJson<StatsResponse>("/api/stats");
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
    getHistoryRaw("5m", 6 * 60, 30_000),
    getHistoryRaw("15m", 12 * 60, 30_000)
  ]);
  const totalSamples = h5.sample_count + h15.sample_count;
  const rounds = [...h5.rounds, ...h15.rounds];
  const upCount = rounds.filter((r) => Number(r.label_up) === 1).length;
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

export async function getChart(marketType: MarketType, view: WindowType): Promise<ChartResponse> {
  const minutes = windowToMinutes(view);
  const maxPoints = maxPointsForView(view);
  const qs = new URLSearchParams({
    market_type: marketType,
    minutes: String(minutes),
    max_points: String(maxPoints)
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
    const lookback = minutes === 0 ? 12 * 60 : minutes;
    const estimatedLimit =
      minutes === 0 ? 60_000 : Math.min(80_000, Math.max(4_000, lookback * 700));
    const history = await getHistoryRaw(marketType, lookback, estimatedLimit);
    const nowMs = Date.now();
    const cutoffMs = minutes > 0 ? nowMs - minutes * 60_000 : 0;
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
      .filter((p) => (minutes > 0 ? p.timestamp_ms >= cutoffMs : true))
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
          outcome: Number(r.label_up)
        }))
        .filter((r) => (minutes > 0 ? (r.end_ts_ms ?? 0) >= cutoffMs : true)),
      total_samples: points.length,
      downsampled: sampled.step > 1,
      step: sampled.step
    };
  } catch (err) {
    throw err;
  }
}

export async function getRoundHistory(marketType: MarketType, limit = 200): Promise<RoundsResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    limit: String(limit)
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
    const history = await getHistoryRaw(marketType, 12 * 60, 50_000);
    const latestSampleByRound = new Map<string, HistoryRaw["samples"][number]>();
    for (const s of history.samples) {
      const prev = latestSampleByRound.get(s.round_id);
      if (!prev || s.ts_ireland_sample_ms > prev.ts_ireland_sample_ms) {
        latestSampleByRound.set(s.round_id, s);
      }
    }
    const rows = history.rounds
      .map((r) => {
        const latest = latestSampleByRound.get(r.round_id);
        const target = r.target_price;
        const final = r.settle_price;
        return {
          round_id: r.round_id,
          market_id: r.market_id,
          market_type: marketType,
          start_time_ms: r.start_ts_ms,
          end_time_ms: r.end_ts_ms,
          target_price: target,
          final_btc_price: final,
          outcome: Number(r.label_up),
          delta_pct: target > 0 ? ((final - target) / target) * 100 : null,
          mkt_price_cents:
            latest?.mid_yes != null
              ? latest.mid_yes * 100
              : midpointProb(latest?.bid_yes ?? null, latest?.ask_yes ?? null) != null
                ? (midpointProb(latest?.bid_yes ?? null, latest?.ask_yes ?? null) as number) * 100
                : null
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

export async function getAvailableRounds(marketType: MarketType): Promise<AvailableRoundsResponse> {
  const qs = new URLSearchParams({
    market_type: marketType
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
    const history = await getRoundHistory(marketType, 600);
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
  marketType: MarketType
): Promise<RoundChartResponse> {
  const qs = new URLSearchParams({
    round_id: roundId,
    market_type: marketType,
    max_points: "12000"
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
    const history = await getHistoryRaw(marketType, 12 * 60, 80_000);
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
            outcome: Number(round.label_up),
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
  lookbackHours = 72
): Promise<HeatmapResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    lookback_hours: String(lookbackHours)
  });
  const data = await requestJson<HeatmapResponse>(`/api/heatmap?${qs.toString()}`);
  apiSupport.heatmap = true;
  return data;
}

export async function getAccuracySeries(
  marketType: MarketType,
  lookbackHours = 24,
): Promise<AccuracySeriesResponse> {
  const qs = new URLSearchParams({
    market_type: marketType,
    lookback_hours: String(lookbackHours),
  });
  const data = await requestJson<AccuracySeriesResponse>(`/api/accuracy_series?${qs.toString()}`);
  apiSupport.accuracy = true;
  return data;
}

export function connectLiveWs(
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
    ws = new WebSocket(buildWsUrl("/ws/live"));
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
