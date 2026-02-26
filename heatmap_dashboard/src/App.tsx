import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  connectLiveWs,
  getAccuracySeries,
  getAvailableRounds,
  getChart,
  getCollectorStatus,
  getHeatmap,
  getLatestAllRaw,
  getRoundChart,
  getRoundHistory,
  getStrategyPaper,
  getStats
} from "./api";
import { AccuracyChart } from "./components/AccuracyChart";
import { HeatmapGrid } from "./components/HeatmapGrid";
import { MarketChart } from "./components/MarketChart";
import type {
  AccuracySeriesResponse,
  AccuracyPoint,
  AvailableRoundsResponse,
  ChartPoint,
  ChartResponse,
  CollectorStatusResponse,
  HeatmapCell,
  HeatmapResponse,
  LiveSnapshot,
  MarketType,
  RoundHistoryRow,
  RoundChartResponse,
  RoundsResponse,
  StrategyPaperResponse,
  StatsResponse,
  WindowType
} from "./types";

const WINDOW_OPTIONS: Array<{ value: WindowType; label: string }> = [
  { value: "5m", label: "5m" },
  { value: "15m", label: "15m" },
  { value: "30m", label: "30m" },
  { value: "1h", label: "1h" },
  { value: "2h", label: "2h" },
  { value: "4h", label: "4h" },
  { value: "all", label: "All" }
];

const LIVE_POLL_MS = 900;
const WS_STALE_FALLBACK_MS = 3000;
const LIVE_UI_MIN_INTERVAL_MS = 900;
const ET_TIMEZONE = "America/New_York";
const COLLECTOR_POLL_MS = 5_000;
const STRATEGY_POLL_MS = 5_000;
type StrategyPaperSource = "replay" | "live";
const STRATEGY_PAPER_PROFILE = Object.freeze({
  lookbackMinutes: 24 * 60,
  maxTrades: 320,
  fullHistory: false,
  useAutotune: false
});
const STRATEGY_LIVE_PROFILE = Object.freeze({
  liveExecute: false,
  liveQuoteUsdc: 1,
  liveMaxOrders: 1,
  liveEntryOnly: true
});

type TimeMode = "local" | "et";

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

function maxLocalPointsForView(view: WindowType): number {
  switch (view) {
    case "5m":
      return 3_500;
    case "15m":
      return 5_500;
    case "30m":
      return 7_500;
    case "1h":
      return 10_000;
    case "2h":
      return 14_000;
    case "4h":
      return 18_000;
    default:
      return 26_000;
  }
}

function trimPointsToWindow(points: ChartPoint[], view: WindowType, nowMs: number): ChartPoint[] {
  if (points.length === 0) {
    return points;
  }
  let next = points;
  const viewMinutes = windowToMinutes(view);
  if (viewMinutes > 0) {
    const latestTs = points[points.length - 1]?.timestamp_ms ?? nowMs;
    const anchorTs = Math.max(nowMs, latestTs);
    const cutoffTs = anchorTs - viewMinutes * 60_000;
    const start = points.findIndex((p) => p.timestamp_ms >= cutoffTs);
    if (start >= 0) {
      next = points.slice(start);
    } else {
      next = points.slice(-1);
    }
  }
  const cap = maxLocalPointsForView(view);
  if (next.length > cap) {
    next = next.slice(next.length - cap);
  }
  return next;
}

function trimChartToWindow(chart: ChartResponse, view: WindowType, nowMs: number): ChartResponse {
  const points = trimPointsToWindow(chart.points, view, nowMs);
  return {
    ...chart,
    points,
    rounds: chart.rounds
  };
}

function formatUsd(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) {
    return "--";
  }
  return `$${v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatPct(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) {
    return "--";
  }
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(4)}%`;
}

function formatCent(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) {
    return "--";
  }
  return `${(v * 100).toFixed(1)}¢`;
}

function formatPercentValue(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) {
    return "--";
  }
  return `${(v * 100).toFixed(1)}%`;
}

function midpointProb(
  bid: number | null | undefined,
  ask: number | null | undefined
): number | null {
  const hasBid = bid != null && Number.isFinite(bid);
  const hasAsk = ask != null && Number.isFinite(ask);
  if (hasBid && hasAsk) {
    return Math.max(0, Math.min(1, ((bid as number) + (ask as number)) * 0.5));
  }
  if (hasBid) {
    return Math.max(0, Math.min(1, bid as number));
  }
  if (hasAsk) {
    return Math.max(0, Math.min(1, ask as number));
  }
  return null;
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function finiteOrNull(v: number | null | undefined): number | null {
  return v != null && Number.isFinite(v) ? v : null;
}

function quoteFromPreferredMid(
  preferredMid: number | null | undefined,
  bid: number | null | undefined,
  ask: number | null | undefined,
  fallbackBid: number | null | undefined,
  fallbackAsk: number | null | undefined
): { bid: number | null; ask: number | null } {
  const b = finiteOrNull(bid);
  const a = finiteOrNull(ask);
  const fb = finiteOrNull(fallbackBid);
  const fa = finiteOrNull(fallbackAsk);

  const raw = normalizeQuote(b, a, fb, fa);
  const rawMid = stableMidpointProb(raw.bid, raw.ask, null);
  const mid = finiteOrNull(preferredMid) ?? rawMid;
  if (mid == null) {
    return raw;
  }

  const rawSpread =
    raw.bid != null && raw.ask != null ? Math.max(0, raw.ask - raw.bid) : 0;
  const spread = clamp(rawSpread > 0 ? rawSpread : 0.008, 0.002, 0.08);
  const nextBid = clamp(mid - spread * 0.5, 0, 1);
  const nextAsk = clamp(mid + spread * 0.5, 0, 1);
  return {
    bid: Math.min(nextBid, nextAsk),
    ask: Math.max(nextBid, nextAsk)
  };
}

function stableMidpointProb(
  bid: number | null | undefined,
  ask: number | null | undefined,
  fallback: number | null
): number | null {
  const b = bid != null && Number.isFinite(bid) ? bid : null;
  const a = ask != null && Number.isFinite(ask) ? ask : null;
  if (b != null && a != null) {
    const lo = Math.min(b, a);
    const hi = Math.max(b, a);
    return clamp((lo + hi) * 0.5, 0, 1);
  }
  if (b != null) {
    return clamp(b, 0, 1);
  }
  if (a != null) {
    return clamp(a, 0, 1);
  }
  if (fallback != null && Number.isFinite(fallback)) {
    return clamp(fallback, 0, 1);
  }
  return null;
}

function normalizeQuote(
  bid: number | null | undefined,
  ask: number | null | undefined,
  fallbackBid: number | null | undefined,
  fallbackAsk: number | null | undefined
): { bid: number | null; ask: number | null } {
  const b = bid != null && Number.isFinite(bid) ? clamp(bid, 0, 1) : null;
  const a = ask != null && Number.isFinite(ask) ? clamp(ask, 0, 1) : null;
  const fb = fallbackBid != null && Number.isFinite(fallbackBid) ? clamp(fallbackBid, 0, 1) : null;
  const fa = fallbackAsk != null && Number.isFinite(fallbackAsk) ? clamp(fallbackAsk, 0, 1) : null;

  let nextBid = b ?? fb;
  let nextAsk = a ?? fa;
  if (nextBid == null && nextAsk == null) {
    return { bid: null, ask: null };
  }
  if (nextBid == null) {
    nextBid = nextAsk;
  }
  if (nextAsk == null) {
    nextAsk = nextBid;
  }
  if ((nextBid as number) > (nextAsk as number)) {
    const t = nextBid;
    nextBid = nextAsk;
    nextAsk = t;
  }
  return { bid: nextBid, ask: nextAsk };
}

function displayUpProb(snapshot: LiveSnapshot | null): number | null {
  if (!snapshot) {
    return null;
  }
  return stableMidpointProb(snapshot.best_bid_up, snapshot.best_ask_up, null);
}

function displayDownProb(snapshot: LiveSnapshot | null): number | null {
  if (!snapshot) {
    return null;
  }
  return stableMidpointProb(snapshot.best_bid_down, snapshot.best_ask_down, null);
}

function normalizeLiveSnapshot(
  prevStable: LiveSnapshot | null,
  nextRaw: LiveSnapshot
): LiveSnapshot {
  const preferredUpMid =
    finiteOrNull(nextRaw.mid_yes_smooth) ?? finiteOrNull(nextRaw.mid_yes);
  const preferredDownMid =
    finiteOrNull(nextRaw.mid_no_smooth) ?? finiteOrNull(nextRaw.mid_no);

  const up = quoteFromPreferredMid(
    preferredUpMid,
    nextRaw.best_bid_up,
    nextRaw.best_ask_up,
    prevStable?.best_bid_up ?? null,
    prevStable?.best_ask_up ?? null
  );
  const down = quoteFromPreferredMid(
    preferredDownMid,
    nextRaw.best_bid_down,
    nextRaw.best_ask_down,
    prevStable?.best_bid_down ?? null,
    prevStable?.best_ask_down ?? null
  );
  let upMid = preferredUpMid ?? stableMidpointProb(up.bid, up.ask, null);
  let downMid = preferredDownMid ?? stableMidpointProb(down.bid, down.ask, null);

  if (upMid == null && downMid != null) {
    upMid = clamp(1 - downMid, 0, 1);
  } else if (downMid == null && upMid != null) {
    downMid = clamp(1 - upMid, 0, 1);
  }

  if (upMid != null && downMid != null) {
    const sum = upMid + downMid;
    if (sum > 0) {
      upMid = clamp(upMid / sum, 0, 1);
      downMid = clamp(1 - upMid, 0, 1);
    }
  }

  if (upMid == null || downMid == null) {
    return {
      ...nextRaw,
      best_bid_up: up.bid,
      best_ask_up: up.ask,
      best_bid_down: down.bid,
      best_ask_down: down.ask
    };
  }

  if (prevStable && prevStable.round_id === nextRaw.round_id && prevStable.timestamp_ms > 0) {
    const dtSec = Math.max(0.05, (nextRaw.timestamp_ms - prevStable.timestamp_ms) / 1000);
    const remaining = Math.max(0, nextRaw.time_remaining_s ?? 0);
    const maxStepPerSec = remaining <= 30 ? 1.2 : remaining <= 60 ? 0.8 : 0.45;
    const maxStep = maxStepPerSec * dtSec;
    const prevMid = finiteOrNull(prevStable.mid_yes_smooth) ?? finiteOrNull(prevStable.mid_yes);
    if (prevMid != null) {
      upMid = clamp(upMid, prevMid - maxStep, prevMid + maxStep);
      downMid = clamp(1 - upMid, 0, 1);
    }
  }

  // Keep display quotes deterministic and consistent with normalized mids.
  const upSpread = up.bid != null && up.ask != null ? Math.max(0, up.ask - up.bid) : 0;
  const downSpread = down.bid != null && down.ask != null ? Math.max(0, down.ask - down.bid) : 0;
  const nextUpBid = clamp(upMid - upSpread * 0.5, 0, 1);
  const nextUpAsk = clamp(upMid + upSpread * 0.5, 0, 1);
  const nextDownBid = clamp(downMid - downSpread * 0.5, 0, 1);
  const nextDownAsk = clamp(downMid + downSpread * 0.5, 0, 1);

  const velocity = nextRaw.velocity_bps_per_sec ?? null;
  const acceleration = nextRaw.acceleration ?? null;

  return {
    ...nextRaw,
    mid_yes: upMid,
    mid_no: downMid,
    mid_yes_smooth: finiteOrNull(nextRaw.mid_yes_smooth) ?? upMid,
    mid_no_smooth: finiteOrNull(nextRaw.mid_no_smooth) ?? downMid,
    best_bid_up: Math.min(nextUpBid, nextUpAsk),
    best_ask_up: Math.max(nextUpBid, nextUpAsk),
    best_bid_down: Math.min(nextDownBid, nextDownAsk),
    best_ask_down: Math.max(nextDownBid, nextDownAsk),
    velocity_bps_per_sec: velocity,
    acceleration
  };
}

function formatCentFromCents(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) {
    return "--";
  }
  return `${v.toFixed(1)}¢`;
}

function formatCountdown(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) {
    return "0:00";
  }
  const total = Math.floor(seconds);
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function formatTimeZoneLabel(mode: TimeMode): string {
  return mode === "et" ? "ET" : "Local";
}

function formatTime(ts: number | null | undefined, mode: TimeMode): string {
  if (!ts) {
    return "--";
  }
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: mode === "et" ? ET_TIMEZONE : undefined,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(ts));
}

function formatAgeMs(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v) || v < 0) {
    return "--";
  }
  if (v < 1_000) {
    return `${Math.round(v)}ms`;
  }
  return `${(v / 1_000).toFixed(v >= 10_000 ? 0 : 1)}s`;
}

function formatBps(v: number | null | undefined, unit = "bps/s"): string {
  if (v == null || !Number.isFinite(v)) {
    return "--";
  }
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(2)} ${unit}`;
}

function summarizeGaps(points: ChartPoint[]): { count: number; maxGapMs: number } {
  if (points.length < 2) {
    return { count: 0, maxGapMs: 0 };
  }
  let prev = -1;
  const diffs: number[] = [];
  for (const p of points) {
    const ts = p.timestamp_ms;
    if (!Number.isFinite(ts) || ts <= 0) {
      continue;
    }
    if (prev > 0 && ts > prev) {
      diffs.push(ts - prev);
    }
    prev = ts;
  }
  if (diffs.length === 0) {
    return { count: 0, maxGapMs: 0 };
  }
  const sorted = [...diffs].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)] ?? 100;
  const threshold = Math.max(3000, Math.min(120_000, median * 8));
  let count = 0;
  let maxGapMs = 0;
  for (const d of diffs) {
    if (d > threshold) {
      count += 1;
      if (d > maxGapMs) {
        maxGapMs = d;
      }
    }
  }
  return { count, maxGapMs };
}

function roundCard(title: string, snapshot: LiveSnapshot | null) {
  const up = displayUpProb(snapshot);
  const down = displayDownProb(snapshot);
  return (
    <article className="round-card">
      <div className="card-head">
        <h3>{title}</h3>
        <span className="countdown">倒计时 {formatCountdown(snapshot?.time_remaining_s)}</span>
      </div>
      <div className="round-price">{formatUsd(snapshot?.btc_price)}</div>
      <div className="round-sub">
        <span>目标价 {formatUsd(snapshot?.target_price)}</span>
        <span className={(snapshot?.delta_pct ?? 0) >= 0 ? "up" : "down"}>
          ▲ {formatPct(snapshot?.delta_pct)}
        </span>
        <span>速度 {formatBps(snapshot?.velocity_bps_per_sec, "bps/s")}</span>
        <span>加速度 {formatBps(snapshot?.acceleration, "bps/s²")}</span>
      </div>
      <div className="round-odds">
        <div>
          <label>看涨</label>
          <strong className="up">{formatCent(up)}</strong>
          <small>
            {formatCent(snapshot?.best_bid_up)} / {formatCent(snapshot?.best_ask_up)}
          </small>
        </div>
        <div>
          <label>看跌</label>
          <strong className="down">{formatCent(down)}</strong>
          <small>
            {formatCent(snapshot?.best_bid_down)} / {formatCent(snapshot?.best_ask_down)}
          </small>
        </div>
      </div>
    </article>
  );
}

interface MarketSectionProps {
  title: string;
  marketType: MarketType;
  live: LiveSnapshot | null;
  timeMode: TimeMode;
  windowType: WindowType;
  onWindowChange: (w: WindowType) => void;
  chart: ChartResponse | null;
  loading: boolean;
}

function MarketSection({
  title,
  marketType,
  live,
  timeMode,
  windowType,
  onWindowChange,
  chart,
  loading
}: MarketSectionProps) {
  const gapInfo = useMemo(() => summarizeGaps(chart?.points ?? []), [chart?.points]);
  const up = displayUpProb(live);
  const down = displayDownProb(live);

  return (
    <section className="panel market-panel">
      <header className="panel-head">
        <div>
          <h2>{title}</h2>
          <p className="muted">
            BTC {formatUsd(live?.btc_price)} | 目标价 {formatUsd(live?.target_price)} | Δ{" "}
            <span className={(live?.delta_pct ?? 0) >= 0 ? "up" : "down"}>{formatPct(live?.delta_pct)}</span>
          </p>
        </div>
        <div className="btn-group">
          {loading ? <span className="loading-chip">加载中...</span> : null}
          {WINDOW_OPTIONS.map((w) => (
            <button
              key={`${marketType}-${w.value}`}
              className={windowType === w.value ? "active" : ""}
              onClick={() => onWindowChange(w.value)}
            >
              {w.label}
            </button>
          ))}
        </div>
      </header>
      <div className="market-kpi">
        <span>
          看涨 <b className="up">{formatCent(up)}</b>
        </span>
        <span>
          看跌 <b className="down">{formatCent(down)}</b>
        </span>
        <span>倒计时 {formatCountdown(live?.time_remaining_s)}</span>
        <span>速度 {formatBps(live?.velocity_bps_per_sec, "bps/s")}</span>
        <span>加速度 {formatBps(live?.acceleration, "bps/s²")}</span>
      </div>
      <MarketChart
        points={chart?.points ?? []}
        rounds={chart?.rounds ?? []}
        windowType={windowType}
        timeMode={timeMode}
        height={360}
      />
      <div className="panel-foot">
        <span>
          {chart?.total_samples.toLocaleString() ?? "0"} 样本
          {chart?.downsampled ? ` (downsample x${chart.step})` : ""}
          {gapInfo.count > 0 ? ` | 缺口 ${gapInfo.count} (max ${(gapInfo.maxGapMs / 1000).toFixed(1)}s)` : ""}
        </span>
        <span>{chart?.rounds.length.toLocaleString() ?? "0"} 轮</span>
      </div>
    </section>
  );
}

const MemoMarketSection = memo(MarketSection);

interface HeatmapPanelProps {
  heatmapTab: MarketType;
  onTabChange: (v: MarketType) => void;
  cells: HeatmapCell[];
}

const HeatmapPanel = memo(function HeatmapPanel({ heatmapTab, onTabChange, cells }: HeatmapPanelProps) {
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>市场定价热力图</h2>
          <p className="muted">
            按 Δ价差与剩余时间统计的看涨均价分布，用于观察在不同价差区间下市场如何定价。
          </p>
        </div>
        <div className="btn-group">
          <button className={heatmapTab === "5m" ? "active" : ""} onClick={() => onTabChange("5m")}>
            5m
          </button>
          <button className={heatmapTab === "15m" ? "active" : ""} onClick={() => onTabChange("15m")}>
            15m
          </button>
        </div>
      </header>
      <HeatmapGrid cells={cells} marketType={heatmapTab} />
    </section>
  );
});

interface AccuracyPanelProps {
  accuracyTab: MarketType;
  onTabChange: (v: MarketType) => void;
  points: AccuracyPoint[];
  timeMode: TimeMode;
}

const AccuracyPanel = memo(function AccuracyPanel({ accuracyTab, onTabChange, points, timeMode }: AccuracyPanelProps) {
  const summary = useMemo(() => {
    if (points.length === 0) {
      return null;
    }
    const vals = points
      .filter((p) => p.sample_count > 0)
      .map((p) => p.accuracy_pct)
      .filter((v) => Number.isFinite(v));
    if (vals.length === 0) {
      return null;
    }
    const latest = vals[vals.length - 1] ?? null;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
    return { latest, min, max, avg, samples: vals.length };
  }, [points]);

  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>市场方向一致率（过去24小时）</h2>
          <p className="muted">
            5m 使用最近40轮滚动窗口，15m 使用最近20轮滚动窗口；图表按30分钟步进，并以阶梯线展示有效样本。
          </p>
        </div>
        <div className="btn-group">
          <button className={accuracyTab === "5m" ? "active" : ""} onClick={() => onTabChange("5m")}>
            5m
          </button>
          <button className={accuracyTab === "15m" ? "active" : ""} onClick={() => onTabChange("15m")}>
            15m
          </button>
        </div>
      </header>
      <div className="info-cards compact">
        <article className="info-card">
          <span>当前准确率</span>
          <strong>{summary ? formatPercentValue((summary.latest ?? 0) / 100) : "--"}</strong>
          <small>{accuracyTab === "5m" ? "窗口：40轮（5m）" : "窗口：20轮（15m）"}</small>
        </article>
        <article className="info-card">
          <span>区间范围</span>
          <strong>
            {summary ? `${summary.min.toFixed(1)}% ~ ${summary.max.toFixed(1)}%` : "--"}
          </strong>
          <small>min / max</small>
        </article>
        <article className="info-card">
          <span>平均准确率</span>
          <strong>{summary ? `${summary.avg.toFixed(1)}%` : "--"}</strong>
          <small>样本点：{summary?.samples ?? 0}</small>
        </article>
      </div>
      <AccuracyChart points={points} timeMode={timeMode} />
    </section>
  );
});

interface RoundHistoryPanelProps {
  roundHistoryTab: MarketType;
  onTabChange: (v: MarketType) => void;
  rows: RoundHistoryRow[];
  timeMode: TimeMode;
}

const RoundHistoryPanel = memo(function RoundHistoryPanel({
  roundHistoryTab,
  onTabChange,
  rows,
  timeMode
}: RoundHistoryPanelProps) {
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>轮次历史</h2>
          <p className="muted">仅展示最近20条记录，避免无上限增长影响可读性。</p>
        </div>
        <div className="btn-group">
          <button className={roundHistoryTab === "5m" ? "active" : ""} onClick={() => onTabChange("5m")}>
            5m
          </button>
          <button className={roundHistoryTab === "15m" ? "active" : ""} onClick={() => onTabChange("15m")}>
            15m
          </button>
        </div>
      </header>
      <div className="table-wrap">
        <table className="history-table">
          <thead>
            <tr>
              <th>结束时间</th>
              <th>目标价</th>
              <th>结算BTC</th>
              <th>偏离</th>
              <th>结果</th>
              <th>市场价</th>
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 20).map((r) => (
              <tr key={r.round_id}>
                <td>{formatTime(r.end_time_ms, timeMode)}</td>
                <td>{formatUsd(r.target_price)}</td>
                <td>{formatUsd(r.final_btc_price)}</td>
                <td className={(r.delta_pct ?? 0) >= 0 ? "up" : "down"}>{formatPct(r.delta_pct)}</td>
                <td>
                  <span className={`chip ${r.outcome === 1 ? "up" : "down"}`}>
                    {r.outcome === 1 ? "UP" : "DOWN"}
                  </span>
                </td>
                <td>{formatCentFromCents(r.mkt_price_cents)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
});

interface StrategyPanelProps {
  data: StrategyPaperResponse | null;
  loading: boolean;
  timeMode: TimeMode;
  marketType: MarketType;
  source: StrategyPaperSource;
  onMarketTypeChange: (marketType: MarketType) => void;
  onSourceChange: (source: StrategyPaperSource) => void;
}

const StrategyPanel = memo(function StrategyPanel({
  data,
  loading,
  timeMode,
  marketType,
  source,
  onMarketTypeChange,
  onSourceChange
}: StrategyPanelProps) {
  const current = data?.current ?? null;
  const currentView = useMemo(() => {
    if (!current) {
      return {
        score: null as number | null,
        entryThreshold: null as number | null,
        confidencePct: null as number | null,
        pUpPct: null as number | null,
        deltaPct: null as number | null,
        remainingS: null as number | null,
        timestampMs: null as number | null,
        roundId: null as string | null,
      };
    }
    const raw = current as Record<string, unknown>;
    const asNum = (v: unknown): number | null =>
      typeof v === "number" && Number.isFinite(v) ? v : null;
    const asStr = (v: unknown): string | null => (typeof v === "string" && v.trim() ? v : null);
    const score = asNum(raw.score);
    const entryThreshold = asNum(raw.entry_threshold);
    const pFairUp = asNum(raw.p_fair_up);
    const confidence = asNum(raw.confidence);
    const pUpPctRaw = asNum(raw.p_up_pct);
    const deltaPctRaw = asNum(raw.delta_pct);
    const remainingSRaw = asNum(raw.remaining_s);
    const remainingMsRaw = asNum(raw.remaining_ms);
    const timestampMs = asNum(raw.timestamp_ms);
    const roundId = asStr(raw.round_id);
    const confidencePct =
      confidence != null
        ? confidence * 100
        : pFairUp != null
        ? Math.max(pFairUp, 1 - pFairUp) * 100
        : null;
    const pUpPct = pUpPctRaw != null ? pUpPctRaw : pFairUp != null ? pFairUp * 100 : null;
    const deltaPct = deltaPctRaw != null ? deltaPctRaw : asNum(raw.edge_prob);
    const remainingS = remainingSRaw != null ? remainingSRaw : remainingMsRaw != null ? remainingMsRaw / 1000 : null;
    return {
      score,
      entryThreshold,
      confidencePct,
      pUpPct,
      deltaPct,
      remainingS,
      timestampMs,
      roundId,
    };
  }, [current]);
  const liveExec = data?.live_execution;
  const liveState = liveExec?.state_machine;
  const liveOrders = liveExec?.execution?.orders ?? [];
  const liveEvents = (liveExec?.events ?? []) as Array<Record<string, unknown>>;
  const parity = liveExec?.parity_check;
  const liveNetPnl = data?.summary?.net_pnl_cents ?? data?.summary?.total_pnl_cents ?? 0;
  const liveGrossPnl = data?.summary?.gross_pnl_cents ?? 0;
  const liveTotalCost = data?.summary?.total_cost_cents ?? 0;
  const liveWinRate = data?.summary?.win_rate_pct ?? 0;
  const liveDrawdown = data?.summary?.max_drawdown_cents ?? 0;
  const parityPaperEntry = parity?.paper?.entry_count ?? 0;
  const parityPaperExit = parity?.paper?.exit_count ?? 0;
  const parityLiveSubmitEntry = parity?.live?.submitted_entry_count ?? 0;
  const parityLiveSubmitExit = parity?.live?.submitted_exit_count ?? 0;
  const parityLiveAccepted = parity?.live?.accepted_count ?? 0;
  const parityLiveRejected = parity?.live?.rejected_count ?? 0;
  const parityLiveSkipped = parity?.live?.skipped_count ?? 0;
  const liveSubmittedTotal = parity?.live?.submitted_count ?? 0;
  const liveAcceptedRate = liveSubmittedTotal > 0 ? (parityLiveAccepted / liveSubmittedTotal) * 100 : 0;
  const parityRows = [
    { label: "Paper信号", value: parity?.paper?.decision_count ?? 0 },
    { label: "门禁通过", value: liveExec?.gated?.selected_count ?? 0 },
    { label: "提交网关", value: liveSubmittedTotal },
    { label: "网关接受", value: parityLiveAccepted },
  ];
  const parityMax = Math.max(1, ...parityRows.map((r) => r.value));
  const issueReasons = useMemo(() => {
    const buckets = new Map<string, number>();
    for (const row of liveExec?.gated?.skipped_decisions ?? []) {
      const reason = row?.reason?.trim() || "gate_unknown";
      buckets.set(reason, (buckets.get(reason) ?? 0) + 1);
    }
    for (const ev of liveEvents) {
      const accepted = typeof ev.accepted === "boolean" ? ev.accepted : null;
      if (accepted === false) {
        const reason = typeof ev.reason === "string" && ev.reason.trim() ? ev.reason.trim() : "event_rejected";
        buckets.set(reason, (buckets.get(reason) ?? 0) + 1);
      }
    }
    return [...buckets.entries()]
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 6);
  }, [liveExec?.gated?.skipped_decisions, liveEvents]);
  const liveStateLabel = liveState?.state === "in_position" ? "持仓中" : "空仓";
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>策略 Paper（{marketType} 全时段）</h2>
          <p className="muted">单模型双向策略：自动判断 UP/DOWN，只验证入场与出场，不做加仓和反向。</p>
        </div>
        <div className="panel-actions">
          <div className="btn-group">
            <button className={marketType === "5m" ? "active" : ""} onClick={() => onMarketTypeChange("5m")}>
              5m
            </button>
            <button className={marketType === "15m" ? "active" : ""} onClick={() => onMarketTypeChange("15m")}>
              15m
            </button>
          </div>
          <div className="btn-group">
            <button className={source === "replay" ? "active" : ""} onClick={() => onSourceChange("replay")}>
              Paper模拟
            </button>
            <button className={source === "live" ? "active" : ""} onClick={() => onSourceChange("live")}>
              真实交易
            </button>
          </div>
          {loading ? (
            <span className="loading-chip">计算中...</span>
          ) : (
            <span className="loading-chip">{source === "live" ? "实时策略" : "模拟策略"}</span>
          )}
        </div>
      </header>
      <div className="info-cards compact strategy-matrix">
        <article className="info-card">
          <span>当前动作</span>
          <strong className={current?.suggested_action?.includes("UP") ? "up" : current?.suggested_action?.includes("DOWN") ? "down" : ""}>
            {current?.suggested_action ?? "--"}
          </strong>
          <small>
            {current ? `${formatTime(currentView.timestampMs, timeMode)} · ${currentView.roundId ?? "--"}` : "等待数据"}
          </small>
        </article>
        <article className="info-card">
          <span>信号强度 / 阈值</span>
          <strong>
            {current && currentView.score != null && currentView.entryThreshold != null
              ? `${currentView.score.toFixed(3)} / ${currentView.entryThreshold.toFixed(3)}`
              : "--"}
          </strong>
          <small>置信度：{currentView.confidencePct != null ? `${currentView.confidencePct.toFixed(1)}%` : "--"}</small>
        </article>
        <article className="info-card">
          <span>累计净收益</span>
          <strong className={(data?.summary.net_pnl_cents ?? data?.summary.total_pnl_cents ?? 0) >= 0 ? "up" : "down"}>
            {data ? `${(data.summary.net_pnl_cents ?? data.summary.total_pnl_cents).toFixed(2)}¢` : "--"}
          </strong>
          <small>
            交易 {data?.summary.trade_count ?? 0} · 胜率 {data ? `${data.summary.win_rate_pct.toFixed(1)}%` : "--"} · 窗口 {data?.lookback_minutes ?? "--"}m
          </small>
        </article>
        <article className="info-card">
          <span>均值 / 回撤</span>
          <strong>
            {data ? `${data.summary.avg_pnl_cents.toFixed(2)}¢ / ${data.summary.max_drawdown_cents.toFixed(2)}¢` : "--"}
          </strong>
          <small>平均每笔 / 最大回撤</small>
        </article>
        <article className="info-card">
          <span>毛收益 / 总成本</span>
          <strong>
            {data ? `${(data.summary.gross_pnl_cents ?? 0).toFixed(2)}¢ / ${(data.summary.total_cost_cents ?? 0).toFixed(2)}¢` : "--"}
          </strong>
          <small>
            净利润率 {data ? `${(data.summary.net_margin_pct ?? 0).toFixed(2)}%` : "--"}
          </small>
        </article>
        <article className="info-card">
          <span>市场状态</span>
          <strong>
            {currentView.pUpPct != null ? `UP ${currentView.pUpPct.toFixed(1)}%` : "--"}
          </strong>
          <small>
            Δ {currentView.deltaPct != null ? `${currentView.deltaPct.toFixed(4)}%` : "--"} · 剩余{" "}
            {currentView.remainingS != null ? `${currentView.remainingS.toFixed(1)}s` : "--"} · 样本 {data?.samples ?? 0}
          </small>
        </article>
      </div>
      {source === "live" ? (
        <div className="live-execution-wrap">
          <div className="info-cards compact live-kpi-grid">
            <article className="info-card">
              <span>Live净收益</span>
              <strong className={liveNetPnl >= 0 ? "up" : "down"}>{`${liveNetPnl.toFixed(2)}¢`}</strong>
              <small>
                胜率 {liveWinRate.toFixed(1)}% · 回撤 {liveDrawdown.toFixed(2)}¢
              </small>
            </article>
            <article className="info-card">
              <span>毛收益 / 总成本</span>
              <strong>{`${liveGrossPnl.toFixed(2)}¢ / ${liveTotalCost.toFixed(2)}¢`}</strong>
              <small>
                净利润率 {(data?.summary?.net_margin_pct ?? 0).toFixed(2)}%
              </small>
            </article>
            <article className="info-card">
              <span>执行模式</span>
              <strong>{liveExec?.execution?.mode ?? "--"}</strong>
              <small>
                source={data?.source ?? "--"} · engine={data?.strategy_alias ?? "--"}
              </small>
            </article>
            <article className="info-card">
              <span>持仓状态机</span>
              <strong className={liveState?.state === "in_position" ? "up" : ""}>
                {liveStateLabel}
                {liveState?.side ? ` · ${liveState.side}` : ""}
              </strong>
              <small>
                入场 {liveState?.entry_price_cents != null ? `${liveState.entry_price_cents.toFixed(2)}¢` : "--"} ·
                轮次 {liveState?.entry_round_id ?? "--"}
              </small>
            </article>
            <article className="info-card">
              <span>订单成功率</span>
              <strong className={liveAcceptedRate >= 70 ? "up" : liveAcceptedRate >= 40 ? "warn" : "down"}>
                {liveSubmittedTotal > 0 ? `${liveAcceptedRate.toFixed(1)}%` : "--"}
              </strong>
              <small>
                提交 {liveSubmittedTotal} · 接受 {parityLiveAccepted} · 拒绝 {parityLiveRejected}
              </small>
            </article>
            <article className="info-card">
              <span>同策略一致性</span>
              <strong className={parity?.level === "critical" ? "down" : parity?.level === "warn" ? "warn" : "up"}>
                {parity?.status ?? "--"}
              </strong>
              <small>
                Paper 入/出 {parityPaperEntry}/{parityPaperExit} · Live 入/出 {parityLiveSubmitEntry}/{parityLiveSubmitExit}
              </small>
            </article>
          </div>

          <div className="live-block-grid">
            <article className="live-block parity-block">
              <header>
                <h3>同策略执行漏斗</h3>
                <small>Paper 信号到 Live 成交链路</small>
              </header>
              <div className="parity-funnel">
                {parityRows.map((row) => (
                  <div key={row.label} className="parity-row">
                    <label>{row.label}</label>
                    <div className="parity-bar">
                      <span style={{ width: `${Math.max(4, (row.value / parityMax) * 100)}%` }} />
                    </div>
                    <strong>{row.value}</strong>
                  </div>
                ))}
              </div>
              <div className="parity-meta muted">
                skipped {parityLiveSkipped} · rejected {parityLiveRejected}
              </div>
            </article>

            <article className="live-block diag-block">
              <header>
                <h3>差异诊断</h3>
                <small>为什么 Paper 有信号但 Live 可能没成交</small>
              </header>
              <div className="diag-main">
                <span className="muted">当前状态</span>
                <strong className={parity?.level === "critical" ? "down" : parity?.level === "warn" ? "warn" : "up"}>
                  {parity?.status ?? "--"}
                </strong>
              </div>
              <ul className="diag-list">
                {issueReasons.map((item) => (
                  <li key={item.reason}>
                    <span>{item.reason}</span>
                    <strong>{item.count}</strong>
                  </li>
                ))}
                {issueReasons.length === 0 ? <li><span>暂无异常</span><strong>0</strong></li> : null}
              </ul>
            </article>
          </div>

          <div className="live-tables-grid">
            <div className="table-wrap">
              <h3 className="table-title">Live执行明细（最近8条）</h3>
              <table className="history-table live-exec-table">
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>动作</th>
                    <th>方向</th>
                    <th>轮次</th>
                    <th>价格(¢)</th>
                    <th>结果</th>
                    <th>端点</th>
                  </tr>
                </thead>
                <tbody>
                  {liveOrders.slice(-8).reverse().map((order, idx) => (
                    <tr key={`${order.decision_key ?? "k"}-${idx}`}>
                      <td>
                        {formatTime(
                          typeof order.decision?.ts_ms === "number" ? order.decision.ts_ms : null,
                          timeMode
                        )}
                      </td>
                      <td>{order.decision?.action ?? "--"}</td>
                      <td>{order.decision?.side ?? "--"}</td>
                      <td>{order.decision?.round_id ?? "--"}</td>
                      <td>
                        {order.decision?.price_cents != null && Number.isFinite(order.decision.price_cents)
                          ? Number(order.decision.price_cents).toFixed(2)
                          : "--"}
                      </td>
                      <td className={order.accepted ? "up" : "down"}>
                        {order.accepted ? "ACCEPTED" : "SKIP/REJECT"}
                      </td>
                      <td>{order.endpoint ?? "--"}</td>
                    </tr>
                  ))}
                  {liveOrders.length === 0 ? (
                    <tr>
                      <td colSpan={7}>暂无执行记录（当前可能为空仓或未触发有效信号）。</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">状态机事件（最近8条）</h3>
              <table className="history-table live-event-table">
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>动作</th>
                    <th>方向</th>
                    <th>原因</th>
                    <th>状态</th>
                  </tr>
                </thead>
                <tbody>
                  {liveEvents.slice(-8).reverse().map((ev, idx) => {
                    const tsMs = typeof ev.ts_ms === "number" ? ev.ts_ms : null;
                    const accepted = typeof ev.accepted === "boolean" ? ev.accepted : null;
                    const action = typeof ev.action === "string" ? ev.action : "--";
                    const side = typeof ev.side === "string" ? ev.side : "--";
                    const reason = typeof ev.reason === "string" ? ev.reason : "--";
                    return (
                      <tr key={`event-${idx}-${tsMs ?? "na"}`}>
                        <td>{formatTime(tsMs, timeMode)}</td>
                        <td>{action}</td>
                        <td>{side}</td>
                        <td>{reason}</td>
                        <td className={accepted === true ? "up" : accepted === false ? "down" : ""}>
                          {accepted === true ? "accepted" : accepted === false ? "rejected" : "--"}
                        </td>
                      </tr>
                    );
                  })}
                  {liveEvents.length === 0 ? (
                    <tr>
                      <td colSpan={5}>暂无状态机事件。</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ) : null}
      <div className="table-wrap">
        <h3 className="table-title">{source === "live" ? "Paper对照交易（同参数回放）" : "交易记录"}</h3>
        <table className="history-table">
          <thead>
            <tr>
              <th>方向</th>
              <th>轮次</th>
              <th>入场</th>
              <th>出场</th>
              <th>价格(¢)</th>
              <th>净盈亏</th>
              <th>成本</th>
              <th>时长</th>
              <th>原因</th>
            </tr>
          </thead>
          <tbody>
            {(data?.trades ?? []).slice(-12).reverse().map((t) => (
              <tr key={t.id}>
                <td><span className={`chip ${t.side === "UP" ? "up" : "down"}`}>{t.side}</span></td>
                <td>{t.entry_round_id.length > 20 ? `${t.entry_round_id.slice(0, 20)}…` : t.entry_round_id}</td>
                <td>{formatTime(t.entry_ts_ms, timeMode)}</td>
                <td>{formatTime(t.exit_ts_ms, timeMode)}</td>
                <td>
                  <div>{t.entry_price_raw_cents.toFixed(2)} → {t.exit_price_raw_cents.toFixed(2)}</div>
                  <small className="muted">exec {t.entry_price_cents.toFixed(2)} → {t.exit_price_cents.toFixed(2)}</small>
                </td>
                <td className={(t.pnl_net_cents ?? t.pnl_cents) >= 0 ? "up" : "down"}>
                  {(t.pnl_net_cents ?? t.pnl_cents).toFixed(2)}¢
                </td>
                <td>
                  <div>{(t.total_cost_cents ?? 0).toFixed(2)}¢</div>
                  <small className="muted">
                    fee {(t.entry_fee_cents + t.exit_fee_cents).toFixed(2)} · slip {(t.entry_slippage_cents + t.exit_slippage_cents).toFixed(2)}
                  </small>
                </td>
                <td>{t.duration_s.toFixed(1)}s</td>
                <td>{t.entry_reason} / {t.exit_reason}</td>
              </tr>
            ))}
            {(data?.trades?.length ?? 0) === 0 ? (
              <tr>
                <td colSpan={9}>暂无交易样本，等待策略信号触发。</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  );
});

export default function App() {
  const [wsStatus, setWsStatus] = useState<"connecting" | "open" | "closed" | "error">("connecting");
  const [live, setLive] = useState<Record<MarketType, LiveSnapshot | null>>({
    "5m": null,
    "15m": null
  });
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [collectorStatus, setCollectorStatus] = useState<CollectorStatusResponse | null>(null);
  const [chartWindow, setChartWindow] = useState<Record<MarketType, WindowType>>({
    "5m": "30m",
    "15m": "30m"
  });
  const [charts, setCharts] = useState<Record<MarketType, ChartResponse | null>>({
    "5m": null,
    "15m": null
  });
  const [chartLoading, setChartLoading] = useState<Record<MarketType, boolean>>({
    "5m": false,
    "15m": false
  });
  const [roundHistoryTab, setRoundHistoryTab] = useState<MarketType>("5m");
  const [roundHistory, setRoundHistory] = useState<Record<MarketType, RoundsResponse | null>>({
    "5m": null,
    "15m": null
  });
  const [explorerTab, setExplorerTab] = useState<MarketType>("5m");
  const [availableRounds, setAvailableRounds] = useState<Record<MarketType, AvailableRoundsResponse | null>>({
    "5m": null,
    "15m": null
  });
  const [selectedDate, setSelectedDate] = useState<string>("");
  const [selectedRoundId, setSelectedRoundId] = useState<string>("");
  const [roundChart, setRoundChart] = useState<RoundChartResponse | null>(null);
  const [heatmapTab, setHeatmapTab] = useState<MarketType>("5m");
  const [heatmap, setHeatmap] = useState<HeatmapResponse | null>(null);
  const [accuracyTab, setAccuracyTab] = useState<MarketType>("5m");
  const [accuracy, setAccuracy] = useState<AccuracySeriesResponse | null>(null);
  const [strategyPaper, setStrategyPaper] = useState<StrategyPaperResponse | null>(null);
  const [strategyLoading, setStrategyLoading] = useState<boolean>(false);
  const [strategyMarketType, setStrategyMarketType] = useState<MarketType>("5m");
  const [strategySource, setStrategySource] = useState<StrategyPaperSource>("replay");
  const [errorText, setErrorText] = useState<string>("");
  const [timeMode, setTimeMode] = useState<TimeMode>("local");
  const lastLiveTsRef = useRef<Record<MarketType, number>>({ "5m": 0, "15m": 0 });
  const liveUiCommitRef = useRef<Record<MarketType, { uiTs: number; roundId: string; remSec: number }>>({
    "5m": { uiTs: 0, roundId: "", remSec: -1 },
    "15m": { uiTs: 0, roundId: "", remSec: -1 }
  });
  const chartWindowRef = useRef(chartWindow);
  const wsStatusRef = useRef(wsStatus);
  const lastWsTickMsRef = useRef<number>(0);
  const pushLivePointRef = useRef<(marketType: MarketType, livePoint: LiveSnapshot | null) => void>(() => {});
  const chartReqSeqRef = useRef<Record<MarketType, number>>({ "5m": 0, "15m": 0 });
  const chartInFlightRef = useRef<Record<MarketType, boolean>>({ "5m": false, "15m": false });
  const strategyInFlightRef = useRef<boolean>(false);
  const stableLiveRef = useRef<Record<MarketType, LiveSnapshot | null>>({ "5m": null, "15m": null });
  const boundaryRefreshRef = useRef<Record<MarketType, number>>({ "5m": 0, "15m": 0 });
  const accuracyTabRef = useRef<MarketType>("5m");

  useEffect(() => {
    chartWindowRef.current = chartWindow;
  }, [chartWindow]);

  useEffect(() => {
    wsStatusRef.current = wsStatus;
  }, [wsStatus]);

  useEffect(() => {
    accuracyTabRef.current = accuracyTab;
  }, [accuracyTab]);

  const loadChartFor = useCallback(
    async (marketType: MarketType, windowType: WindowType) => {
      chartInFlightRef.current[marketType] = true;
      const reqId = (chartReqSeqRef.current[marketType] ?? 0) + 1;
      chartReqSeqRef.current[marketType] = reqId;
      setChartLoading((prev) => ({ ...prev, [marketType]: true }));
      try {
        const data = await getChart(marketType, windowType);
        if (chartReqSeqRef.current[marketType] !== reqId) {
          return;
        }
        setCharts((prev) => ({
          ...prev,
          [marketType]: trimChartToWindow(data, windowType, Date.now())
        }));
        setErrorText("");
      } catch (err) {
        if (chartReqSeqRef.current[marketType] !== reqId) {
          return;
        }
        setErrorText(err instanceof Error ? err.message : String(err));
      } finally {
        chartInFlightRef.current[marketType] = false;
        if (chartReqSeqRef.current[marketType] === reqId) {
          setChartLoading((prev) => ({ ...prev, [marketType]: false }));
        }
      }
    },
    []
  );

  pushLivePointRef.current = (marketType: MarketType, livePoint: LiveSnapshot | null) => {
    if (!livePoint || !livePoint.timestamp_ms) {
      stableLiveRef.current[marketType] = null;
      liveUiCommitRef.current[marketType] = {
        uiTs: performance.now(),
        roundId: "",
        remSec: -1
      };
      setLive((prev) => ({ ...prev, [marketType]: null }));
      return;
    }
    if (livePoint.timestamp_ms <= lastLiveTsRef.current[marketType]) {
      return;
    }
    lastLiveTsRef.current[marketType] = livePoint.timestamp_ms;

    const remSec = Math.floor(Math.max(0, livePoint.time_remaining_s ?? 0));
    const prev = liveUiCommitRef.current[marketType];
    const now = performance.now();
    const shouldCommit =
      livePoint.round_id !== prev.roundId ||
      remSec !== prev.remSec ||
      now - prev.uiTs >= LIVE_UI_MIN_INTERVAL_MS;
    if (!shouldCommit) {
      return;
    }
    liveUiCommitRef.current[marketType] = {
      uiTs: now,
      roundId: livePoint.round_id,
      remSec
    };

    const prevStable = stableLiveRef.current[marketType];
    const stablePoint = normalizeLiveSnapshot(
      prevStable?.round_id === livePoint.round_id ? prevStable : null,
      livePoint
    );
    stableLiveRef.current[marketType] = stablePoint;

    setLive((prev) => ({ ...prev, [marketType]: stablePoint }));
    setCharts((prev) => {
      const current = prev[marketType];
      if (!current) {
        return prev;
      }
      const lastTs = current.points[current.points.length - 1]?.timestamp_ms ?? 0;
      if (stablePoint.timestamp_ms <= lastTs) {
        return prev;
      }

      const nextPoint: ChartPoint = {
        timestamp_ms: stablePoint.timestamp_ms ?? 0,
        delta_pct: stablePoint.delta_pct_smooth ?? stablePoint.delta_pct,
        delta_pct_smooth: stablePoint.delta_pct_smooth ?? stablePoint.delta_pct ?? null,
        mid_yes:
          stablePoint.mid_yes_smooth ??
          stablePoint.mid_yes ??
          midpointProb(stablePoint.best_bid_up, stablePoint.best_ask_up),
        mid_yes_smooth: stablePoint.mid_yes_smooth ?? stablePoint.mid_yes ?? null,
        mid_no:
          stablePoint.mid_no_smooth ??
          stablePoint.mid_no ??
          midpointProb(stablePoint.best_bid_down, stablePoint.best_ask_down),
        mid_no_smooth: stablePoint.mid_no_smooth ?? stablePoint.mid_no ?? null,
        best_bid_up: stablePoint.best_bid_up,
        best_ask_up: stablePoint.best_ask_up,
        best_bid_down: stablePoint.best_bid_down,
        best_ask_down: stablePoint.best_ask_down,
        round_id: stablePoint.round_id,
        time_remaining_s: stablePoint.time_remaining_s,
        btc_price: stablePoint.btc_price,
        target_price: stablePoint.target_price
      };
      const nextPoints = trimPointsToWindow(
        [...current.points, nextPoint],
        chartWindowRef.current[marketType],
        Date.now()
      );
      return {
        ...prev,
        [marketType]: {
          ...current,
          points: nextPoints,
          total_samples: Math.max(current.total_samples, nextPoints.length)
        }
      };
    });

    if (prevStable?.round_id && prevStable.round_id !== stablePoint.round_id) {
      void loadChartFor(marketType, chartWindowRef.current[marketType]);
      if (accuracyTabRef.current === marketType) {
        void getAccuracySeries(marketType, 24)
          .then((v) => {
            setAccuracy(v);
          })
          .catch(() => {
            // ignore boundary refresh errors
          });
      }
    }
    if ((stablePoint.time_remaining_s ?? 999) <= 0.2) {
      const now = Date.now();
      if (now - (boundaryRefreshRef.current[marketType] ?? 0) >= 1000) {
        boundaryRefreshRef.current[marketType] = now;
        void loadChartFor(marketType, chartWindowRef.current[marketType]);
      }
    }
  };

  useEffect(() => {
    let alive = true;
    let inFlight = false;
    const poll = async () => {
      if (!alive || inFlight) {
        return;
      }
      if (
        wsStatusRef.current === "open" &&
        Date.now() - (lastWsTickMsRef.current || 0) < WS_STALE_FALLBACK_MS &&
        (stableLiveRef.current["5m"]?.time_remaining_s ?? 1) > 0 &&
        (stableLiveRef.current["15m"]?.time_remaining_s ?? 1) > 0
      ) {
        return;
      }
      inFlight = true;
      try {
        const rows = await getLatestAllRaw();
        if (!alive) {
          inFlight = false;
          return;
        }
        if (rows.length === 0) {
          pushLivePointRef.current("5m", null);
          pushLivePointRef.current("15m", null);
          inFlight = false;
          return;
        }

        let latest5: (typeof rows)[number] | undefined;
        let latest15: (typeof rows)[number] | undefined;
        for (const row of rows) {
          if (row.symbol !== "BTCUSDT") {
            continue;
          }
          if (row.timeframe === "5m") {
            if (!latest5 || row.ts_ireland_sample_ms > latest5.ts_ireland_sample_ms) {
              latest5 = row;
            }
          } else if (row.timeframe === "15m") {
            if (!latest15 || row.ts_ireland_sample_ms > latest15.ts_ireland_sample_ms) {
              latest15 = row;
            }
          }
        }

        const mapLive = (r: (typeof rows)[number] | undefined, marketType: MarketType): LiveSnapshot | null => {
          if (!r) {
            return null;
          }
          const startMs = Number(r.round_id.split("_").pop() ?? 0) || 0;
          const durMs = marketType === "5m" ? 5 * 60 * 1000 : 15 * 60 * 1000;
          return {
            timestamp_ms: r.ts_ireland_sample_ms,
            round_id: r.round_id,
            market_type: marketType,
            btc_price: r.binance_price,
            target_price: r.target_price,
            delta_pct: r.delta_pct_smooth ?? r.delta_pct,
            delta_pct_smooth: r.delta_pct_smooth ?? null,
            mid_yes: r.mid_yes ?? null,
            mid_no: r.mid_no ?? null,
            mid_yes_smooth: r.mid_yes_smooth ?? null,
            mid_no_smooth: r.mid_no_smooth ?? null,
            best_bid_up: r.bid_yes ?? r.mid_yes_smooth ?? r.mid_yes,
            best_ask_up: r.ask_yes ?? r.mid_yes_smooth ?? r.mid_yes,
            best_bid_down: r.bid_no ?? r.mid_no_smooth ?? r.mid_no,
            best_ask_down: r.ask_no ?? r.mid_no_smooth ?? r.mid_no,
            time_remaining_s: r.remaining_ms / 1000,
            velocity_bps_per_sec: r.velocity_bps_per_sec ?? null,
            acceleration: r.acceleration ?? null,
            slug: `${marketType}-${Math.floor(startMs / 1000)}`,
            start_time_ms: startMs,
            end_time_ms: startMs > 0 ? startMs + durMs : 0
          };
        };

        const l5 = mapLive(latest5, "5m");
        const l15 = mapLive(latest15, "15m");
        pushLivePointRef.current("5m", l5);
        pushLivePointRef.current("15m", l15);
      } catch {
        // ignore high-frequency poll errors
      } finally {
        inFlight = false;
      }
    };

    void poll();
    const id = window.setInterval(poll, LIVE_POLL_MS);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    const disconnect = connectLiveWs(
      (payload) => {
        lastWsTickMsRef.current = Date.now();
        pushLivePointRef.current("5m", payload["5m"]);
        pushLivePointRef.current("15m", payload["15m"]);
      },
      setWsStatus
    );
    return disconnect;
  }, []);

  useEffect(() => {
    const onVisible = () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      void loadChartFor("5m", chartWindowRef.current["5m"]);
      void loadChartFor("15m", chartWindowRef.current["15m"]);
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loadChartFor]);

  useEffect(() => {
    let alive = true;
    const run = async () => {
      try {
        const value = await getStats();
        if (alive) {
          setStats(value);
          setErrorText("");
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    void run();
    const id = window.setInterval(run, 10_000);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    let alive = true;
    let timer: number | null = null;
    const loop = async () => {
      if (!alive) {
        return;
      }
      if (document.visibilityState === "visible") {
        await loadChartFor("5m", chartWindowRef.current["5m"]);
      }
      const currentWindow = chartWindowRef.current["5m"];
      const rem = stableLiveRef.current["5m"]?.time_remaining_s ?? 999;
      const wsFresh =
        wsStatusRef.current === "open" &&
        Date.now() - (lastWsTickMsRef.current || 0) < WS_STALE_FALLBACK_MS;
      const delay =
        rem <= 1
          ? 900
          : rem <= 8
            ? 2_000
            : wsFresh
              ? currentWindow === "all"
                ? 18_000
                : 12_000
              : currentWindow === "all"
                ? 12_000
                : 7_500;
      timer = window.setTimeout(loop, delay);
    };
    void loop();
    return () => {
      alive = false;
      if (timer != null) {
        window.clearTimeout(timer);
      }
    };
  }, [loadChartFor]);

  useEffect(() => {
    let alive = true;
    let timer: number | null = null;
    const loop = async () => {
      if (!alive) {
        return;
      }
      if (document.visibilityState === "visible") {
        await loadChartFor("15m", chartWindowRef.current["15m"]);
      }
      const currentWindow = chartWindowRef.current["15m"];
      const rem = stableLiveRef.current["15m"]?.time_remaining_s ?? 999;
      const wsFresh =
        wsStatusRef.current === "open" &&
        Date.now() - (lastWsTickMsRef.current || 0) < WS_STALE_FALLBACK_MS;
      const delay =
        rem <= 1
          ? 900
          : rem <= 10
            ? 2_200
            : wsFresh
              ? currentWindow === "all"
                ? 20_000
                : 14_000
              : currentWindow === "all"
                ? 16_000
                : 9_500;
      timer = window.setTimeout(loop, delay);
    };
    void loop();
    return () => {
      alive = false;
      if (timer != null) {
        window.clearTimeout(timer);
      }
    };
  }, [loadChartFor]);

  const handleChartWindowChange = (marketType: MarketType, view: WindowType) => {
    if (chartWindowRef.current[marketType] === view) {
      return;
    }
    setChartWindow((prev) => ({ ...prev, [marketType]: view }));
    void loadChartFor(marketType, view);
  };

  useEffect(() => {
    let alive = true;
    const load = async () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      try {
        const [r5, r15] = await Promise.all([getRoundHistory("5m", 250), getRoundHistory("15m", 250)]);
        if (alive) {
          setRoundHistory({ "5m": r5, "15m": r15 });
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    const first = window.setTimeout(load, 1_200);
    const id = window.setInterval(load, 20_000);
    return () => {
      alive = false;
      window.clearTimeout(first);
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      try {
        const data = await getAvailableRounds(explorerTab);
        if (!alive) {
          return;
        }
        setAvailableRounds((prev) => ({ ...prev, [explorerTab]: data }));
        if (data.days.length > 0) {
          const fallbackDate = data.days[0]?.date ?? "";
          setSelectedDate((prev) => (prev ? prev : fallbackDate));
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    const first = window.setTimeout(load, 1_600);
    const id = window.setInterval(load, 60000);
    return () => {
      alive = false;
      window.clearTimeout(first);
      window.clearInterval(id);
    };
  }, [explorerTab]);

  const explorerDateRounds = useMemo(() => {
    const source = availableRounds[explorerTab];
    if (!source || !selectedDate) {
      return [];
    }
    return source.rounds.filter((r) => r.date === selectedDate);
  }, [availableRounds, explorerTab, selectedDate]);

  useEffect(() => {
    if (explorerDateRounds.length === 0) {
      setSelectedRoundId("");
      return;
    }
    if (!explorerDateRounds.some((r) => r.round_id === selectedRoundId)) {
      setSelectedRoundId(explorerDateRounds[0]?.round_id ?? "");
    }
  }, [explorerDateRounds, selectedRoundId]);

  useEffect(() => {
    let alive = true;
    if (!selectedRoundId) {
      setRoundChart(null);
      return () => {
        alive = false;
      };
    }
    const load = async () => {
      try {
        const row = await getRoundChart(selectedRoundId, explorerTab);
        if (alive) {
          setRoundChart(row);
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    void load();
    return () => {
      alive = false;
    };
  }, [selectedRoundId, explorerTab]);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      try {
        const v = await getHeatmap(heatmapTab, 72);
        if (alive) {
          setHeatmap(v);
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    const first = window.setTimeout(load, 2_200);
    const id = window.setInterval(load, 60_000);
    return () => {
      alive = false;
      window.clearTimeout(first);
      window.clearInterval(id);
    };
  }, [heatmapTab]);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      try {
        const v = await getAccuracySeries(accuracyTab, 24);
        if (alive) {
          setAccuracy(v);
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    const first = window.setTimeout(load, 2_600);
    const id = window.setInterval(load, 60_000);
    return () => {
      alive = false;
      window.clearTimeout(first);
      window.clearInterval(id);
    };
  }, [accuracyTab]);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      if (strategyInFlightRef.current) {
        return;
      }
      strategyInFlightRef.current = true;
      setStrategyLoading(true);
      try {
        const data = await getStrategyPaper(strategyMarketType, {
          ...STRATEGY_PAPER_PROFILE,
          source: strategySource,
          ...(strategySource === "live" ? STRATEGY_LIVE_PROFILE : {}),
        });
        if (alive) {
          setStrategyPaper(data);
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      } finally {
        strategyInFlightRef.current = false;
        if (alive) {
          setStrategyLoading(false);
        }
      }
    };
    void load();
    const id = window.setInterval(() => {
      void load();
    }, STRATEGY_POLL_MS);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, [strategyMarketType, strategySource]);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      if (document.visibilityState !== "visible") {
        return;
      }
      try {
        const value = await getCollectorStatus();
        if (alive) {
          setCollectorStatus(value);
        }
      } catch {
        // keep silent; collector status is advisory.
      }
    };
    const first = window.setTimeout(load, 800);
    const id = window.setInterval(load, COLLECTOR_POLL_MS);
    return () => {
      alive = false;
      window.clearTimeout(first);
      window.clearInterval(id);
    };
  }, []);

  const currentHistory = roundHistory[roundHistoryTab];
  const historyRows = useMemo(() => currentHistory?.rounds ?? [], [currentHistory]);
  const heatmapCells = useMemo(() => heatmap?.cells ?? [], [heatmap]);
  const accuracyPoints = useMemo(() => accuracy?.series ?? [], [accuracy]);
  const explorerDays = useMemo(() => availableRounds[explorerTab]?.days ?? [], [availableRounds, explorerTab]);
  const collector5m = collectorStatus?.timeframes["5m"] ?? null;
  const collector15m = collectorStatus?.timeframes["15m"] ?? null;
  const collectorOverallText = collectorStatus
    ? collectorStatus.ok
      ? "采集正常"
      : "采集中断"
    : "采集未知";
  const selectedRoundMeta = useMemo(() => {
    const round = roundChart?.round;
    if (!round) {
      return null;
    }
    const durationMs = Math.max(0, round.end_time_ms - round.start_time_ms);
    return {
      roundId: round.round_id,
      start: round.start_time_ms,
      end: round.end_time_ms,
      durationMin: durationMs / 60_000,
      target: round.target_price,
      outcome: round.outcome
    };
  }, [roundChart]);

  return (
    <main className="page">
      <header className="hero panel">
        <div className="hero-left">
          <p className="hero-kicker">POLYEDGE PREDATOR</p>
          <h1>
            实时研究仪表盘 <span>Polymarket BTC 短周期</span>
          </h1>
          <p className="hero-sub">
            统一展示 5m / 15m 市场的概率、价差、轮次与准确率，用于策略研究与训练数据校验。
          </p>
        </div>
        <div className="hero-right">
          <div className="live-indicator">
            <span className={`dot ${wsStatus === "open" ? "ok" : ""}`} />
            {wsStatus === "open" ? "实时连接" : "重连中"}
          </div>
          <div className="btn-group">
            <button className={timeMode === "local" ? "active" : ""} onClick={() => setTimeMode("local")}>
              本地时间
            </button>
            <button className={timeMode === "et" ? "active" : ""} onClick={() => setTimeMode("et")}>
              PolyEdge ET
            </button>
          </div>
          <div className="hero-meta">
            <span>时区: {formatTimeZoneLabel(timeMode)}</span>
            <span>5m: {formatTime(live["5m"]?.timestamp_ms, timeMode)}</span>
            <span>15m: {formatTime(live["15m"]?.timestamp_ms, timeMode)}</span>
            <span className={collectorStatus ? (collectorStatus.ok ? "up" : "down") : ""}>
              {collectorOverallText}
            </span>
            <span>
              5m采集: {collector5m?.status ?? "--"} · {formatAgeMs(collector5m?.age_ms ?? null)}
            </span>
            <span>
              15m采集: {collector15m?.status ?? "--"} · {formatAgeMs(collector15m?.age_ms ?? null)}
            </span>
          </div>
        </div>
      </header>

      <section className="round-grid">
        {roundCard("5分钟轮次", live["5m"])}
        {roundCard("15分钟轮次", live["15m"])}
      </section>

      <MemoMarketSection
        title="5分钟市场"
        marketType="5m"
        live={live["5m"]}
        timeMode={timeMode}
        windowType={chartWindow["5m"]}
        onWindowChange={(v) => handleChartWindowChange("5m", v)}
        chart={charts["5m"]}
        loading={chartLoading["5m"]}
      />

      <MemoMarketSection
        title="15分钟市场"
        marketType="15m"
        live={live["15m"]}
        timeMode={timeMode}
        windowType={chartWindow["15m"]}
        onWindowChange={(v) => handleChartWindowChange("15m", v)}
        chart={charts["15m"]}
        loading={chartLoading["15m"]}
      />

      <StrategyPanel
        data={strategyPaper}
        loading={strategyLoading}
        timeMode={timeMode}
        marketType={strategyMarketType}
        source={strategySource}
        onMarketTypeChange={setStrategyMarketType}
        onSourceChange={setStrategySource}
      />

      <section className="panel">
        <header className="panel-head">
          <div>
            <h2>轮次浏览器</h2>
            <p className="muted">选择日期和轮次后，会显示该轮完整时间线和关键参数。</p>
          </div>
          <div className="btn-group">
            <button className={explorerTab === "5m" ? "active" : ""} onClick={() => setExplorerTab("5m")}>
              5m
            </button>
            <button className={explorerTab === "15m" ? "active" : ""} onClick={() => setExplorerTab("15m")}>
              15m
            </button>
          </div>
        </header>
        <div className="explorer-day-chips">
          {explorerDays.slice(0, 8).map((d) => (
            <button
              key={d.date}
              className={selectedDate === d.date ? "active" : ""}
              onClick={() => setSelectedDate(d.date)}
            >
              {d.date}
              <span>{d.round_count}轮</span>
            </button>
          ))}
        </div>
        <div className="explorer-controls">
          <label>
            <span>日期</span>
            <select value={selectedDate} onChange={(e) => setSelectedDate(e.target.value)}>
              <option value="">选择日期</option>
              {explorerDays.map((d) => (
                <option key={d.date} value={d.date}>
                  {d.date}（{d.round_count}轮）
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>轮次</span>
            <select value={selectedRoundId} onChange={(e) => setSelectedRoundId(e.target.value)}>
              <option value="">选择轮次</option>
              {explorerDateRounds.map((r) => (
                <option key={r.round_id} value={r.round_id}>
                  {formatTime(r.start_time_ms, timeMode)} · {r.round_id}
                </option>
              ))}
            </select>
          </label>
        </div>
        {selectedRoundMeta ? (
          <div className="info-cards">
            <article className="info-card">
              <span>选中轮次</span>
              <strong>{selectedRoundMeta.roundId}</strong>
              <small>
                {formatTime(selectedRoundMeta.start, timeMode)} - {formatTime(selectedRoundMeta.end, timeMode)}
              </small>
            </article>
            <article className="info-card">
              <span>时长</span>
              <strong>{selectedRoundMeta.durationMin.toFixed(1)} min</strong>
              <small>{explorerTab.toUpperCase()} 周期</small>
            </article>
            <article className="info-card">
              <span>目标价 / 结果</span>
              <strong>
                {formatUsd(selectedRoundMeta.target)} ·{" "}
                <span className={selectedRoundMeta.outcome === 1 ? "up" : "down"}>
                  {selectedRoundMeta.outcome === 1 ? "UP" : selectedRoundMeta.outcome === 0 ? "DOWN" : "--"}
                </span>
              </strong>
              <small>便于快速审查该轮时间线</small>
            </article>
          </div>
        ) : null}
        {roundChart && roundChart.points.length > 0 ? (
          <MarketChart
            points={roundChart.points}
            rounds={roundChart.round ? [roundChart.round] : []}
            windowType="all"
            timeMode={timeMode}
            height={320}
          />
        ) : (
          <div className="empty-panel">请选择日期与轮次查看图表</div>
        )}
      </section>

      <HeatmapPanel heatmapTab={heatmapTab} onTabChange={setHeatmapTab} cells={heatmapCells} />

      <section className="stats-grid">
        <article className="panel stat-card">
          <h4>总样本数</h4>
          <strong>{stats?.total_samples.toLocaleString() ?? "0"}</strong>
        </article>
        <article className="panel stat-card">
          <h4>总轮次数</h4>
          <strong>{stats?.total_rounds.toLocaleString() ?? "0"}</strong>
        </article>
        <article className="panel stat-card">
          <h4>看涨 / 看跌</h4>
          <strong>
            {stats?.up_count.toLocaleString() ?? "0"} / {stats?.down_count.toLocaleString() ?? "0"}
          </strong>
        </article>
        <article className="panel stat-card">
          <h4>运行时长</h4>
          <strong>{stats ? `${stats.uptime_hours.toFixed(1)}h` : "0.0h"}</strong>
        </article>
        <article className="panel stat-card">
          <h4>市场准确率</h4>
          <strong>{stats?.market_accuracy != null ? `${(stats.market_accuracy * 100).toFixed(1)}%` : "--"}</strong>
        </article>
      </section>

      <RoundHistoryPanel
        roundHistoryTab={roundHistoryTab}
        onTabChange={setRoundHistoryTab}
        rows={historyRows}
        timeMode={timeMode}
      />

      <AccuracyPanel
        accuracyTab={accuracyTab}
        onTabChange={setAccuracyTab}
        points={accuracyPoints}
        timeMode={timeMode}
      />

      <footer className="status-row">
        <span>WS状态: {wsStatus}</span>
        <span>时区: {formatTimeZoneLabel(timeMode)}</span>
        <span>5m最新Tick: {formatTime(live["5m"]?.timestamp_ms, timeMode)}</span>
        <span>15m最新Tick: {formatTime(live["15m"]?.timestamp_ms, timeMode)}</span>
        <span className={collectorStatus ? (collectorStatus.ok ? "up" : "down") : ""}>
          {collectorOverallText}
        </span>
        <span>
          5m采集延迟: {formatAgeMs(collector5m?.age_ms ?? null)} · 轮次{" "}
          {collector5m?.round_id || "--"}
        </span>
        <span>
          15m采集延迟: {formatAgeMs(collector15m?.age_ms ?? null)} · 轮次{" "}
          {collector15m?.round_id || "--"}
        </span>
        {errorText ? <span className="down">{errorText}</span> : null}
      </footer>
    </main>
  );
}
