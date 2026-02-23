import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  connectLiveWs,
  getAccuracySeries,
  getAvailableRounds,
  getChart,
  getHeatmap,
  getLatestAllRaw,
  getRoundChart,
  getRoundHistory,
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
  HeatmapCell,
  HeatmapResponse,
  LiveSnapshot,
  MarketType,
  RoundHistoryRow,
  RoundChartResponse,
  RoundsResponse,
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

const LIVE_POLL_MS = 1500;
const WS_STALE_FALLBACK_MS = 3000;
const LIVE_UI_MIN_INTERVAL_MS = 300;

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

function trimPointsToWindow(points: ChartPoint[], view: WindowType, nowMs: number): ChartPoint[] {
  const minutes = windowToMinutes(view);
  let next = points;
  if (minutes > 0) {
    const cutoffMs = nowMs - minutes * 60_000;
    next = next.filter((p) => p.timestamp_ms >= cutoffMs);
  }
  const cap = view === "all" ? 12_000 : Math.min(12_000, Math.max(3_500, minutes * 700));
  if (next.length > cap) {
    next = next.slice(next.length - cap);
  }
  return next;
}

function trimChartToWindow(chart: ChartResponse, view: WindowType, nowMs: number): ChartResponse {
  const minutes = windowToMinutes(view);
  const points = trimPointsToWindow(chart.points, view, nowMs);
  const rounds =
    minutes <= 0
      ? chart.rounds
      : chart.rounds.filter((r) => {
          const endTs = r.end_ts_ms ?? r.end_time_ms ?? 0;
          return endTs >= nowMs - minutes * 60_000;
        });
  return {
    ...chart,
    points,
    rounds
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

function formatTime(ts: number | null | undefined): string {
  if (!ts) {
    return "--";
  }
  return new Date(ts).toLocaleTimeString("zh-CN", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
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
  return (
    <article className="round-card">
      <div className="card-head">
        <h3>{title}</h3>
        <span className="countdown">⏱ {formatCountdown(snapshot?.time_remaining_s)}</span>
      </div>
      <div className="round-price">{formatUsd(snapshot?.btc_price)}</div>
      <div className="round-sub">
        <span>目标价 {formatUsd(snapshot?.target_price)}</span>
        <span className={(snapshot?.delta_pct ?? 0) >= 0 ? "up" : "down"}>
          ▲ {formatPct(snapshot?.delta_pct)}
        </span>
      </div>
      <div className="round-odds">
        <div>
          <label>看涨</label>
          <strong className="up">{formatCent(snapshot?.best_bid_up)}</strong>
          <small>
            {formatCent(snapshot?.best_bid_up)} / {formatCent(snapshot?.best_ask_up)}
          </small>
        </div>
        <div>
          <label>看跌</label>
          <strong className="down">{formatCent(snapshot?.best_bid_down)}</strong>
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
  windowType: WindowType;
  onWindowChange: (w: WindowType) => void;
  chart: ChartResponse | null;
  loading: boolean;
}

function MarketSection({
  title,
  marketType,
  live,
  windowType,
  onWindowChange,
  chart,
  loading
}: MarketSectionProps) {
  const gapInfo = useMemo(() => summarizeGaps(chart?.points ?? []), [chart?.points]);

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
          {loading ? <span className="loading-chip">Loading...</span> : null}
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
          看涨 <b className="up">{formatCent(live?.best_bid_up)}</b>
        </span>
        <span>
          看跌 <b className="down">{formatCent(live?.best_bid_down)}</b>
        </span>
        <span>⏱ {formatCountdown(live?.time_remaining_s)}</span>
      </div>
      <MarketChart points={chart?.points ?? []} rounds={chart?.rounds ?? []} height={360} />
      <div className="panel-foot">
        <span>
          {chart?.total_samples.toLocaleString() ?? "0"} samples
          {chart?.downsampled ? ` (downsample x${chart.step})` : ""}
          {gapInfo.count > 0 ? ` | gaps ${gapInfo.count} (max ${(gapInfo.maxGapMs / 1000).toFixed(1)}s)` : ""}
        </span>
        <span>{chart?.rounds.length.toLocaleString() ?? "0"} rounds</span>
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
          <h2>MARKET PRICING HEATMAP</h2>
          <p className="muted">
            Average UP price by delta % and time remaining — How does the market price UP/DOWN given
            the current gap?
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
      <HeatmapGrid cells={cells} />
    </section>
  );
});

interface AccuracyPanelProps {
  accuracyTab: MarketType;
  onTabChange: (v: MarketType) => void;
  points: AccuracyPoint[];
}

const AccuracyPanel = memo(function AccuracyPanel({ accuracyTab, onTabChange, points }: AccuracyPanelProps) {
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>MARKET ACCURACY (Rolling 20 Rounds)</h2>
          <p className="muted">这是最近20个轮次的滚动正确率：市场最终定价方向与实际结果一致的比例。</p>
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
      <AccuracyChart points={points} />
    </section>
  );
});

interface RoundHistoryPanelProps {
  roundHistoryTab: MarketType;
  onTabChange: (v: MarketType) => void;
  rows: RoundHistoryRow[];
}

const RoundHistoryPanel = memo(function RoundHistoryPanel({
  roundHistoryTab,
  onTabChange,
  rows
}: RoundHistoryPanelProps) {
  return (
    <section className="panel">
      <header className="panel-head">
        <h2>轮次历史</h2>
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
                <td>{formatTime(r.end_time_ms)}</td>
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

export default function App() {
  const [wsStatus, setWsStatus] = useState<"connecting" | "open" | "closed" | "error">("connecting");
  const [live, setLive] = useState<Record<MarketType, LiveSnapshot | null>>({
    "5m": null,
    "15m": null
  });
  const [stats, setStats] = useState<StatsResponse | null>(null);
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
  const [errorText, setErrorText] = useState<string>("");
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

  useEffect(() => {
    chartWindowRef.current = chartWindow;
  }, [chartWindow]);

  useEffect(() => {
    wsStatusRef.current = wsStatus;
  }, [wsStatus]);

  const loadChartFor = useCallback(
    async (marketType: MarketType, windowType: WindowType) => {
      if (chartInFlightRef.current[marketType]) {
        return;
      }
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

    setLive((prev) => ({ ...prev, [marketType]: livePoint }));
    setCharts((prev) => {
      const current = prev[marketType];
      if (!current) {
        return prev;
      }
      const lastTs = current.points[current.points.length - 1]?.timestamp_ms ?? 0;
      if (livePoint.timestamp_ms <= lastTs) {
        return prev;
      }

      const nextPoint: ChartPoint = {
        timestamp_ms: livePoint.timestamp_ms ?? 0,
        delta_pct: livePoint.delta_pct,
        mid_yes: midpointProb(livePoint.best_bid_up, livePoint.best_ask_up),
        mid_no: midpointProb(livePoint.best_bid_down, livePoint.best_ask_down),
        best_bid_up: livePoint.best_bid_up,
        best_ask_up: livePoint.best_ask_up,
        best_bid_down: livePoint.best_bid_down,
        best_ask_down: livePoint.best_ask_down,
        round_id: livePoint.round_id,
        time_remaining_s: livePoint.time_remaining_s,
        btc_price: livePoint.btc_price,
        target_price: livePoint.target_price
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
        Date.now() - (lastWsTickMsRef.current || 0) < WS_STALE_FALLBACK_MS
      ) {
        return;
      }
      inFlight = true;
      try {
        const rows = await getLatestAllRaw();
        if (!alive || rows.length === 0) {
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
            delta_pct: r.delta_pct,
            best_bid_up: r.bid_yes ?? r.mid_yes,
            best_ask_up: r.ask_yes ?? r.mid_yes,
            best_bid_down: r.bid_no ?? r.mid_no,
            best_ask_down: r.ask_no ?? r.mid_no,
            time_remaining_s: r.remaining_ms / 1000,
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
      const delay = currentWindow === "all" ? 12_000 : 7_500;
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
      const delay = currentWindow === "all" ? 16_000 : 9_500;
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
    void load();
    const id = window.setInterval(load, 45_000);
    return () => {
      alive = false;
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
    void load();
    const id = window.setInterval(load, 60000);
    return () => {
      alive = false;
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
    void load();
    const id = window.setInterval(load, 45_000);
    return () => {
      alive = false;
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
        const v = await getAccuracySeries(accuracyTab, 20, 600);
        if (alive) {
          setAccuracy(v);
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      }
    };
    void load();
    const id = window.setInterval(load, 45_000);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, [accuracyTab]);

  const currentHistory = roundHistory[roundHistoryTab];
  const historyRows = useMemo(() => currentHistory?.rounds ?? [], [currentHistory]);
  const heatmapCells = useMemo(() => heatmap?.cells ?? [], [heatmap]);
  const accuracyPoints = useMemo(() => accuracy?.series ?? [], [accuracy]);
  const explorerDays = useMemo(() => availableRounds[explorerTab]?.days ?? [], [availableRounds, explorerTab]);

  return (
    <main className="page">
      <header className="hero">
        <h1>
          PolyEdge Predictor <span>实时研究看板</span>
        </h1>
        <div className="live-indicator">
          <span className={`dot ${wsStatus === "open" ? "ok" : ""}`} />
          {wsStatus === "open" ? "实时连接" : "重连中"}
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
        windowType={chartWindow["5m"]}
        onWindowChange={(v) => handleChartWindowChange("5m", v)}
        chart={charts["5m"]}
        loading={chartLoading["5m"]}
      />

      <MemoMarketSection
        title="15分钟市场"
        marketType="15m"
        live={live["15m"]}
        windowType={chartWindow["15m"]}
        onWindowChange={(v) => handleChartWindowChange("15m", v)}
        chart={charts["15m"]}
        loading={chartLoading["15m"]}
      />

      <section className="panel">
        <header className="panel-head">
          <h2>轮次浏览器</h2>
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
                  {formatTime(r.start_time_ms)} · {r.round_id}
                </option>
              ))}
            </select>
          </label>
        </div>
        {roundChart && roundChart.points.length > 0 ? (
          <MarketChart points={roundChart.points} rounds={roundChart.round ? [roundChart.round] : []} height={320} />
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
      />

      <AccuracyPanel accuracyTab={accuracyTab} onTabChange={setAccuracyTab} points={accuracyPoints} />

      <footer className="status-row">
        <span>WS状态: {wsStatus}</span>
        <span>5m最新Tick: {formatTime(live["5m"]?.timestamp_ms)}</span>
        <span>15m最新Tick: {formatTime(live["15m"]?.timestamp_ms)}</span>
        {errorText ? <span className="down">{errorText}</span> : null}
      </footer>
    </main>
  );
}
