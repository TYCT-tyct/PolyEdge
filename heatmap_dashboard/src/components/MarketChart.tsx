import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  PointerEvent as ReactPointerEvent,
  WheelEvent as ReactWheelEvent
} from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import type { ChartPoint, ChartRound } from "../types";
import type { WindowType } from "../types";

interface MarketChartProps {
  points: ChartPoint[];
  rounds: ChartRound[];
  windowType?: WindowType;
  timeMode: "local" | "et";
  height?: number;
}

interface DomainRange {
  minMs: number;
  maxMs: number;
}

interface ViewRange {
  startMs: number;
  endMs: number;
}

interface BrushState {
  leftPct: number;
  widthPct: number;
}

interface DragState {
  mode: "move" | "left" | "right" | "select";
  startX: number;
  startMs: number;
  endMs: number;
}

interface PreparedPlot {
  data: uPlot.AlignedData;
  bucketed: ChartPoint[];
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function windowToMs(view: WindowType | undefined): number {
  switch (view) {
    case "5m":
      return 5 * 60_000;
    case "15m":
      return 15 * 60_000;
    case "30m":
      return 30 * 60_000;
    case "1h":
      return 60 * 60_000;
    case "2h":
      return 2 * 60 * 60_000;
    case "4h":
      return 4 * 60 * 60_000;
    default:
      return 0;
  }
}

const ONE_SECOND_MS = 1_000;
const HOLD_LAST_MAX_GAP_MS = 3_000;
const PAIR_SUM_TOLERANCE_CENTS = 6;
const DISPLAY_CENT_STEP = 1.0;
const DELTA_AXIS_HEADROOM_PCT = 0.05;

function toMidCents(
  bid: number | null | undefined,
  ask: number | null | undefined
): number | null {
  const hasBid = bid != null && Number.isFinite(bid);
  const hasAsk = ask != null && Number.isFinite(ask);
  if (hasBid && hasAsk) {
    return clamp(((bid + ask) * 0.5) * 100, 0, 100);
  }
  if (hasBid) {
    return clamp(bid * 100, 0, 100);
  }
  if (hasAsk) {
    return clamp(ask * 100, 0, 100);
  }
  return null;
}

function toProbCents(prob: number | null | undefined): number | null {
  if (prob == null || !Number.isFinite(prob)) {
    return null;
  }
  return clamp(prob * 100, 0, 100);
}

function quantizeCents(value: number | null): number | null {
  if (value == null || !Number.isFinite(value)) {
    return null;
  }
  const snapped = Math.round(value / DISPLAY_CENT_STEP) * DISPLAY_CENT_STEP;
  return clamp(snapped, 0, 100);
}

function median(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1]! + sorted[mid]!) * 0.5;
  }
  return sorted[mid]!;
}

function computeRobustPctRange(values: Array<number | null>): [number, number] {
  const finite = values.filter((v): v is number => v != null && Number.isFinite(v));
  if (finite.length === 0) {
    return [-0.1, 0.1];
  }

  const minV = Math.min(...finite);
  const maxV = Math.max(...finite);
  const absMax = Math.max(Math.abs(minV), Math.abs(maxV));

  // Keep 0 as center and add explicit headroom for readability.
  const padded = absMax + DELTA_AXIS_HEADROOM_PCT;
  const step =
    padded < 0.20 ? 0.01 :
    padded < 0.60 ? 0.02 :
    padded < 1.50 ? 0.05 :
    padded < 3.00 ? 0.10 :
    padded < 8.00 ? 0.20 : 0.50;

  let bound = Math.ceil(padded / step) * step;
  bound = Number(bound.toFixed(4));
  if (!Number.isFinite(bound) || bound <= 0) {
    return [-0.1, 0.1];
  }
  if (bound < 0.05) {
    bound = 0.05;
  }
  return [-bound, bound];
}

function computeVisiblePctRange(
  xsSec: number[],
  values: Array<number | null>,
  startMs: number,
  endMs: number
): [number, number] {
  if (xsSec.length === 0 || values.length === 0 || xsSec.length !== values.length) {
    return computeRobustPctRange(values);
  }
  const startSec = Math.min(startMs, endMs) / 1000;
  const endSec = Math.max(startMs, endMs) / 1000;
  const visible: Array<number | null> = [];
  for (let i = 0; i < xsSec.length; i += 1) {
    const ts = xsSec[i];
    if (!Number.isFinite(ts)) {
      continue;
    }
    if (ts >= startSec && ts <= endSec) {
      visible.push(values[i] ?? null);
    }
  }
  if (visible.length === 0) {
    return computeRobustPctRange(values);
  }
  return computeRobustPctRange(visible);
}

function toDisplayUpCents(p: ChartPoint): number | null {
  return toProbCents(p.mid_yes) ?? toMidCents(p.best_bid_up, p.best_ask_up);
}

function toDisplayDownCents(p: ChartPoint): number | null {
  return toProbCents(p.mid_no) ?? toMidCents(p.best_bid_down, p.best_ask_down);
}

function normalizePairedCentsSoft(
  up: number | null,
  down: number | null
): { up: number | null; down: number | null } {
  if (up == null && down == null) {
    return { up: null, down: null };
  }
  if (up == null && down != null) {
    return { up: clamp(100 - down, 0, 100), down: clamp(down, 0, 100) };
  }
  if (down == null && up != null) {
    return { up: clamp(up, 0, 100), down: clamp(100 - up, 0, 100) };
  }

  const safeUp = clamp(up as number, 0, 100);
  const safeDown = clamp(down as number, 0, 100);
  const sum = safeUp + safeDown;
  if (Math.abs(sum - 100) <= PAIR_SUM_TOLERANCE_CENTS) {
    return { up: safeUp, down: safeDown };
  }
  if (sum <= 0) {
    return { up: null, down: null };
  }
  const scale = 100 / sum;
  return {
    up: clamp(safeUp * scale, 0, 100),
    down: clamp(safeDown * scale, 0, 100)
  };
}

function resolvePairFromPoint(p: ChartPoint): { up: number | null; down: number | null } {
  const rawUp = toDisplayUpCents(p);
  const rawDown = toDisplayDownCents(p);
  return normalizePairedCentsSoft(rawUp, rawDown);
}

function resolveDeltaPct(p: ChartPoint): number | null {
  if (p.delta_pct != null && Number.isFinite(p.delta_pct)) {
    return p.delta_pct;
  }
  if (
    p.btc_price != null &&
    p.target_price != null &&
    Number.isFinite(p.btc_price) &&
    Number.isFinite(p.target_price) &&
    p.target_price > 0
  ) {
    return ((p.btc_price - p.target_price) / p.target_price) * 100;
  }
  return null;
}

function sortAndDedupePoints(points: ChartPoint[]): ChartPoint[] {
  if (points.length < 2) {
    return points.filter((p) => Number.isFinite(p.timestamp_ms) && p.timestamp_ms > 0);
  }
  const sorted = points
    .filter((p) => Number.isFinite(p.timestamp_ms) && p.timestamp_ms > 0)
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
  return deduped;
}

function synthPoint(
  source: ChartPoint,
  timestampMs: number,
  delta: number | null,
  up: number | null,
  down: number | null,
  asGap = false
): ChartPoint {
  return {
    ...source,
    timestamp_ms: timestampMs,
    delta_pct: delta,
    mid_yes: up == null ? null : up / 100,
    mid_no: down == null ? null : down / 100,
    best_bid_up: asGap ? null : source.best_bid_up,
    best_ask_up: asGap ? null : source.best_ask_up,
    best_bid_down: asGap ? null : source.best_bid_down,
    best_ask_down: asGap ? null : source.best_ask_down,
    round_id: asGap ? "" : source.round_id,
    btc_price: asGap ? null : source.btc_price,
    target_price: asGap ? null : source.target_price
  };
}

function bucketizePoints(points: ChartPoint[]): ChartPoint[] {
  const sorted = sortAndDedupePoints(points);
  if (sorted.length < 2) {
    return sorted;
  }

  type BucketObservation = {
    source: ChartPoint;
    sourceTsMs: number;
    delta: number | null;
    up: number | null;
    down: number | null;
  };

  type BucketAgg = {
    bucketStartMs: number;
    roundId: string;
    source: ChartPoint;
    sourceTsMs: number;
    deltas: number[];
    ups: number[];
    downs: number[];
  };

  const buckets = new Map<string, BucketAgg>();
  for (const p of sorted) {
    const ts = p.timestamp_ms;
    const bucketStartMs = Math.floor(ts / ONE_SECOND_MS) * ONE_SECOND_MS;
    const roundId = (p.round_id ?? "").trim();
    const key = `${roundId}|${bucketStartMs}`;
    const pair = resolvePairFromPoint(p);
    const delta = resolveDeltaPct(p);
    const existing = buckets.get(key);
    if (!existing) {
      buckets.set(key, {
        bucketStartMs,
        roundId,
        source: p,
        sourceTsMs: ts,
        deltas: delta != null && Number.isFinite(delta) ? [delta] : [],
        ups: pair.up != null && Number.isFinite(pair.up) ? [pair.up] : [],
        downs: pair.down != null && Number.isFinite(pair.down) ? [pair.down] : []
      });
      continue;
    }
    if (ts >= existing.sourceTsMs) {
      existing.source = p;
      existing.sourceTsMs = ts;
    }
    if (delta != null && Number.isFinite(delta)) {
      existing.deltas.push(delta);
    }
    if (pair.up != null && Number.isFinite(pair.up)) {
      existing.ups.push(pair.up);
    }
    if (pair.down != null && Number.isFinite(pair.down)) {
      existing.downs.push(pair.down);
    }
  }

  const ordered = [...buckets.values()].sort((a, b) => {
    if (a.bucketStartMs !== b.bucketStartMs) {
      return a.bucketStartMs - b.bucketStartMs;
    }
    return a.sourceTsMs - b.sourceTsMs;
  });
  if (ordered.length === 0) {
    return [];
  }

  const out: ChartPoint[] = [];
  let lastObs: BucketObservation | null = null;
  let lastBucketStartMs: number | null = null;
  let lastRoundId = "";

  for (const agg of ordered) {
    if (
      lastObs &&
      lastBucketStartMs != null &&
      agg.roundId === lastRoundId &&
      agg.bucketStartMs > lastBucketStartMs + ONE_SECOND_MS
    ) {
      for (
        let bucket = lastBucketStartMs + ONE_SECOND_MS;
        bucket < agg.bucketStartMs;
        bucket += ONE_SECOND_MS
      ) {
        const ts = bucket + ONE_SECOND_MS - 1;
        if (bucket - lastObs.sourceTsMs <= HOLD_LAST_MAX_GAP_MS) {
          out.push(synthPoint(lastObs.source, ts, lastObs.delta, lastObs.up, lastObs.down));
        } else {
          // Long feed holes should be visible as gaps instead of synthetic continuity.
          out.push(synthPoint(lastObs.source, ts, null, null, null, true));
          lastObs = null;
          break;
        }
      }
    }

    const ts = agg.bucketStartMs + ONE_SECOND_MS - 1;
    const observedDelta = median(agg.deltas) ?? resolveDeltaPct(agg.source) ?? lastObs?.delta ?? null;
    let observedPair = normalizePairedCentsSoft(median(agg.ups), median(agg.downs));
    if (observedPair.up == null && observedPair.down == null && lastObs && agg.roundId === lastRoundId) {
      observedPair = { up: lastObs.up, down: lastObs.down };
    }

    const obs: BucketObservation = {
      source: agg.source,
      sourceTsMs: agg.sourceTsMs,
      delta: observedDelta,
      up: observedPair.up,
      down: observedPair.down
    };
    lastObs = obs;
    lastBucketStartMs = agg.bucketStartMs;
    lastRoundId = agg.roundId;
    out.push(synthPoint(obs.source, ts, obs.delta, obs.up, obs.down));
  }
  return out;
}

function preparePlot(points: ChartPoint[]): PreparedPlot {
  const bucketed = bucketizePoints(points);
  const xs: number[] = [];
  const delta: Array<number | null> = [];
  const upRaw: Array<number | null> = [];

  for (const p of bucketed) {
    if (!Number.isFinite(p.timestamp_ms) || p.timestamp_ms <= 0) {
      continue;
    }
    const pair = resolvePairFromPoint(p);
    xs.push(p.timestamp_ms / 1000);
    delta.push(resolveDeltaPct(p));
    upRaw.push(pair.up);
  }

  const upDisplay = upRaw.map((v) => quantizeCents(v));
  const downDisplay = upDisplay.map((v) => (v == null ? null : quantizeCents(100 - v)));
  const deltaDisplay = delta.map((v) =>
    v == null || !Number.isFinite(v) ? null : Number(v.toFixed(4))
  );

  return {
    data: [xs, deltaDisplay, upDisplay, downDisplay],
    bucketed
  };
}

function buildMiniPath(points: ChartPoint[], width: number, height: number): string {
  if (points.length < 2 || width <= 0 || height <= 0) {
    return "";
  }
  const valid = points
    .map((p) => ({ ts: p.timestamp_ms, delta: resolveDeltaPct(p) }))
    .filter((p) => Number.isFinite(p.ts) && p.ts > 0 && p.delta != null && Number.isFinite(p.delta));
  if (valid.length < 2) {
    return "";
  }

  const minX = valid[0]!.ts;
  const maxX = valid[valid.length - 1]!.ts;
  const ys = valid.map((p) => p.delta as number);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const xSpan = Math.max(1, maxX - minX);
  const ySpan = Math.max(1e-9, maxY - minY);

  let d = "";
  for (let i = 0; i < valid.length; i += 1) {
    const p = valid[i]!;
    const x = ((p.ts - minX) / xSpan) * width;
    const y = height - (((p.delta as number) - minY) / ySpan) * height;
    d += `${i === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)} `;
  }
  return d.trim();
}

function computeDomain(points: ChartPoint[]): DomainRange | null {
  if (points.length < 2) {
    return null;
  }
  let minMs = Number.POSITIVE_INFINITY;
  let maxMs = 0;
  let validCount = 0;
  for (const p of points) {
    const ts = p.timestamp_ms;
    if (!Number.isFinite(ts) || ts <= 0) {
      continue;
    }
    validCount += 1;
    if (ts < minMs) {
      minMs = ts;
    }
    if (ts > maxMs) {
      maxMs = ts;
    }
  }
  if (validCount < 2 || !Number.isFinite(minMs) || maxMs <= 0) {
    return null;
  }
  return {
    minMs,
    maxMs
  };
}

function viewToBrush(domain: DomainRange, view: ViewRange): BrushState {
  const span = Math.max(1, domain.maxMs - domain.minMs);
  const leftPct = ((view.startMs - domain.minMs) / span) * 100;
  const widthPct = ((view.endMs - view.startMs) / span) * 100;
  return {
    leftPct: clamp(leftPct, 0, 100),
    widthPct: clamp(widthPct, 0.6, 100)
  };
}

function MarketChartImpl({ points, rounds, windowType = "all", timeMode, height = 420 }: MarketChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const miniCanvasRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  const tooltipRef = useRef<HTMLDivElement | null>(null);
  const bucketedRef = useRef<ChartPoint[]>([]);
  const roundsRef = useRef<ChartRound[]>(rounds);
  const domainRef = useRef<DomainRange | null>(null);
  const viewRef = useRef<ViewRange | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const userAdjustedRef = useRef(false);
  const lastRoundIdRef = useRef<string>("");

  const prepared = useMemo(() => preparePlot(points), [points]);
  const data = prepared.data;
  const miniPath = useMemo(() => buildMiniPath(prepared.bucketed, 1200, 38), [prepared.bucketed]);
  const domain = useMemo(() => computeDomain(prepared.bucketed), [prepared.bucketed]);
  const steppedPathBuilder = useMemo(() => uPlot.paths.stepped?.({ align: 1 }), []);
  const axisTimeFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat("zh-CN", {
        timeZone: timeMode === "et" ? "America/New_York" : undefined,
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      }),
    [timeMode]
  );
  const legendTimeFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat("zh-CN", {
        timeZone: timeMode === "et" ? "America/New_York" : undefined,
        hour12: false,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      }),
    [timeMode]
  );
  const formatMiniTs = useCallback(
    (tsMs: number): string => {
      if (!Number.isFinite(tsMs) || tsMs <= 0) {
        return "--:--:--";
      }
      return axisTimeFormatter.format(new Date(tsMs));
    },
    [axisTimeFormatter]
  );
  const formatLegendTs = useCallback(
    (tsSec: number): string => {
      if (!Number.isFinite(tsSec) || tsSec <= 0) {
        return "--";
      }
      return legendTimeFormatter.format(new Date(tsSec * 1000));
    },
    [legendTimeFormatter]
  );

  const [brush, setBrush] = useState<BrushState>({ leftPct: 0, widthPct: 100 });

  const syncPctScaleToView = useCallback((startMs: number, endMs: number) => {
    const plot = plotRef.current;
    if (!plot) {
      return;
    }
    const xs = ((plot.data[0] as number[]) ?? []).slice();
    const deltas = ((plot.data[1] as Array<number | null>) ?? []).slice();
    const [min, max] = computeVisiblePctRange(xs, deltas, startMs, endMs);
    plot.setScale("pct", { min, max });
  }, []);

  const applyView = useCallback(
    (startMsRaw: number, endMsRaw: number, fromUser: boolean) => {
      const d = domainRef.current;
      if (!d) {
        return;
      }
      const fullSpan = Math.max(1, d.maxMs - d.minMs);
      const minSpan = Math.max(2000, Math.round(fullSpan * 0.004));
      let startMs = clamp(startMsRaw, d.minMs, d.maxMs);
      let endMs = clamp(endMsRaw, d.minMs, d.maxMs);
      if (endMs < startMs) {
        const t = startMs;
        startMs = endMs;
        endMs = t;
      }
      if (endMs - startMs < minSpan) {
        const mid = (startMs + endMs) * 0.5;
        startMs = clamp(mid - minSpan * 0.5, d.minMs, d.maxMs - minSpan);
        endMs = clamp(startMs + minSpan, d.minMs + minSpan, d.maxMs);
      }

      if (fromUser) {
        userAdjustedRef.current = true;
      }

      viewRef.current = { startMs, endMs };
      setBrush(viewToBrush(d, { startMs, endMs }));
      plotRef.current?.setScale("x", { min: startMs / 1000, max: endMs / 1000 });
      syncPctScaleToView(startMs, endMs);
    },
    [syncPctScaleToView]
  );

  const applyWindowPreset = useCallback(
    (nextWindowType: WindowType, anchorEndMs?: number | null) => {
      const d = domainRef.current;
      if (!d) {
        return;
      }
      const fullSpan = Math.max(1, d.maxMs - d.minMs);
      const targetSpan = windowToMs(nextWindowType);
      if (targetSpan <= 0 || targetSpan >= fullSpan) {
        applyView(d.minMs, d.maxMs, false);
        return;
      }
      const endMs = clamp(anchorEndMs ?? d.maxMs, d.minMs, d.maxMs);
      const startMs = endMs - targetSpan;
      applyView(startMs, endMs, false);
    },
    [applyView]
  );

  const resetView = useCallback(() => {
    const d = domainRef.current;
    if (!d) {
      return;
    }
    userAdjustedRef.current = false;
    applyView(d.minMs, d.maxMs, false);
  }, [applyView]);

  useEffect(() => {
    roundsRef.current = rounds;
    plotRef.current?.redraw();
  }, [rounds]);

  useEffect(() => {
    bucketedRef.current = prepared.bucketed;
  }, [prepared.bucketed]);

  useEffect(() => {
    const root = rootRef.current;
    if (!root) {
      return;
    }

    const tooltipEl = document.createElement("div");
    tooltipEl.className = "chart-tooltip";
    tooltipEl.style.display = "none";
    root.appendChild(tooltipEl);
    tooltipRef.current = tooltipEl;

    const plot = new uPlot(
      {
        width: Math.max(640, root.clientWidth),
        height,
        focus: { alpha: 0.2 },
        cursor: { drag: { x: true, y: false } },
        scales: {
          x: { time: true },
          pct: {
            auto: false,
            range: (u) => computeRobustPctRange((u.data[1] as Array<number | null>) ?? [])
          },
          prob: { range: [0, 100] }
        },
        axes: [
          {
            stroke: "#706c5c",
            grid: { stroke: "rgba(104, 95, 72, 0.22)" },
            ticks: { stroke: "#706c5c" },
            space: 90,
            values: (_u, vals) =>
              vals.map((v) => axisTimeFormatter.format(new Date(v * 1000)))
          },
          {
            scale: "pct",
            stroke: "#f0df4e",
            grid: { stroke: "rgba(104, 95, 72, 0.24)" },
            values: (_u, vals) =>
              vals.map((v) => {
                const sign = v > 0 ? "+" : "";
                return `${sign}${v.toFixed(2)}%`;
              })
          },
          {
            scale: "prob",
            side: 1,
            stroke: "#9e9580",
            grid: { show: false },
            values: (_u, vals) => vals.map((v) => `${Math.round(v)}¢`)
          }
        ],
        series: [
          {
            value: (_u, v) => formatLegendTs(v as number)
          },
          {
            label: "Δ价差%",
            scale: "pct",
            stroke: "#efe349",
            width: 1.7,
            spanGaps: false,
            paths: steppedPathBuilder,
            points: { show: false }
          },
          {
            label: "看涨价格",
            scale: "prob",
            stroke: "#64be4e",
            width: 1.55,
            spanGaps: false,
            paths: steppedPathBuilder,
            points: { show: false }
          },
          {
            label: "看跌价格",
            scale: "prob",
            stroke: "#ff2f57",
            width: 1.55,
            spanGaps: false,
            paths: steppedPathBuilder,
            points: { show: false }
          }
        ],
        hooks: {
          setCursor: [
            (u) => {
              const tip = tooltipRef.current;
              if (!tip) {
                return;
              }
              const plotBox = u.bbox;
              const cursorLeft = u.cursor.left;
              const cursorTop = u.cursor.top;
              if (
                cursorLeft == null ||
                cursorTop == null ||
                !Number.isFinite(cursorLeft) ||
                !Number.isFinite(cursorTop) ||
                cursorLeft < 0 ||
                cursorTop < 0 ||
                cursorLeft > plotBox.width ||
                cursorTop > plotBox.height
              ) {
                tip.style.display = "none";
                return;
              }
              let idx: number | null = null;
              if (u.cursor.idx != null && u.cursor.idx >= 0) {
                idx = u.cursor.idx;
              }
              if (idx == null && u.cursor.left != null && Number.isFinite(u.cursor.left)) {
                try {
                  idx = u.posToIdx(u.cursor.left);
                } catch {
                  idx = null;
                }
              }
              if (idx == null || idx < 0) {
                tip.style.display = "none";
                return;
              }
              const xs = (u.data[0] as number[]) ?? [];
              if (idx >= xs.length) {
                tip.style.display = "none";
                return;
              }

              const tsSec = xs[idx];
              const delta = ((u.data[1] as Array<number | null>) ?? [])[idx];
              const up = ((u.data[2] as Array<number | null>) ?? [])[idx];
              const down = ((u.data[3] as Array<number | null>) ?? [])[idx];
              const row = bucketedRef.current[idx] ?? null;
              const btc = row?.btc_price ?? null;
              const target = row?.target_price ?? null;

              const fmtPct = (v: number | null | undefined) =>
                v == null || !Number.isFinite(v) ? "--" : `${v > 0 ? "+" : ""}${v.toFixed(4)}%`;
              const fmtCent = (v: number | null | undefined) =>
                v == null || !Number.isFinite(v) ? "--" : `${v.toFixed(1)}¢`;
              const fmtUsd = (v: number | null | undefined) =>
                v == null || !Number.isFinite(v)
                  ? "--"
                  : `$${v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

              tip.innerHTML = `
                <div class="tt-time">${formatLegendTs(tsSec)}</div>
                <div class="tt-row"><span>Δ价差</span><strong class="${(delta ?? 0) >= 0 ? "up" : "down"}">${fmtPct(delta)}</strong></div>
                <div class="tt-row"><span>看涨价格</span><strong class="up">${fmtCent(up)}</strong></div>
                <div class="tt-row"><span>看跌价格</span><strong class="down">${fmtCent(down)}</strong></div>
                <div class="tt-row"><span>比特币</span><strong>${fmtUsd(btc)}</strong></div>
                <div class="tt-row"><span>目标价</span><strong>${fmtUsd(target)}</strong></div>
              `;
              tip.style.display = "block";

              const w = tip.offsetWidth || 220;
              const h = tip.offsetHeight || 140;
              const rootWidth = u.root.clientWidth;
              const rootHeight = u.root.clientHeight;

              const anchorX = Math.round(cursorLeft + plotBox.left);
              const anchorY = Math.round(cursorTop + plotBox.top);

              // Preferred placement: right-bottom of crosshair intersection.
              let left = anchorX + 8;
              let top = anchorY + 8;

              // Flip if overflow.
              if (left + w > rootWidth - 6) {
                left = anchorX - w - 8;
              }
              if (top + h > rootHeight - 6) {
                top = anchorY - h - 8;
              }

              left = clamp(left, 6, Math.max(6, rootWidth - w - 6));
              top = clamp(top, 6, Math.max(6, rootHeight - h - 6));

              tip.style.left = `${left}px`;
              tip.style.top = `${top}px`;
            }
          ],
          draw: [
            (u) => {
              const ctx = u.ctx;
              const { left, top, width, height: chartHeight } = u.bbox;

              ctx.save();
              const zeroY = Math.round(u.valToPos(0, "pct", true));
              ctx.strokeStyle = "rgba(239, 227, 76, 0.45)";
              ctx.setLineDash([5, 5]);
              ctx.beginPath();
              ctx.moveTo(left, zeroY);
              ctx.lineTo(left + width, zeroY);
              ctx.stroke();
              ctx.setLineDash([]);

              ctx.strokeStyle = "rgba(102, 95, 75, 0.3)";
              for (const r of roundsRef.current) {
                const xEndMs = r.end_ts_ms ?? r.end_time_ms ?? 0;
                if (xEndMs <= 0) {
                  continue;
                }
                const x = Math.round(u.valToPos(xEndMs / 1000, "x", true));
                if (x >= left && x <= left + width) {
                  ctx.beginPath();
                  ctx.moveTo(x, top);
                  ctx.lineTo(x, top + chartHeight);
                  ctx.stroke();
                }
              }
              ctx.restore();
            }
          ]
        }
      },
      data,
      root
    );

    plotRef.current = plot;
    const onResize = () => {
      const current = plotRef.current;
      const host = rootRef.current;
      if (!current || !host) {
        return;
      }
      current.setSize({ width: Math.max(640, host.clientWidth), height });
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      tooltipRef.current = null;
      tooltipEl.remove();
      plot.destroy();
      plotRef.current = null;
    };
  }, [axisTimeFormatter, formatLegendTs, height, steppedPathBuilder]);

  useEffect(() => {
    const plot = plotRef.current;
    if (!plot) {
      return;
    }
    plot.setData(data);
    const curView = viewRef.current;
    if (curView) {
      syncPctScaleToView(curView.startMs, curView.endMs);
      return;
    }
    const d = domainRef.current;
    if (d) {
      syncPctScaleToView(d.minMs, d.maxMs);
    }
  }, [data, syncPctScaleToView]);

  useEffect(() => {
    domainRef.current = domain;
    if (!domain) {
      return;
    }
    if (!viewRef.current || !userAdjustedRef.current) {
      applyWindowPreset(windowType, domain.maxMs);
      return;
    }
    const cur = viewRef.current;
    const span = cur.endMs - cur.startMs;
    if (!Number.isFinite(span) || span <= 0) {
      applyView(domain.minMs, domain.maxMs, false);
      return;
    }
    const start = clamp(cur.startMs, domain.minMs, domain.maxMs);
    const end = clamp(cur.endMs, domain.minMs, domain.maxMs);
    applyView(start, end, false);
  }, [domain, applyView, applyWindowPreset, windowType]);

  useEffect(() => {
    userAdjustedRef.current = false;
    const anchor = viewRef.current?.endMs ?? domainRef.current?.maxMs ?? null;
    applyWindowPreset(windowType, anchor);
  }, [windowType, applyWindowPreset]);

  useEffect(() => {
    const latestRoundId = prepared.bucketed[prepared.bucketed.length - 1]?.round_id ?? "";
    if (!latestRoundId) {
      return;
    }

    const prevRoundId = lastRoundIdRef.current;
    lastRoundIdRef.current = latestRoundId;
    if (!prevRoundId || prevRoundId === latestRoundId) {
      return;
    }

    // On market rollover, jump viewport so the new round is immediately visible.
    const d = domainRef.current;
    if (!d) {
      return;
    }
    const latestRoundStartMs =
      prepared.bucketed.find((p) => p.round_id === latestRoundId)?.timestamp_ms ?? null;
    const prevView = viewRef.current;
    const prevSpan = prevView ? Math.max(5_000, prevView.endMs - prevView.startMs) : d.maxMs - d.minMs;
    userAdjustedRef.current = false;
    if (latestRoundStartMs != null && Number.isFinite(latestRoundStartMs) && latestRoundStartMs > 0) {
      const preRollMs = Math.min(20_000, Math.max(3_000, Math.round(prevSpan * 0.08)));
      const anchorStart = latestRoundStartMs - preRollMs;
      applyView(anchorStart, anchorStart + prevSpan, false);
      return;
    }
    applyView(d.minMs, d.maxMs, false);
  }, [prepared.bucketed, applyView]);

  useEffect(() => {
    const onPointerMove = (ev: PointerEvent) => {
      const drag = dragRef.current;
      const d = domainRef.current;
      const host = miniCanvasRef.current;
      if (!drag || !d || !host) {
        return;
      }
      const rect = host.getBoundingClientRect();
      const width = Math.max(1, rect.width);
      const msPerPx = Math.max(1, (d.maxMs - d.minMs) / width);
      const deltaMs = (ev.clientX - drag.startX) * msPerPx;

      if (drag.mode === "move") {
        applyView(drag.startMs + deltaMs, drag.endMs + deltaMs, true);
      } else if (drag.mode === "left") {
        applyView(drag.startMs + deltaMs, drag.endMs, true);
      } else if (drag.mode === "select") {
        const x = clamp(ev.clientX - rect.left, 0, width);
        const ms = d.minMs + x * msPerPx;
        applyView(Math.min(drag.startMs, ms), Math.max(drag.startMs, ms), true);
      } else {
        applyView(drag.startMs, drag.endMs + deltaMs, true);
      }
    };

    const onPointerUp = () => {
      dragRef.current = null;
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
    };

    if (dragRef.current) {
      window.addEventListener("pointermove", onPointerMove);
      window.addEventListener("pointerup", onPointerUp);
    }

    return () => {
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
    };
  }, [applyView]);

  const onMiniPointerDown = useCallback(
    (ev: ReactPointerEvent<HTMLDivElement>) => {
      const d = domainRef.current;
      const view = viewRef.current;
      const host = miniCanvasRef.current;
      if (!d || !view || !host) {
        return;
      }

      const rect = host.getBoundingClientRect();
      const width = Math.max(1, rect.width);
      const x = clamp(ev.clientX - rect.left, 0, width);
      const leftPx = (brush.leftPct / 100) * width;
      const rightPx = ((brush.leftPct + brush.widthPct) / 100) * width;
      const handleHit = 12;

      let mode: DragState["mode"] = "move";
      if (Math.abs(x - leftPx) <= handleHit) {
        mode = "left";
      } else if (Math.abs(x - rightPx) <= handleHit) {
        mode = "right";
      } else if (brush.widthPct >= 99.5) {
        const msPerPx = Math.max(1, (d.maxMs - d.minMs) / width);
        const ms = d.minMs + x * msPerPx;
        dragRef.current = {
          mode: "select",
          startX: ev.clientX,
          startMs: ms,
          endMs: ms
        };
        return;
      } else if (x < leftPx || x > rightPx) {
        const curSpan = view.endMs - view.startMs;
        const centerPct = x / width;
        const centerMs = d.minMs + (d.maxMs - d.minMs) * centerPct;
        applyView(centerMs - curSpan * 0.5, centerMs + curSpan * 0.5, true);
        const cur = viewRef.current;
        if (!cur) {
          return;
        }
        dragRef.current = {
          mode: "move",
          startX: ev.clientX,
          startMs: cur.startMs,
          endMs: cur.endMs
        };
        return;
      }

      dragRef.current = {
        mode,
        startX: ev.clientX,
        startMs: view.startMs,
        endMs: view.endMs
      };
    },
    [applyView, brush.leftPct, brush.widthPct]
  );

  const onMiniWheel = useCallback(
    (ev: ReactWheelEvent<HTMLDivElement>) => {
      const d = domainRef.current;
      const view = viewRef.current;
      const host = miniCanvasRef.current;
      if (!d || !view || !host) {
        return;
      }
      ev.preventDefault();
      const rect = host.getBoundingClientRect();
      const width = Math.max(1, rect.width);
      const cursorPct = clamp((ev.clientX - rect.left) / width, 0, 1);
      const focusMs = d.minMs + (d.maxMs - d.minMs) * cursorPct;
      const span = Math.max(1500, view.endMs - view.startMs);
      const zoom = ev.deltaY > 0 ? 1.2 : 0.82;
      const nextSpan = clamp(span * zoom, 1500, d.maxMs - d.minMs);
      const start = focusMs - nextSpan * cursorPct;
      const end = start + nextSpan;
      applyView(start, end, true);
    },
    [applyView]
  );

  const miniStart = domain?.minMs ?? 0;
  const miniEnd = domain?.maxMs ?? 0;

  return (
    <div className="chart-wrap">
      <div ref={rootRef} className="chart-root" />
      <div className="mini-strip">
        <div className="mini-ts">{formatMiniTs(miniStart)}</div>
        <div
          ref={miniCanvasRef}
          className="mini-canvas"
          onPointerDown={onMiniPointerDown}
          onWheel={onMiniWheel}
          onDoubleClick={resetView}
        >
          <svg viewBox="0 0 1200 38" preserveAspectRatio="none" className="mini-svg">
            <path d={miniPath} className="mini-path" />
          </svg>
          <div
            className="mini-brush"
            style={{
              left: `${brush.leftPct}%`,
              width: `${brush.widthPct}%`
            }}
          >
            <span className="mini-handle left" />
            <span className="mini-handle right" />
          </div>
        </div>
        <div className="mini-ts">{formatMiniTs(miniEnd)}</div>
      </div>
    </div>
  );
}

export const MarketChart = memo(MarketChartImpl);
