import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  PointerEvent as ReactPointerEvent,
  WheelEvent as ReactWheelEvent
} from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import type { ChartPoint, ChartRound } from "../types";

interface MarketChartProps {
  points: ChartPoint[];
  rounds: ChartRound[];
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

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function computeGapThresholdMs(points: ChartPoint[]): number {
  if (points.length < 4) {
    return 3000;
  }
  const deltas: number[] = [];
  let prev = points[0]?.timestamp_ms ?? 0;
  for (let i = 1; i < points.length; i += 1) {
    const cur = points[i]?.timestamp_ms ?? 0;
    if (cur > prev) {
      deltas.push(cur - prev);
    }
    prev = cur;
  }
  if (deltas.length === 0) {
    return 3000;
  }
  deltas.sort((a, b) => a - b);
  const median = deltas[Math.floor(deltas.length / 2)] ?? 100;
  return Math.max(3000, Math.min(120_000, median * 8));
}

function toMidCents(
  bid: number | null | undefined,
  ask: number | null | undefined
): number | null {
  const hasBid = bid != null && Number.isFinite(bid);
  const hasAsk = ask != null && Number.isFinite(ask);
  if (hasBid && hasAsk) {
    return (((bid as number) + (ask as number)) * 0.5) * 100;
  }
  if (hasBid) {
    return (bid as number) * 100;
  }
  if (hasAsk) {
    return (ask as number) * 100;
  }
  return null;
}

function toPlotData(points: ChartPoint[]): uPlot.AlignedData {
  const bucketed = bucketizePoints(points);
  const xs: number[] = [];
  const delta: Array<number | null> = [];
  const up: Array<number | null> = [];
  const down: Array<number | null> = [];
  const gapThresholdMs = computeGapThresholdMs(bucketed);
  let prevTs = 0;

  for (const p of bucketed) {
    if (!Number.isFinite(p.timestamp_ms) || p.timestamp_ms <= 0) {
      continue;
    }
    if (prevTs > 0 && p.timestamp_ms > prevTs + gapThresholdMs) {
      xs.push((prevTs + 1) / 1000);
      delta.push(null);
      up.push(null);
      down.push(null);
    }

    xs.push(p.timestamp_ms / 1000);
    delta.push(p.delta_pct);
    let upC = toMidCents(p.best_bid_up, p.best_ask_up);
    let downC = toMidCents(p.best_bid_down, p.best_ask_down);
    if (upC != null && downC != null) {
      const sum = upC + downC;
      if (sum > 1e-9 && Math.abs(sum - 100) > 8) {
        const scale = 100 / sum;
        upC *= scale;
        downC *= scale;
      }
      upC = clamp(upC, 0, 100);
      downC = clamp(downC, 0, 100);
    }
    up.push(upC);
    down.push(downC);
    prevTs = p.timestamp_ms;
  }

  const bucketMs = inferBucketMs(bucketed);
  const alpha = 0.34;
  const maxStep = 8;
  const upSmoothed = smoothProbSeries(up, alpha, maxStep);
  const downSmoothed = smoothProbSeries(down, alpha, maxStep);
  const [upNorm, downNorm] = normalizePairSeries(upSmoothed, downSmoothed);

  return [xs, delta, upNorm, downNorm];
}

function median(values: number[]): number {
  if (values.length === 0) {
    return NaN;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1]! + sorted[mid]!) * 0.5;
  }
  return sorted[mid]!;
}

function inferBucketMs(points: ChartPoint[]): number {
  return 1000;
}

function bucketizePoints(points: ChartPoint[]): ChartPoint[] {
  if (points.length < 4) {
    return points;
  }
  const bucketMs = inferBucketMs(points);
  if (bucketMs <= 100) {
    return points;
  }

  const buckets = new Map<number, ChartPoint[]>();
  for (const p of points) {
    const ts = p.timestamp_ms;
    if (!Number.isFinite(ts) || ts <= 0) {
      continue;
    }
    const key = Math.floor(ts / bucketMs) * bucketMs;
    const arr = buckets.get(key);
    if (arr) {
      arr.push(p);
    } else {
      buckets.set(key, [p]);
    }
  }

  const keys = [...buckets.keys()].sort((a, b) => a - b);
  const out: ChartPoint[] = [];
  for (const key of keys) {
    const arr = buckets.get(key)!;
    if (arr.length === 0) {
      continue;
    }
    const last = arr[arr.length - 1]!;
    const deltas = arr.map((x) => x.delta_pct).filter((v): v is number => v != null && Number.isFinite(v));
    const ups = arr
      .map((x) => toMidCents(x.best_bid_up, x.best_ask_up))
      .filter((v): v is number => v != null && Number.isFinite(v));
    const downs = arr
      .map((x) => toMidCents(x.best_bid_down, x.best_ask_down))
      .filter((v): v is number => v != null && Number.isFinite(v));

    const upC = ups.length > 0 ? median(ups) / 100 : null;
    const downC = downs.length > 0 ? median(downs) / 100 : null;
    out.push({
      ...last,
      timestamp_ms: key + bucketMs - 1,
      delta_pct: deltas.length > 0 ? median(deltas) : last.delta_pct,
      best_bid_up: upC,
      best_ask_up: upC,
      best_bid_down: downC,
      best_ask_down: downC
    });
  }
  return out;
}

function smoothProbSeries(
  values: Array<number | null>,
  alpha: number,
  maxStep: number
): Array<number | null> {
  const medianed = values.slice();
  for (let i = 1; i < values.length - 1; i += 1) {
    const a = values[i - 1];
    const b = values[i];
    const c = values[i + 1];
    if (a == null || b == null || c == null) {
      continue;
    }
    const arr = [a, b, c].sort((x, y) => x - y);
    medianed[i] = arr[1] ?? b;
  }

  const out: Array<number | null> = [];
  let prev: number | null = null;
  for (const v of medianed) {
    if (v == null || !Number.isFinite(v)) {
      out.push(null);
      prev = null;
      continue;
    }
    const cur = clamp(v, 0, 100);
    if (prev == null) {
      out.push(cur);
      prev = cur;
      continue;
    }
    let next = prev + alpha * (cur - prev);
    const delta = next - prev;
    if (delta > maxStep) {
      next = prev + maxStep;
    } else if (delta < -maxStep) {
      next = prev - maxStep;
    }
    next = clamp(next, 0, 100);
    out.push(next);
    prev = next;
  }
  return out;
}

function normalizePairSeries(
  up: Array<number | null>,
  down: Array<number | null>
): [Array<number | null>, Array<number | null>] {
  const upOut = up.slice();
  const downOut = down.slice();
  for (let i = 0; i < upOut.length; i += 1) {
    const u = upOut[i];
    const d = downOut[i];
    if (u == null || d == null) {
      continue;
    }
    const sum = u + d;
    if (sum <= 1e-9) {
      continue;
    }
    const scale = 100 / sum;
    upOut[i] = clamp(u * scale, 0, 100);
    downOut[i] = clamp(d * scale, 0, 100);
  }
  return [upOut, downOut];
}

function buildMiniPath(points: ChartPoint[], width: number, height: number): string {
  if (points.length < 2 || width <= 0 || height <= 0) {
    return "";
  }
  const valid = points.filter((p) => Number.isFinite(p.timestamp_ms) && Number.isFinite(p.delta_pct ?? NaN));
  if (valid.length < 2) {
    return "";
  }

  const minX = valid[0]!.timestamp_ms;
  const maxX = valid[valid.length - 1]!.timestamp_ms;
  const ys = valid.map((p) => p.delta_pct as number);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const xSpan = Math.max(1, maxX - minX);
  const ySpan = Math.max(1e-9, maxY - minY);

  let d = "";
  for (let i = 0; i < valid.length; i += 1) {
    const p = valid[i]!;
    const x = ((p.timestamp_ms - minX) / xSpan) * width;
    const y = height - (((p.delta_pct as number) - minY) / ySpan) * height;
    d += `${i === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)} `;
  }
  return d.trim();
}

function formatMiniTs(tsMs: number): string {
  if (!Number.isFinite(tsMs) || tsMs <= 0) {
    return "--:--";
  }
  return new Date(tsMs).toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit"
  });
}

function computeDomain(points: ChartPoint[]): DomainRange | null {
  const validTs = points
    .map((p) => p.timestamp_ms)
    .filter((ts) => Number.isFinite(ts) && ts > 0)
    .sort((a, b) => a - b);
  if (validTs.length < 2) {
    return null;
  }
  return {
    minMs: validTs[0]!,
    maxMs: validTs[validTs.length - 1]!
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

function MarketChartImpl({ points, rounds, height = 420 }: MarketChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const miniCanvasRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  const roundsRef = useRef<ChartRound[]>(rounds);
  const domainRef = useRef<DomainRange | null>(null);
  const viewRef = useRef<ViewRange | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const userAdjustedRef = useRef(false);

  const data = useMemo(() => toPlotData(points), [points]);
  const miniPath = useMemo(() => buildMiniPath(points, 1200, 38), [points]);
  const domain = useMemo(() => computeDomain(points), [points]);

  const [brush, setBrush] = useState<BrushState>({ leftPct: 0, widthPct: 100 });

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
    },
    []
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
    const root = rootRef.current;
    if (!root) {
      return;
    }

    const steppedPath = uPlot.paths.stepped({ align: 1 });
    const linearPath = uPlot.paths.linear();
    const plot = new uPlot(
      {
        width: Math.max(640, root.clientWidth),
        height,
        focus: { alpha: 0.2 },
        cursor: { drag: { x: true, y: false } },
        scales: {
          x: { time: true },
          pct: { auto: true },
          prob: { range: [0, 100] }
        },
        axes: [
          {
            stroke: "#706c5c",
            grid: { stroke: "rgba(104, 95, 72, 0.22)" },
            ticks: { stroke: "#706c5c" },
            values: (_u, vals) =>
              vals.map((v) =>
                new Date(v * 1000).toLocaleTimeString("en-US", {
                  hour12: false,
                  hour: "2-digit",
                  minute: "2-digit"
                })
              )
          },
          {
            scale: "pct",
            stroke: "#f0df4e",
            grid: { stroke: "rgba(104, 95, 72, 0.24)" },
            values: (_u, vals) => vals.map((v) => `${v.toFixed(2)}%`)
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
          {},
          {
            label: "Δ价差%",
            scale: "pct",
            stroke: "#efe349",
            width: 1.7,
            paths: steppedPath,
            points: { show: false }
          },
          {
            label: "看涨价格",
            scale: "prob",
            stroke: "#64be4e",
            width: 1.35,
            paths: linearPath,
            points: { show: false }
          },
          {
            label: "看跌价格",
            scale: "prob",
            stroke: "#ff2f57",
            width: 1.35,
            paths: linearPath,
            points: { show: false }
          }
        ],
        hooks: {
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
      plot.destroy();
      plotRef.current = null;
    };
  }, [height]);

  useEffect(() => {
    plotRef.current?.setData(data);
  }, [data]);

  useEffect(() => {
    domainRef.current = domain;
    if (!domain) {
      return;
    }
    if (!viewRef.current || !userAdjustedRef.current) {
      applyView(domain.minMs, domain.maxMs, false);
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
  }, [domain, applyView]);

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
