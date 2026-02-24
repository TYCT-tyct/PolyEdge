import { memo, useEffect, useMemo, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import type { AccuracyPoint } from "../types";

interface AccuracyChartProps {
  points: AccuracyPoint[];
  timeMode: "local" | "et";
}

const HALF_HOUR_SEC = 30 * 60;
const LOOKBACK_SEC = 24 * 60 * 60;

function axisParts(formatter: Intl.DateTimeFormat, tsSec: number): {
  mm: string;
  dd: string;
  hh: string;
  mi: string;
} {
  const parts = formatter.formatToParts(new Date(tsSec * 1000));
  let mm = "";
  let dd = "";
  let hh = "";
  let mi = "";
  for (const p of parts) {
    if (p.type === "month") {
      mm = p.value;
    } else if (p.type === "day") {
      dd = p.value;
    } else if (p.type === "hour") {
      hh = p.value;
    } else if (p.type === "minute") {
      mi = p.value;
    }
  }
  return { mm, dd, hh, mi };
}

function formatAxisTs(formatter: Intl.DateTimeFormat, tsSec: number, includeDate: boolean): string {
  const { mm, dd, hh, mi } = axisParts(formatter, tsSec);
  if (!hh || !mi) {
    return formatter.format(new Date(tsSec * 1000));
  }
  if (!includeDate) {
    return `${hh}:${mi}`;
  }
  if (!mm || !dd) {
    return `${hh}:${mi}`;
  }
  return `${mm}/${dd} ${hh}:${mi}`;
}

function toData(points: AccuracyPoint[]): uPlot.AlignedData {
  const xs: number[] = [];
  const ys: Array<number | null> = [];
  for (const p of points) {
    xs.push(p.timestamp_ms / 1000);
    ys.push(p.sample_count > 0 ? p.accuracy_pct : null);
  }
  return [xs, ys];
}

function AccuracyChartImpl({ points, timeMode }: AccuracyChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  const data = useMemo(() => toData(points), [points]);
  const axisTimeFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat("en-US", {
        timeZone: timeMode === "et" ? "America/New_York" : undefined,
        hour12: false,
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit"
      }),
    [timeMode]
  );

  useEffect(() => {
    const root = rootRef.current;
    if (!root) {
      return;
    }

    const plot = new uPlot(
      {
        width: Math.max(640, root.clientWidth),
        height: 250,
        scales: {
          x: {
            time: true,
            range: () => {
              const nowSec = Date.now() / 1000;
              return [nowSec - LOOKBACK_SEC, nowSec];
            }
          },
          acc: { range: [0, 100] }
        },
        axes: [
          {
            stroke: "#706c5c",
            grid: { stroke: "rgba(104, 95, 72, 0.22)" },
            splits: (u, _axisIdx, _space, _incr) => {
              const min = (u.scales.x.min ?? 0) as number;
              const max = (u.scales.x.max ?? 0) as number;
              if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
                return [];
              }
              const start = Math.ceil(min / HALF_HOUR_SEC) * HALF_HOUR_SEC;
              const out: number[] = [];
              for (let t = start; t <= max; t += HALF_HOUR_SEC) {
                out.push(t);
              }
              return out;
            },
            values: (u, vals) => {
              const widthPx = Math.max(640, u.bbox.width);
              const maxLabels = Math.max(4, Math.floor(widthPx / 110));
              const stride = Math.max(1, Math.ceil(vals.length / maxLabels));
              let lastShownDay = "";
              return vals.map((v, idx) => {
                const isEdge = idx === 0 || idx + 1 === vals.length;
                if (!isEdge && idx % stride !== 0) {
                  return "";
                }
                const { mm, dd } = axisParts(axisTimeFormatter, v);
                const day = `${mm}/${dd}`;
                const includeDate = idx === 0 || day !== lastShownDay;
                lastShownDay = day;
                return formatAxisTs(axisTimeFormatter, v, includeDate);
              });
            }
          },
          {
            scale: "acc",
            stroke: "#efe349",
            grid: { stroke: "rgba(104, 95, 72, 0.22)" },
            values: (_u, vals) => vals.map((v) => `${Math.round(v)}%`)
          }
        ],
        series: [
          {},
          {
            label: "准确率",
            scale: "acc",
            stroke: "#efe349",
            width: 1.8,
            paths: uPlot.paths.stepped?.({ align: 1, alignGaps: 1, ascDesc: true }),
            points: { show: false }
          }
        ]
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
      current.setSize({ width: Math.max(640, host.clientWidth), height: 250 });
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      plot.destroy();
      plotRef.current = null;
    };
  }, [axisTimeFormatter]);

  useEffect(() => {
    plotRef.current?.setData(data);
  }, [data]);

  return <div ref={rootRef} className="chart-root" />;
}

export const AccuracyChart = memo(AccuracyChartImpl);
