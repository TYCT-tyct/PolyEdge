import { memo, useEffect, useMemo, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import type { AccuracyPoint } from "../types";

interface AccuracyChartProps {
  points: AccuracyPoint[];
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

function AccuracyChartImpl({ points }: AccuracyChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  const data = useMemo(() => toData(points), [points]);

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
          x: { time: true },
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
              const stepSec = 1800; // 30 minutes
              const start = Math.ceil(min / stepSec) * stepSec;
              const out: number[] = [];
              for (let t = start; t <= max; t += stepSec) {
                out.push(t);
              }
              return out;
            },
            values: (_u, vals) =>
              vals.map((v) =>
                new Date(v * 1000).toLocaleTimeString("zh-CN", {
                  hour12: false,
                  hour: "2-digit",
                  minute: "2-digit"
                })
              )
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
  }, []);

  useEffect(() => {
    plotRef.current?.setData(data);
  }, [data]);

  return <div ref={rootRef} className="chart-root" />;
}

export const AccuracyChart = memo(AccuracyChartImpl);
