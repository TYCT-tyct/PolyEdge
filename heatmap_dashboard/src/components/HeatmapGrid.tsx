import { memo, useMemo } from "react";

import type { HeatmapCell } from "../types";

interface HeatmapGridProps {
  cells: HeatmapCell[];
  marketType: "5m" | "15m";
}

function colorForValue(cents: number, opacity: number): string {
  const clamped = Math.max(0, Math.min(100, cents));
  const alpha = Math.max(0.1, Math.min(1, opacity));
  if (clamped < 50) {
    const t = clamped / 50;
    const r = 255;
    const g = Math.round(40 + t * 160);
    const b = Math.round(70 - t * 30);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }
  const t = (clamped - 50) / 50;
  const r = Math.round(250 - t * 140);
  const g = Math.round(200 + t * 35);
  const b = Math.round(55 - t * 15);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

const DELTA_FALLBACK_MIN_PCT = -0.45;
const DELTA_FALLBACK_MAX_PCT = 0.75;
const DELTA_HARD_MIN_PCT = -5.0;
const DELTA_HARD_MAX_PCT = 5.0;
const DELTA_STEP_PCT = 0.05;
const DELTA_MAJOR_STEP_PCT = 0.15;

function formatDeltaBin(v: number): string {
  return `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;
}

function roundToStep(v: number, step: number): number {
  return Number((Math.round(v / step) * step).toFixed(2));
}

function floorToStep(v: number, step: number): number {
  return Number((Math.floor(v / step) * step).toFixed(2));
}

function ceilToStep(v: number, step: number): number {
  return Number((Math.ceil(v / step) * step).toFixed(2));
}

function HeatmapGridImpl({ cells, marketType }: HeatmapGridProps) {
  const { deltaBins, timeBins, cellMap, maxCount, minBin } = useMemo(() => {
    const observed = cells
      .map((c) => c.delta_bin_pct)
      .filter((v) => Number.isFinite(v))
      .map((v) => clamp(v, DELTA_HARD_MIN_PCT, DELTA_HARD_MAX_PCT));
    const obsMin = observed.length > 0 ? Math.min(...observed) : DELTA_FALLBACK_MIN_PCT;
    const obsMax = observed.length > 0 ? Math.max(...observed) : DELTA_FALLBACK_MAX_PCT;
    const minWithPad = floorToStep(
      clamp(obsMin - DELTA_MAJOR_STEP_PCT, DELTA_HARD_MIN_PCT, DELTA_HARD_MAX_PCT),
      DELTA_STEP_PCT
    );
    const maxWithPad = ceilToStep(
      clamp(obsMax + DELTA_MAJOR_STEP_PCT, DELTA_HARD_MIN_PCT, DELTA_HARD_MAX_PCT),
      DELTA_STEP_PCT
    );
    const minBin = Math.min(minWithPad, 0);
    const maxBin = Math.max(maxWithPad, 0);

    const dList: number[] = [];
    for (let d = minBin; d <= maxBin + 1e-9; d += DELTA_STEP_PCT) {
      dList.push(Number(d.toFixed(2)));
    }

    const durationSec = marketType === "5m" ? 300 : 900;
    const tList: number[] = [];
    for (let t = durationSec - 30; t >= 0; t -= 30) {
      tList.push(t);
    }

    const map = new Map<string, HeatmapCell>();
    let max = 0;
    for (const c of cells) {
      const d = roundToStep(clamp(c.delta_bin_pct, minBin, maxBin), DELTA_STEP_PCT);
      const t = Math.max(0, Math.min(durationSec - 30, c.time_left_s_bin));
      map.set(`${t}|${d.toFixed(2)}`, c);
      if (c.sample_count > max) {
        max = c.sample_count;
      }
    }
    return { deltaBins: dList, timeBins: tList, cellMap: map, maxCount: max, minBin };
  }, [cells, marketType]);

  if (cells.length === 0) {
    return <div className="empty-panel">暂无热力图数据</div>;
  }

  const majorEvery = Math.round(DELTA_MAJOR_STEP_PCT / DELTA_STEP_PCT);

  return (
    <div className="heatmap-wrap">
      <div className="heatmap-canvas">
        <div className="heatmap-y-axis">
          <span>剩余时间</span>
          {timeBins.map((t) => (
            <label key={`y-${t}`}>{t >= 60 ? `${Math.floor(t / 60)}:${String(t % 60).padStart(2, "0")}` : `${t}s`}</label>
          ))}
        </div>
        <div className="heatmap-core-wrap">
          <div
            className="heatmap-core"
            style={{
              gridTemplateColumns: `repeat(${deltaBins.length}, minmax(44px, 1fr))`,
              gridTemplateRows: `repeat(${timeBins.length}, minmax(38px, auto))`
            }}
          >
            {timeBins.map((t, rowIdx) =>
              deltaBins.map((d, colIdx) => {
                const cell = cellMap.get(`${t}|${d.toFixed(2)}`);
                if (!cell) {
                  return (
                    <div
                      key={`empty-${t}-${d}`}
                      className="heatmap-cell ghost"
                      style={{ gridRow: rowIdx + 1, gridColumn: colIdx + 1 }}
                    />
                  );
                }
                const density = maxCount > 0 ? cell.sample_count / maxCount : 0;
                return (
                  <div
                    key={`${t}-${d}`}
                    className="heatmap-cell block"
                    style={{
                      gridRow: rowIdx + 1,
                      gridColumn: colIdx + 1,
                      backgroundColor: colorForValue(cell.avg_up_price_cents, Math.max(cell.opacity, 0.25)),
                      opacity: 0.35 + density * 0.65
                    }}
                    title={`Δ ${d.toFixed(2)}% | ${t}s | UP ${cell.avg_up_price_cents.toFixed(1)}¢ | n=${cell.sample_count}`}
                  >
                    {Math.round(cell.avg_up_price_cents)}
                  </div>
                );
              })
            )}
          </div>
          <div
            className="heatmap-x-axis"
            style={{ gridTemplateColumns: `repeat(${deltaBins.length}, minmax(44px, 1fr))` }}
          >
            {deltaBins.map((d) => (
              <span
                key={`x-${d}`}
                className={Math.round((d - minBin) / DELTA_STEP_PCT) % majorEvery === 0 ? "major" : "minor"}
              >
                {Math.round((d - minBin) / DELTA_STEP_PCT) % majorEvery === 0
                  ? formatDeltaBin(d)
                  : ""}
              </span>
            ))}
          </div>
          <div className="heatmap-axis-title">Δ价差%（BTC 相对 Target）</div>
        </div>
      </div>
      <div className="heatmap-legend">
        <span className="down">DOWN 0¢</span>
        <div className="legend-gradient" />
        <span className="up">UP 100¢</span>
        <span className="muted">不透明度 = 样本密度</span>
      </div>
    </div>
  );
}

export const HeatmapGrid = memo(HeatmapGridImpl);
