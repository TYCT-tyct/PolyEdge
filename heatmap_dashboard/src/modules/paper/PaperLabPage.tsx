import { useEffect, useMemo, useRef, useState } from "react";

import {
  getStrategyAutotuneHistory,
  getStrategyAutotuneLatest,
  getStrategyPaper
} from "../../api";
import type { MarketSymbol, MarketType, StrategyPaperResponse } from "../../types";

type TimeMode = "local" | "et";
type StrategyPaperSource = "replay" | "live";
const PAPER_AUTOTUNE_DISABLED = true;

const ET_TIMEZONE = "America/New_York";
const STRATEGY_POLL_MIN_MS = 3_500;
const STRATEGY_POLL_MAX_MS = 20_000;
const AUTOTUNE_POLL_MIN_MS = 6_000;
const AUTOTUNE_POLL_MAX_MS = 60_000;
const STRATEGY_PREFS_STORAGE_KEY = "polyedge.strategy.prefs.v4";

const STRATEGY_PAPER_PROFILE = Object.freeze({
  fullHistory: false,
  useAutotune: false
});

const STRATEGY_LIVE_PROFILE = Object.freeze({
  liveExecute: false,
  liveQuoteUsdc: 1,
  liveMaxOrders: 1,
  liveEntryOnly: true
});

type StrategyUiPrefs = {
  marketType: MarketType;
  source: StrategyPaperSource;
  useAutotune: boolean;
};

function readStrategyUiPrefs(): Partial<StrategyUiPrefs> {
  if (typeof window === "undefined" || !window.localStorage) {
    return {};
  }
  try {
    const raw = window.localStorage.getItem(STRATEGY_PREFS_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const marketType =
      parsed.marketType === "5m" || parsed.marketType === "15m"
        ? (parsed.marketType as MarketType)
        : undefined;
    const source =
      parsed.source === "replay" || parsed.source === "live"
        ? (parsed.source as StrategyPaperSource)
        : undefined;
    const useAutotune =
      typeof parsed.useAutotune === "boolean" ? parsed.useAutotune : undefined;
    return {
      marketType,
      source,
      useAutotune
    };
  } catch {
    return {};
  }
}

function writeStrategyUiPrefs(prefs: StrategyUiPrefs): void {
  if (typeof window === "undefined" || !window.localStorage) {
    return;
  }
  try {
    window.localStorage.setItem(STRATEGY_PREFS_STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    // ignore localStorage failures
  }
}

function asFiniteNumber(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function formatTime(ts: number | null | undefined, timeMode: TimeMode): string {
  if (ts == null || !Number.isFinite(ts) || ts <= 0) {
    return "--";
  }
  const opts: Intl.DateTimeFormatOptions = {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  };
  if (timeMode === "et") {
    return new Intl.DateTimeFormat("en-US", {
      ...opts,
      timeZone: ET_TIMEZONE
    }).format(new Date(ts));
  }
  return new Intl.DateTimeFormat("zh-CN", opts).format(new Date(ts));
}

function formatClockTime(ts: number | null | undefined, timeMode: TimeMode): string {
  if (ts == null || !Number.isFinite(ts) || ts <= 0) {
    return "--";
  }
  const opts: Intl.DateTimeFormatOptions = {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  };
  if (timeMode === "et") {
    return new Intl.DateTimeFormat("en-US", {
      ...opts,
      timeZone: ET_TIMEZONE
    }).format(new Date(ts));
  }
  return new Intl.DateTimeFormat("zh-CN", opts).format(new Date(ts));
}

function shortRoundId(roundId: string | null | undefined): string {
  if (!roundId) {
    return "--";
  }
  return roundId.length > 22 ? `${roundId.slice(0, 22)}...` : roundId;
}

function strategyPaperSignature(payload: StrategyPaperResponse): string {
  const summary = payload.summary;
  const current = payload.current;
  const last = payload.trades[payload.trades.length - 1];
  return [
    payload.source ?? "",
    payload.market_type,
    payload.samples,
    summary.trade_count,
    summary.win_rate_pct.toFixed(4),
    summary.total_pnl_cents.toFixed(4),
    summary.net_pnl_cents.toFixed(4),
    current?.round_id ?? "",
    current?.timestamp_ms ?? 0,
    current?.suggested_action ?? "",
    last?.id ?? -1,
    last?.exit_ts_ms ?? 0,
    payload.autotune_active_key ?? "",
    payload.autotune_live_key ?? ""
  ].join("|");
}

function autotuneSignature(
  latest: Record<string, unknown> | null,
  history: Array<Record<string, unknown>>
): string {
  const head = history[0] ?? null;
  const tail = history[history.length - 1] ?? null;
  const ts = (row: Record<string, unknown> | null): number =>
    asFiniteNumber(row?.saved_at_ms) ??
    asFiniteNumber(row?.updated_at_ms) ??
    asFiniteNumber(row?.created_at_ms) ??
    0;
  return [
    ts(latest),
    typeof latest?.source === "string" ? latest.source : "",
    history.length,
    ts(head),
    ts(tail)
  ].join("|");
}

function textCell(v: unknown): string {
  return typeof v === "string" && v.trim().length > 0 ? v.trim() : "--";
}

function numCell(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

export function PaperLabPage({
  selectedSymbol,
  timeMode
}: {
  selectedSymbol: MarketSymbol;
  timeMode: TimeMode;
}) {
  const prefs = useMemo(() => readStrategyUiPrefs(), []);
  const [strategyPaper, setStrategyPaper] = useState<StrategyPaperResponse | null>(null);
  const [strategyLoading, setStrategyLoading] = useState<boolean>(false);
  const [strategyMarketType, setStrategyMarketType] = useState<MarketType>(
    prefs.marketType ?? "5m"
  );
  const [strategySource, setStrategySource] = useState<StrategyPaperSource>(
    prefs.source ?? "replay"
  );
  const [strategyUseAutotune] = useState<boolean>(false);
  const [strategyAutotuneLatest, setStrategyAutotuneLatest] = useState<Record<string, unknown> | null>(null);
  const [strategyAutotuneHistory, setStrategyAutotuneHistory] = useState<Array<Record<string, unknown>>>([]);
  const [strategyAutotuneLoading, setStrategyAutotuneLoading] = useState<boolean>(false);
  const [errorText, setErrorText] = useState<string>("");

  const strategyInFlightRef = useRef<boolean>(false);
  const strategySigRef = useRef<string>("");
  const strategyUnchangedRef = useRef<number>(0);
  const strategyAutotuneInFlightRef = useRef<boolean>(false);
  const strategyAutotuneSigRef = useRef<string>("");
  const strategyAutotuneUnchangedRef = useRef<number>(0);
  const strategyHasLoadedRef = useRef<boolean>(false);
  const strategyAutotuneHasLoadedRef = useRef<boolean>(false);

  const strategyEnabled = selectedSymbol === "BTCUSDT";

  useEffect(() => {
    writeStrategyUiPrefs({
      marketType: strategyMarketType,
      source: strategySource,
      useAutotune: strategyUseAutotune
    });
  }, [strategyMarketType, strategySource, strategyUseAutotune]);

  useEffect(() => {
    let alive = true;
    if (!strategyEnabled) {
      setStrategyPaper(null);
      setStrategyLoading(false);
      strategyHasLoadedRef.current = false;
      return () => {
        alive = false;
      };
    }
    let timer: number | null = null;

    const nextDelayMs = (changed: boolean): number => {
      if (document.visibilityState !== "visible") {
        return STRATEGY_POLL_MAX_MS;
      }
      if (changed) {
        strategyUnchangedRef.current = 0;
        return STRATEGY_POLL_MIN_MS;
      }
      strategyUnchangedRef.current += 1;
      return Math.min(
        STRATEGY_POLL_MAX_MS,
        STRATEGY_POLL_MIN_MS + strategyUnchangedRef.current * 2_000
      );
    };

    const schedule = (delay: number) => {
      if (!alive) {
        return;
      }
      timer = window.setTimeout(() => {
        void loop();
      }, delay);
    };

    const loop = async () => {
      if (!alive) {
        return;
      }
      if (document.visibilityState !== "visible") {
        schedule(STRATEGY_POLL_MAX_MS);
        return;
      }
      if (strategyInFlightRef.current) {
        schedule(1_000);
        return;
      }
      strategyInFlightRef.current = true;
      if (!strategyHasLoadedRef.current) {
        setStrategyLoading(true);
      }
      let changed = false;
      try {
        const data = await getStrategyPaper(strategyMarketType, {
          ...STRATEGY_PAPER_PROFILE,
          useAutotune: strategyUseAutotune,
          source: strategySource === "live" ? "auto" : "replay",
          ...(strategySource === "live" ? STRATEGY_LIVE_PROFILE : {})
        });
        if (alive) {
          const nextSig = strategyPaperSignature(data);
          changed = nextSig !== strategySigRef.current;
          if (changed) {
            strategySigRef.current = nextSig;
            setStrategyPaper(data);
          }
          strategyHasLoadedRef.current = true;
          setErrorText("");
        }
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      } finally {
        strategyInFlightRef.current = false;
        if (alive) {
          setStrategyLoading(false);
          schedule(nextDelayMs(changed));
        }
      }
    };

    void loop();
    return () => {
      alive = false;
      if (timer != null) {
        window.clearTimeout(timer);
      }
    };
  }, [strategyEnabled, strategyMarketType, strategySource, strategyUseAutotune]);

  useEffect(() => {
    let alive = true;
    if (!strategyEnabled || !strategyUseAutotune) {
      setStrategyAutotuneLatest(null);
      setStrategyAutotuneHistory([]);
      setStrategyAutotuneLoading(false);
      strategyAutotuneHasLoadedRef.current = false;
      return () => {
        alive = false;
      };
    }

    let timer: number | null = null;
    const nextDelayMs = (changed: boolean): number => {
      if (document.visibilityState !== "visible") {
        return AUTOTUNE_POLL_MAX_MS;
      }
      if (changed) {
        strategyAutotuneUnchangedRef.current = 0;
        return AUTOTUNE_POLL_MIN_MS;
      }
      strategyAutotuneUnchangedRef.current += 1;
      return Math.min(
        AUTOTUNE_POLL_MAX_MS,
        AUTOTUNE_POLL_MIN_MS + strategyAutotuneUnchangedRef.current * 6_000
      );
    };

    const schedule = (delay: number) => {
      if (!alive) {
        return;
      }
      timer = window.setTimeout(() => {
        void loop();
      }, delay);
    };

    const loop = async () => {
      if (!alive) {
        return;
      }
      if (document.visibilityState !== "visible") {
        schedule(AUTOTUNE_POLL_MAX_MS);
        return;
      }
      if (strategyAutotuneInFlightRef.current) {
        schedule(1_500);
        return;
      }
      strategyAutotuneInFlightRef.current = true;
      if (!strategyAutotuneHasLoadedRef.current) {
        setStrategyAutotuneLoading(true);
      }
      let changed = false;
      try {
        const [latest, history] = await Promise.all([
          getStrategyAutotuneLatest(strategyMarketType),
          getStrategyAutotuneHistory(strategyMarketType, 20)
        ]);
        if (!alive) {
          return;
        }
        const latestData = latest.active_data ?? latest.data ?? null;
        const historyItems = history.items ?? [];
        const nextSig = autotuneSignature(latestData, historyItems);
        changed = nextSig !== strategyAutotuneSigRef.current;
        if (changed) {
          strategyAutotuneSigRef.current = nextSig;
          setStrategyAutotuneLatest(latestData);
          setStrategyAutotuneHistory(historyItems);
        }
        strategyAutotuneHasLoadedRef.current = true;
      } catch (err) {
        if (alive) {
          setErrorText(err instanceof Error ? err.message : String(err));
        }
      } finally {
        strategyAutotuneInFlightRef.current = false;
        if (alive) {
          setStrategyAutotuneLoading(false);
          schedule(nextDelayMs(changed));
        }
      }
    };

    void loop();
    return () => {
      alive = false;
      if (timer != null) {
        window.clearTimeout(timer);
      }
    };
  }, [strategyEnabled, strategyMarketType, strategyUseAutotune]);

  if (!strategyEnabled) {
    return (
      <section className="panel">
        <header className="panel-head">
          <div>
            <h2>Paper 模块</h2>
            <p className="muted">
              当前仅 BTCUSDT 开启 Paper/Live 策略计算。其他币种仍保持采集展示，不执行策略。
            </p>
          </div>
        </header>
      </section>
    );
  }

  const summary = strategyPaper?.summary;
  const current = strategyPaper?.current;
  const finite = (v: number | null | undefined): number | null =>
    typeof v === "number" && Number.isFinite(v) ? v : null;
  const currentScore = finite(current?.score);
  const currentThreshold = finite(current?.entry_threshold);
  const currentConfidencePct = finite(current?.confidence) != null ? (current!.confidence * 100) : null;
  const currentPUpPct = finite(current?.p_up_pct) != null ? (current!.p_up_pct * 100) : null;
  const currentDeltaPct = finite(current?.delta_pct) != null ? (current!.delta_pct * 100) : null;
  const currentRemainingS = finite(current?.remaining_s);
  const currentSpreadUp = finite(current?.spread_up_cents);
  const currentSpreadDown = finite(current?.spread_down_cents);
  const summaryNet = summary ? (finite(summary.net_pnl_cents) ?? finite(summary.total_pnl_cents) ?? 0) : null;
  const liveWarmupFallback =
    strategySource === "live" &&
    strategyPaper?.source === "replay" &&
    typeof strategyPaper?.source_fallback_error === "string" &&
    strategyPaper.source_fallback_error.length > 0;

  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>策略 Paper（{strategyMarketType}全时段）</h2>
          <p className="muted">单模型双向策略：自动判断 UP/DOWN，只验证入场与出场，不做加仓和反向。</p>
        </div>
        <div className="panel-actions">
          <span className="loading-chip">{strategyLoading ? "计算中..." : strategySource === "live" ? "实时策略" : "模拟策略"}</span>
        </div>
      </header>

      <div className="info-cards compact strategy-matrix">
        <article className="info-card">
          <span>当前动作</span>
          <strong className={current?.suggested_action?.includes("UP") ? "up" : current?.suggested_action?.includes("DOWN") ? "down" : ""}>
            {current?.suggested_action ?? "--"}
          </strong>
          <small>
            {current
              ? `${formatTime(current.timestamp_ms, timeMode)} · ${current.round_id ?? "--"}`
              : "等待数据"}
          </small>
        </article>
        <article className="info-card">
          <span>信号强度 / 阈值</span>
          <strong>
            {currentScore != null && currentThreshold != null
              ? `${currentScore.toFixed(3)} / ${currentThreshold.toFixed(3)}`
              : "--"}
          </strong>
          <small>置信度：{currentConfidencePct != null ? `${currentConfidencePct.toFixed(1)}%` : "--"}</small>
        </article>
        <article className="info-card">
          <span>累计净收益</span>
          <strong className={(summaryNet ?? 0) >= 0 ? "up" : "down"}>
            {summaryNet != null ? `${summaryNet.toFixed(2)}¢` : "--"}
          </strong>
          <small>
            交易 {summary?.trade_count ?? 0} · 胜率 {summary ? `${summary.win_rate_pct.toFixed(1)}%` : "--"} · 窗口 {strategyPaper?.lookback_minutes ?? "--"}m
          </small>
        </article>
        <article className="info-card">
          <span>均值 / 回撤</span>
          <strong>
            {summary
              ? `${summary.avg_pnl_cents.toFixed(2)}¢ / ${summary.max_drawdown_cents.toFixed(2)}¢`
              : "--"}
          </strong>
          <small>平均每笔 / 最大回撤</small>
        </article>
        <article className="info-card">
          <span>毛收益 / 总成本</span>
          <strong>
            {summary
              ? `${summary.gross_pnl_cents.toFixed(2)}¢ / ${summary.total_cost_cents.toFixed(2)}¢`
              : "--"}
          </strong>
          <small>净利润率 {summary ? `${summary.net_margin_pct.toFixed(2)}%` : "--"}</small>
        </article>
        <article className="info-card">
          <span>市场状态</span>
          <strong>
            {currentPUpPct != null ? `UP ${currentPUpPct.toFixed(1)}%` : "--"}
          </strong>
          <small>
            Δ {currentDeltaPct != null ? `${currentDeltaPct.toFixed(4)}%` : "--"} · 剩余 {currentRemainingS != null ? `${currentRemainingS.toFixed(1)}s` : "--"} · 样本 {strategyPaper?.samples ?? 0}
          </small>
        </article>
      </div>

      <div className="paper-toolbar">
        <div className="btn-group">
          <button className={strategyMarketType === "5m" ? "active" : ""} onClick={() => setStrategyMarketType("5m")}>
            5m
          </button>
          <button className={strategyMarketType === "15m" ? "active" : ""} onClick={() => setStrategyMarketType("15m")}>
            15m
          </button>
        </div>
        <div className="btn-group">
          <button className={strategySource === "replay" ? "active" : ""} onClick={() => setStrategySource("replay")}>
            Paper模拟
          </button>
          <button className={strategySource === "live" ? "active" : ""} onClick={() => setStrategySource("live")}>
            真实交易
          </button>
        </div>
        <div className="btn-group">
          <button className={strategyUseAutotune ? "active" : ""} disabled>
            AutoTune 开
          </button>
          <button className={!strategyUseAutotune ? "active" : ""} disabled>
            AutoTune 关
          </button>
        </div>
        <article className="info-card paper-quote-card">
          <span>盘口状态</span>
          <strong>
            {currentSpreadUp != null && currentSpreadDown != null
              ? `${currentSpreadUp.toFixed(2)}¢ / ${currentSpreadDown.toFixed(2)}¢`
              : "--"}
          </strong>
          <small>UP / DOWN 点差</small>
        </article>
      </div>

      {!PAPER_AUTOTUNE_DISABLED ? (
        <div className="table-wrap">
          <h3 className="table-title">AutoTune 历史（最近 8 条）</h3>
          <table className="history-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>来源</th>
                <th>entry_base</th>
                <th>entry_cap</th>
                <th>止损</th>
                <th>备注</th>
              </tr>
            </thead>
            <tbody>
              {strategyAutotuneHistory.slice(0, 8).map((row, idx) => {
                const cfg =
                  row.config && typeof row.config === "object"
                    ? (row.config as Record<string, unknown>)
                    : null;
                const ts =
                  numCell(row.saved_at_ms) ??
                  numCell(row.updated_at_ms) ??
                  numCell(row.created_at_ms);
                return (
                  <tr key={`auto-${idx}-${ts ?? 0}`}>
                    <td>{formatTime(ts, timeMode)}</td>
                    <td>{textCell(row.source)}</td>
                    <td>{numCell(cfg?.entry_threshold_base)?.toFixed(3) ?? "--"}</td>
                    <td>{numCell(cfg?.entry_threshold_cap)?.toFixed(3) ?? "--"}</td>
                    <td>{numCell(cfg?.stop_loss_cents)?.toFixed(2) ?? "--"}</td>
                    <td>{textCell(row.note)}</td>
                  </tr>
                );
              })}
              {strategyAutotuneHistory.length === 0 ? (
                <tr>
                  <td colSpan={6}>{strategyAutotuneLoading ? "加载中..." : "暂无历史"}</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      ) : null}

      <div className="table-wrap">
        <h3 className="table-title">交易记录（最近 20 条）</h3>
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
            {(strategyPaper?.trades ?? []).slice(-20).reverse().map((t) => (
              <tr key={`trade-${t.id}`}>
                <td>
                  <span className={`chip ${t.side === "UP" ? "up" : "down"}`}>{t.side}</span>
                </td>
                <td>{shortRoundId(t.entry_round_id)}</td>
                <td>{formatClockTime(t.entry_ts_ms, timeMode)}</td>
                <td>{formatClockTime(t.exit_ts_ms, timeMode)}</td>
                <td>
                  <div>{`${t.entry_price_raw_cents.toFixed(2)} → ${t.exit_price_raw_cents.toFixed(2)}`}</div>
                  <small className="muted">{`exec ${t.entry_price_cents.toFixed(2)} → ${t.exit_price_cents.toFixed(2)}`}</small>
                </td>
                <td className={(t.pnl_net_cents ?? t.pnl_cents) >= 0 ? "up" : "down"}>
                  {(t.pnl_net_cents ?? t.pnl_cents).toFixed(2)}¢
                </td>
                <td>
                  <div>{`${t.total_cost_cents.toFixed(2)}¢`}</div>
                  <small className="muted">
                    {`fee ${(t.entry_fee_cents + t.exit_fee_cents).toFixed(2)} · slip ${(t.entry_slippage_cents + t.exit_slippage_cents + (t.exit_emergency_penalty_cents ?? 0)).toFixed(2)}`}
                  </small>
                </td>
                <td>{`${t.duration_s.toFixed(1)}s`}</td>
                <td>{`${t.entry_reason} / ${t.exit_reason}`}</td>
              </tr>
            ))}
            {(strategyPaper?.trades.length ?? 0) === 0 ? (
              <tr>
                <td colSpan={9}>暂无交易样本</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      <footer className="status-row">
        <span>source: {strategyPaper?.source ?? "--"}</span>
        <span>market: {strategyMarketType}</span>
        <span>
          runtime:
          {` ${
            strategyPaper?.runtime_defaults?.lookback_minutes ?? "--"
          }m / ${strategyPaper?.runtime_defaults?.max_trades ?? "--"} trades`}
        </span>
        <span>
          liveCtrl: {strategyPaper?.runtime_control?.mode ?? "normal"}
          {strategyPaper?.runtime_control?.effective_drain_only ? " (drain)" : ""}
        </span>
        <span>activeKey: {strategyPaper?.autotune_active_key ?? "--"}</span>
        <span>liveKey: {strategyPaper?.autotune_live_key ?? "--"}</span>
        <span>autotune: forced_off</span>
        {liveWarmupFallback ? (
          <span className="muted">live 预热中：已自动回退到 replay 展示</span>
        ) : null}
        {errorText ? <span className="down">{errorText}</span> : null}
      </footer>
    </section>
  );
}
