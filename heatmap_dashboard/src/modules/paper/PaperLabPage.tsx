import { useEffect, useMemo, useRef, useState } from "react";

import {
  getStrategyPaper
} from "../../api";
import type { MarketSymbol, MarketType, StrategyPaperResponse } from "../../types";

type TimeMode = "local" | "et";
type StrategyPaperSource = "replay" | "live";

const ET_TIMEZONE = "America/New_York";
const CN_TIMEZONE = "Asia/Shanghai";
const STRATEGY_POLL_MIN_MS = 3_500;
const STRATEGY_POLL_MAX_MS = 20_000;
const STRATEGY_PREFS_STORAGE_KEY = "polyedge.strategy.prefs.v4";
const PAPER_LOOKBACK_MINUTES = 1440;

const STRATEGY_PAPER_PROFILE = Object.freeze({
  fullHistory: false
});

const STRATEGY_LIVE_PROFILE = Object.freeze({
  liveExecute: false,
  liveQuoteUsdc: 1,
  liveMaxOrders: 1,
  liveEntryOnly: true
});
const STRATEGY_ENABLED_SYMBOLS: MarketSymbol[] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"];

type StrategyUiPrefs = {
  marketType: MarketType;
  source: StrategyPaperSource;
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
    return {
      marketType,
      source
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
  return new Intl.DateTimeFormat("zh-CN", {
    ...opts,
    timeZone: CN_TIMEZONE
  }).format(new Date(ts));
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
  return new Intl.DateTimeFormat("zh-CN", {
    ...opts,
    timeZone: CN_TIMEZONE
  }).format(new Date(ts));
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
    payload.symbol ?? "",
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
    last?.exit_ts_ms ?? 0
  ].join("|");
}

function textCell(v: unknown): string {
  return typeof v === "string" && v.trim().length > 0 ? v.trim() : "--";
}

function numCell(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function compactStrategyPayload(payload: StrategyPaperResponse): StrategyPaperResponse {
  const trimmedTrades = payload.trades.length > 60 ? payload.trades.slice(-60) : payload.trades;
  const live = payload.live_execution;
  if (!live) {
    return { ...payload, trades: trimmedTrades };
  }
  const compactLive = {
    ...live,
    decisions:
      live.decisions && live.decisions.length > 80
        ? live.decisions.slice(live.decisions.length - 80)
        : live.decisions,
    paper_records:
      live.paper_records && live.paper_records.length > 60
        ? live.paper_records.slice(live.paper_records.length - 60)
        : live.paper_records,
    live_records:
      live.live_records && live.live_records.length > 60
        ? live.live_records.slice(live.live_records.length - 60)
        : live.live_records,
    events:
      live.events && live.events.length > 50
        ? live.events.slice(live.events.length - 50)
        : live.events,
    gated: live.gated
      ? {
          ...live.gated,
          submitted_decisions:
            live.gated.submitted_decisions &&
            live.gated.submitted_decisions.length > 60
              ? live.gated.submitted_decisions.slice(
                  live.gated.submitted_decisions.length - 60
                )
              : live.gated.submitted_decisions,
          skipped_decisions:
            live.gated.skipped_decisions &&
            live.gated.skipped_decisions.length > 60
              ? live.gated.skipped_decisions.slice(
                  live.gated.skipped_decisions.length - 60
                )
              : live.gated.skipped_decisions
        }
      : live.gated
  };
  return { ...payload, trades: trimmedTrades, live_execution: compactLive };
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
  const [errorText, setErrorText] = useState<string>("");

  const strategyInFlightRef = useRef<boolean>(false);
  const strategySigRef = useRef<string>("");
  const strategyUnchangedRef = useRef<number>(0);
  const strategyHasLoadedRef = useRef<boolean>(false);

  const strategyEnabled = STRATEGY_ENABLED_SYMBOLS.includes(selectedSymbol);

  useEffect(() => {
    writeStrategyUiPrefs({
      marketType: strategyMarketType,
      source: strategySource
    });
  }, [strategyMarketType, strategySource]);

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
          lookbackMinutes: PAPER_LOOKBACK_MINUTES,
          source: strategySource,
          ...(strategySource === "live" ? STRATEGY_LIVE_PROFILE : {})
        }, selectedSymbol);
        const compactData = compactStrategyPayload(data);
        if (alive) {
          const nextSig = strategyPaperSignature(compactData);
          changed = nextSig !== strategySigRef.current;
          if (changed) {
            strategySigRef.current = nextSig;
            setStrategyPaper(compactData);
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
  }, [strategyEnabled, selectedSymbol, strategyMarketType, strategySource]);

  if (!strategyEnabled) {
    return (
      <section className="panel">
        <header className="panel-head">
          <div>
            <h2>Paper 模块</h2>
            <p className="muted">
              {selectedSymbol} 暂未开启 Paper/Live 策略计算，请检查后端 FORGE_STRATEGY_SYMBOLS 配置。
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
  const lastTrade = strategyPaper?.trades?.length
    ? strategyPaper.trades[strategyPaper.trades.length - 1]
    : null;
  const lastTradeTs =
    finite(lastTrade?.exit_ts_ms) ??
    finite(lastTrade?.entry_ts_ms);
  const nowTs = finite(current?.timestamp_ms) ?? Date.now();
  const idleMinutes =
    lastTradeTs != null ? Math.max(0, (nowTs - lastTradeTs) / 60_000) : null;
  const liveExecution = strategyPaper?.live_execution;
  const liveParity = liveExecution?.parity_check;
  const liveState = liveExecution?.state_machine;
  const runtimeControl = strategyPaper?.runtime_control;
  const executionAggr = strategyPaper?.execution_aggressiveness;
  const executionTarget = liveExecution?.execution_target;
  const liveEvents = (liveExecution?.events ?? []).slice(-20).reverse();
  const liveOrders = (liveExecution?.execution?.orders ?? []).slice(-20).reverse();
  const liveOrderLatencies = liveOrders
    .map((row) => numCell(row.order_latency_ms))
    .filter((v): v is number => v != null && Number.isFinite(v) && v >= 0);
  const liveAvgLatencyMs =
    liveOrderLatencies.length > 0
      ? liveOrderLatencies.reduce((sum, v) => sum + v, 0) / liveOrderLatencies.length
      : null;
  const liveMaxLatencyMs =
    liveOrderLatencies.length > 0 ? Math.max(...liveOrderLatencies) : null;
  const liveAttemptCounts = liveOrders
    .map((row) =>
      Array.isArray(row.attempts)
        ? row.attempts.length
        : null
    )
    .filter((v): v is number => v != null && v > 0);
  const liveAvgAttempts =
    liveAttemptCounts.length > 0
      ? liveAttemptCounts.reduce((sum, v) => sum + v, 0) / liveAttemptCounts.length
      : null;
  const lastRejectReason =
    liveEvents.find((row) => row.accepted === false)?.reason ??
    liveOrders.find((row) => row.accepted === false)?.reject_reason ??
    liveOrders.find((row) => row.accepted === false)?.error ??
    executionAggr?.last_error;
  const topSkipReason = (() => {
    const skipped = liveExecution?.gated?.skipped_decisions;
    if (!skipped || skipped.length === 0) {
      return null;
    }
    const freq = new Map<string, number>();
    for (const row of skipped) {
      const reason = textCell(row.reason);
      if (reason === "--") {
        continue;
      }
      freq.set(reason, (freq.get(reason) ?? 0) + 1);
    }
    if (freq.size === 0) {
      return null;
    }
    return [...freq.entries()].sort((a, b) => b[1] - a[1])[0][0];
  })();
  const lookbackMeta = strategyPaper?.lookback;
  const lookbackCoverageMinutes = finite(lookbackMeta?.coverage_minutes_by_samples);
  const lookbackRequestedMinutes =
    finite(lookbackMeta?.requested_lookback_minutes) ?? finite(strategyPaper?.lookback_minutes);
  const lookbackCoverageLabel =
    lookbackCoverageMinutes != null && lookbackRequestedMinutes != null
      ? `${lookbackCoverageMinutes.toFixed(0)}/${lookbackRequestedMinutes.toFixed(0)}m`
      : "--";
  const lookbackTruncated = lookbackMeta?.truncated_by_points === true;
  const profileSource = strategyPaper?.config_source ?? "--";
  const effectiveProfile =
    !strategyPaper
      ? "--"
      : profileSource === "default"
      ? strategyPaper.baseline_profile ?? "default"
      : profileSource;

  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>策略 Paper（{selectedSymbol} · {strategyMarketType}全时段）</h2>
          <p className="muted">单模型双向策略：自动判断 UP/DOWN，具体入场/加仓/退出行为以后端策略参数为准。</p>
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
            交易 {summary?.trade_count ?? 0} · 胜率 {summary ? `${summary.win_rate_pct.toFixed(1)}%` : "--"} · 窗口{" "}
            {strategyPaper?.lookback_minutes ?? "--"}m · 覆盖 {lookbackCoverageLabel}
            {lookbackTruncated ? " (截断)" : ""}
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
          <button
            className={strategySource === "replay" ? "active" : ""}
            onClick={() => setStrategySource("replay")}
          >
            Paper模拟
          </button>
          <button
            className={strategySource === "live" ? "active" : ""}
            onClick={() => setStrategySource("live")}
          >
            真实交易
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

      {strategySource === "live" ? (
        <section className="live-execution-wrap">
          <div className="info-cards compact live-kpi-grid">
            <article className="info-card">
              <span>执行状态</span>
              <strong
                className={
                  liveParity?.status === "ok"
                    ? "up"
                    : liveParity?.status === "dry_run"
                    ? ""
                    : "down"
                }
              >
                {textCell(liveParity?.status)}
              </strong>
              <small>level: {textCell(liveParity?.level)}</small>
            </article>
            <article className="info-card">
              <span>下单统计</span>
              <strong>
                {`${numCell(liveParity?.live?.submitted_count) ?? 0} / ${
                  numCell(liveParity?.live?.accepted_count) ?? 0
                }`}
              </strong>
              <small>
                提交 / 接受（拒绝 {numCell(liveParity?.live?.rejected_count) ?? 0}）
              </small>
            </article>
            <article className="info-card">
              <span>执行延迟</span>
              <strong>
                {liveAvgLatencyMs != null ? `${liveAvgLatencyMs.toFixed(0)}ms` : "--"}
              </strong>
              <small>
                max {liveMaxLatencyMs != null ? `${liveMaxLatencyMs.toFixed(0)}ms` : "--"} · avg
                attempts {liveAvgAttempts != null ? liveAvgAttempts.toFixed(2) : "--"}
              </small>
            </article>
            <article className="info-card">
              <span>门禁阻断</span>
              <strong>{numCell(liveExecution?.gated?.skipped_count) ?? 0}</strong>
              <small>top reason: {textCell(topSkipReason)}</small>
            </article>
            <article className="info-card">
              <span>控制模式</span>
              <strong>{textCell(runtimeControl?.mode)}</strong>
              <small>
                execute={runtimeControl?.effective_live_execute ? "on" : "off"} · drain=
                {runtimeControl?.effective_drain_only ? "on" : "off"}
              </small>
            </article>
          </div>

          <div className="info-cards compact live-matrix">
            <article className="info-card">
              <span>实盘安全臂</span>
              <strong>{runtimeControl?.live_submit_allowed ? "ARMED" : "SAFE"}</strong>
              <small>
                required={runtimeControl?.live_arm_required ? "yes" : "no"} · armed=
                {runtimeControl?.live_armed ? "yes" : "no"} · hard_kill=
                {runtimeControl?.live_hard_kill ? "yes" : "no"}
              </small>
            </article>
            <article className="info-card">
              <span>目标市场绑定</span>
              <strong>{textCell(executionTarget?.market_id)}</strong>
              <small>
                {textCell(executionTarget?.symbol)} / {textCell(executionTarget?.timeframe)} · end{" "}
                {textCell(executionTarget?.end_date)}
              </small>
            </article>
            <article className="info-card">
              <span>执行自适应</span>
              <strong>
                entry x{numCell(executionAggr?.entry_slippage_mult)?.toFixed(2) ?? "--"} · exit x
                {numCell(executionAggr?.exit_slippage_mult)?.toFixed(2) ?? "--"}
              </strong>
              <small>
                reject_ema {numCell(executionAggr?.reject_ema)?.toFixed(3) ?? "--"} · latency_ema{" "}
                {numCell(executionAggr?.latency_ema_ms)?.toFixed(0) ?? "--"}ms
              </small>
            </article>
          </div>

          <div className="live-block-grid">
            <article className="live-block">
              <header>
                <h3>状态机</h3>
                <small>持仓与在途状态</small>
              </header>
              <ul className="diag-list">
                <li>
                  <span>state / side</span>
                  <strong>
                    {textCell(liveState?.state)} / {textCell(liveState?.side)}
                  </strong>
                </li>
                <li>
                  <span>entry round</span>
                  <strong>{textCell(liveState?.entry_round_id)}</strong>
                </li>
                <li>
                  <span>entry market/token</span>
                  <strong>
                    {textCell(liveState?.entry_market_id)} / {textCell(liveState?.entry_token_id)}
                  </strong>
                </li>
                <li>
                  <span>size / vwap</span>
                  <strong>
                    {numCell(liveState?.position_size_shares)?.toFixed(4) ?? "--"} /{" "}
                    {numCell(liveState?.vwap_entry_cents)?.toFixed(2) ?? "--"}¢
                  </strong>
                </li>
                <li>
                  <span>net / realized</span>
                  <strong>
                    {numCell(liveState?.net_quote_usdc)?.toFixed(4) ?? "--"} /{" "}
                    {numCell(liveState?.realized_pnl_usdc)?.toFixed(4) ?? "--"} USDC
                  </strong>
                </li>
                <li>
                  <span>last action</span>
                  <strong>{textCell(liveState?.last_action)}</strong>
                </li>
                <li>
                  <span>updated</span>
                  <strong>{formatClockTime(numCell(liveState?.updated_ts_ms), timeMode)}</strong>
                </li>
              </ul>
            </article>
            <article className="live-block">
              <header>
                <h3>链路告警</h3>
                <small>阻断、错误、节奏</small>
              </header>
              <ul className="diag-list">
                <li>
                  <span>target missing</span>
                  <strong>{liveParity?.live?.no_live_market_target ? "yes" : "no"}</strong>
                </li>
                <li>
                  <span>last reject</span>
                  <strong>{textCell(lastRejectReason)}</strong>
                </li>
                <li>
                  <span>pending before</span>
                  <strong>{numCell(runtimeControl?.pending_orders_before_cycle) ?? 0}</strong>
                </li>
                <li>
                  <span>trigger</span>
                  <strong>{textCell(runtimeControl?.trigger_source)}</strong>
                </li>
                <li>
                  <span>loop</span>
                  <strong>
                    {numCell(runtimeControl?.base_loop_ms) ?? "--"}ms /{" "}
                    {numCell(runtimeControl?.fast_loop_ms) ?? "--"}ms
                  </strong>
                </li>
                <li>
                  <span>round switched</span>
                  <strong>{runtimeControl?.round_switched ? "yes" : "no"}</strong>
                </li>
              </ul>
            </article>
          </div>
          <div className="live-tables-grid">
            <div className="table-wrap">
              <h3 className="table-title">真实下单记录（最近 20）</h3>
              <table className="history-table live-exec-table">
                <thead>
                  <tr>
                    <th>结果</th>
                    <th>动作</th>
                    <th>方向</th>
                    <th>轮次</th>
                    <th>order_id</th>
                    <th>请求</th>
                    <th>延迟</th>
                    <th>原因</th>
                  </tr>
                </thead>
                <tbody>
                  {liveOrders.map((row, idx) => {
                    const response =
                      row.response && typeof row.response === "object"
                        ? (row.response as Record<string, unknown>)
                        : null;
                    const orderId =
                      textCell(response?.order_id) !== "--"
                        ? textCell(response?.order_id)
                        : textCell(response?.id);
                    const finalRequest =
                      row.final_request && typeof row.final_request === "object"
                        ? (row.final_request as Record<string, unknown>)
                        : row.request && typeof row.request === "object"
                        ? (row.request as Record<string, unknown>)
                        : null;
                    const reqPrice = numCell(finalRequest?.price_cents);
                    const reqSize = numCell(finalRequest?.size_shares);
                    const reqTif = textCell(finalRequest?.tif);
                    const reqStyle = textCell(finalRequest?.style);
                    const reqQuote = numCell(finalRequest?.quote_size_usdc);
                    const attempts = Array.isArray(row.attempts) ? row.attempts.length : 0;
                    return (
                      <tr key={`live-order-${idx}`}>
                        <td className={row.ok && row.accepted !== false ? "up" : "down"}>
                          {row.ok && row.accepted !== false ? "ok" : "fail"}
                        </td>
                        <td>{textCell(row.decision?.action)}</td>
                        <td>{textCell(row.decision?.side)}</td>
                        <td>{shortRoundId(textCell(row.decision?.round_id))}</td>
                        <td>{orderId}</td>
                        <td>
                          {reqPrice != null ? `${reqPrice.toFixed(2)}¢` : "--"} /{" "}
                          {reqSize != null ? reqSize.toFixed(4) : "--"} · {reqStyle}/{reqTif}
                          <div className="muted">
                            quote {reqQuote != null ? reqQuote.toFixed(4) : "--"} · retry {attempts}
                          </div>
                        </td>
                        <td>
                          {numCell(row.order_latency_ms) != null
                            ? `${numCell(row.order_latency_ms)!.toFixed(0)}ms`
                            : "--"}
                        </td>
                        <td>{textCell(row.error ?? row.reject_reason ?? row.reason)}</td>
                      </tr>
                    );
                  })}
                  {liveOrders.length === 0 ? (
                    <tr>
                      <td colSpan={8}>暂无真实下单记录</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">链路事件（最近 20）</h3>
              <table className="history-table live-event-table">
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>结果</th>
                    <th>动作</th>
                    <th>方向</th>
                    <th>order_id</th>
                    <th>原因</th>
                  </tr>
                </thead>
                <tbody>
                  {liveEvents.map((row, idx) => (
                    <tr key={`live-event-${idx}`}>
                      <td>{formatClockTime(numCell(row.ts_ms), timeMode)}</td>
                      <td className={row.accepted === false ? "down" : row.accepted === true ? "up" : ""}>
                        {row.accepted === true ? "ok" : row.accepted === false ? "fail" : "--"}
                      </td>
                      <td>{textCell(row.action)}</td>
                      <td>{textCell(row.side)}</td>
                      <td>{textCell(row.order_id)}</td>
                      <td>{textCell(row.reason)}</td>
                    </tr>
                  ))}
                  {liveEvents.length === 0 ? (
                    <tr>
                      <td colSpan={6}>暂无链路事件</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </section>
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
        <span>symbol: {strategyPaper?.symbol ?? selectedSymbol}</span>
        <span>profile: {effectiveProfile}</span>
        <span>profileSource: {profileSource}</span>
        <span>market: {strategyMarketType}</span>
        <span>
          最近成交: {formatTime(lastTradeTs, timeMode)}
          {idleMinutes != null ? ` (${idleMinutes.toFixed(1)}m 前)` : ""}
        </span>
        <span>
          runtime:
          {` ${
            strategyPaper?.runtime_defaults?.lookback_minutes ?? "--"
          }m / ${strategyPaper?.runtime_defaults?.max_trades ?? "--"} trades`}
        </span>
        <span>
          maxEntries/round: {strategyPaper?.config?.max_entries_per_round ?? "--"}
        </span>
        <span>
          coverage: {lookbackCoverageLabel}
          {lookbackTruncated ? " (truncated)" : ""}
        </span>
        <span>
          liveCtrl: {strategyPaper?.runtime_control?.mode ?? "normal"}
          {strategyPaper?.runtime_control?.effective_drain_only ? " (drain)" : ""}
        </span>
        {liveWarmupFallback ? (
          <span className="muted">live 预热中：已自动回退到 replay 展示</span>
        ) : null}
        {errorText ? <span className="down">{errorText}</span> : null}
      </footer>
    </section>
  );
}
