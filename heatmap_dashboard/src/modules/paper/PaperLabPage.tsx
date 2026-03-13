import { useEffect, useMemo, useRef, useState } from "react";

import {
  getStrategyExecutionAttribution,
  getStrategyPaperLedger,
  getStrategyReplay,
  getStrategyRuntime
} from "../../api";
import type {
  MarketSymbol,
  MarketType,
  StrategyAttributionResponse,
  StrategyPaperResponse
} from "../../types";

type TimeMode = "local" | "et";
type StrategyWorkbenchView =
  | "runtimePaper"
  | "runtimeLive"
  | "paperLedger"
  | "attribution"
  | "replayLab";

const ET_TIMEZONE = "America/New_York";
const CN_TIMEZONE = "Asia/Shanghai";
const STRATEGY_POLL_MIN_MS = 3_500;
const STRATEGY_POLL_MAX_MS = 20_000;
const STRATEGY_REPLAY_POLL_MIN_MS = 12_000;
const STRATEGY_REPLAY_POLL_MAX_MS = 30_000;
const STRATEGY_REPLAY_TIMEOUT_MS = 25_000;
const STRATEGY_PREFS_STORAGE_KEY = "polyedge.strategy.prefs.v5";
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
  view: StrategyWorkbenchView;
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
    const legacySource =
      parsed.source === "replay" || parsed.source === "live"
        ? String(parsed.source)
        : undefined;
    const view =
      parsed.view === "runtimePaper" ||
      parsed.view === "runtimeLive" ||
      parsed.view === "paperLedger" ||
      parsed.view === "attribution" ||
      parsed.view === "replayLab"
        ? (parsed.view as StrategyWorkbenchView)
        : legacySource === "live"
        ? "runtimeLive"
        : legacySource === "replay"
        ? "replayLab"
        : undefined;
    return {
      marketType,
      view
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

function strategyAttributionSignature(payload: StrategyAttributionResponse): string {
  return [
    payload.symbol ?? "",
    payload.market_type,
    payload.updated_at_ms ?? 0,
    payload.paper_records.length,
    payload.live_records.length,
    payload.order_lineage.length,
    payload.position_summary.length
  ].join("|");
}

function textCell(v: unknown): string {
  return typeof v === "string" && v.trim().length > 0 ? v.trim() : "--";
}

function numCell(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function objCell(v: unknown): Record<string, unknown> | null {
  return v != null && typeof v === "object" ? (v as Record<string, unknown>) : null;
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
    order_lineage:
      live.order_lineage && live.order_lineage.length > 80
        ? live.order_lineage.slice(live.order_lineage.length - 80)
        : live.order_lineage,
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

function describeStrategyError(
  view: StrategyWorkbenchView,
  message: string,
  symbol: MarketSymbol,
  marketType: MarketType
): string {
  if (view === "replayLab") {
    return message;
  }
  if (message.includes("live runtime symbol mismatch")) {
    const runtimeSymbol = message.split("runtime=").pop()?.trim();
    return runtimeSymbol
      ? `当前 Runtime 仅运行 ${runtimeSymbol}，所选 ${symbol} ${marketType} 不在实时范围内。`
      : `当前 ${symbol} ${marketType} 不在 Runtime 实时范围内。`;
  }
  if (message.includes("warming up")) {
    return `实时 Runtime 仍在预热，${symbol} ${marketType} 暂无快照。`;
  }
  return message;
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
  const [strategyAttribution, setStrategyAttribution] =
    useState<StrategyAttributionResponse | null>(null);
  const [strategyLoading, setStrategyLoading] = useState<boolean>(false);
  const [strategyMarketType, setStrategyMarketType] = useState<MarketType>(
    prefs.marketType ?? "5m"
  );
  const [strategyView, setStrategyView] = useState<StrategyWorkbenchView>(
    prefs.view ?? "runtimePaper"
  );
  const [errorText, setErrorText] = useState<string>("");

  const strategyInFlightRef = useRef<boolean>(false);
  const strategySigRef = useRef<string>("");
  const strategyUnchangedRef = useRef<number>(0);
  const strategyHasLoadedRef = useRef<boolean>(false);

  const strategyEnabled = STRATEGY_ENABLED_SYMBOLS.includes(selectedSymbol);
  const isReplayView = strategyView === "replayLab";
  const isRuntimePaperView = strategyView === "runtimePaper";
  const isRuntimeLiveView = strategyView === "runtimeLive";
  const isPaperLedgerView = strategyView === "paperLedger";
  const isAttributionView = strategyView === "attribution";
  const isRuntimeView = isRuntimePaperView || isRuntimeLiveView;

  useEffect(() => {
    writeStrategyUiPrefs({
      marketType: strategyMarketType,
      view: strategyView
    });
  }, [strategyMarketType, strategyView]);

  useEffect(() => {
    let alive = true;
    if (!strategyEnabled) {
      setStrategyPaper(null);
      setStrategyAttribution(null);
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
      if (isReplayView) {
        if (changed) {
          strategyUnchangedRef.current = 0;
          return STRATEGY_REPLAY_POLL_MIN_MS;
        }
        strategyUnchangedRef.current += 1;
        return Math.min(
          STRATEGY_REPLAY_POLL_MAX_MS,
          STRATEGY_REPLAY_POLL_MIN_MS + strategyUnchangedRef.current * 2_000
        );
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
        const requestTimeoutMs = isReplayView ? STRATEGY_REPLAY_TIMEOUT_MS : undefined;
        const requestOptions = {
          ...STRATEGY_PAPER_PROFILE,
          lookbackMinutes: PAPER_LOOKBACK_MINUTES,
          timeoutMs: requestTimeoutMs,
          ...(isRuntimeView ? STRATEGY_LIVE_PROFILE : {})
        };
        if (alive) {
          let nextSig = "";
          if (isAttributionView) {
            const attribution = await getStrategyExecutionAttribution(
              strategyMarketType,
              requestOptions,
              selectedSymbol
            );
            nextSig = strategyAttributionSignature(attribution);
            changed = nextSig !== strategySigRef.current;
            if (changed) {
              strategySigRef.current = nextSig;
              setStrategyAttribution(attribution);
              setStrategyPaper(null);
            }
          } else {
            const data = isReplayView
              ? await getStrategyReplay(strategyMarketType, requestOptions, selectedSymbol)
              : isPaperLedgerView
              ? await getStrategyPaperLedger(strategyMarketType, requestOptions, selectedSymbol)
              : await getStrategyRuntime(strategyMarketType, requestOptions, selectedSymbol);
            const compactData = compactStrategyPayload(data);
            nextSig = strategyPaperSignature(compactData);
            changed = nextSig !== strategySigRef.current;
            if (changed) {
              strategySigRef.current = nextSig;
              setStrategyPaper(compactData);
              setStrategyAttribution(null);
            }
          }
          strategyHasLoadedRef.current = true;
          setErrorText("");
        }
      } catch (err) {
        if (alive) {
          const message = err instanceof Error ? err.message : String(err);
          setErrorText(
            describeStrategyError(strategyView, message, selectedSymbol, strategyMarketType)
          );
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
  }, [
    isAttributionView,
    isPaperLedgerView,
    isReplayView,
    isRuntimeView,
    strategyEnabled,
    selectedSymbol,
    strategyMarketType,
    strategyView
  ]);

  if (!strategyEnabled) {
    return (
      <section className="panel">
        <header className="panel-head">
          <div>
            <h2>策略工作台</h2>
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
  const liveSummary = liveExecution?.summary;
  const liveParity = liveExecution?.parity_check;
  const liveState = liveExecution?.state_machine;
  const runtimeControl = strategyPaper?.runtime_control;
  const executionAggr = strategyPaper?.execution_aggressiveness;
  const executionTarget = liveExecution?.execution_target;
  const liveRealizedNetCents =
    numCell(liveSummary?.realized_net_pnl_cents) ??
    numCell(liveSummary?.fills?.realized_net_pnl_cents) ??
    (numCell(liveState?.realized_pnl_usdc) != null
      ? numCell(liveState?.realized_pnl_usdc)! * 100
      : null);
  const liveFillEventCount = numCell(liveSummary?.fills?.fill_event_count);
  const liveFillDecisionCount = numCell(liveSummary?.fills?.fill_decision_count);
  const liveFillCostCents = numCell(liveSummary?.fills?.total_fill_cost_cents);
  const livePaperFillCount = numCell(liveSummary?.paper_live_fill_count);
  const liveEvents = (liveExecution?.events ?? []).slice(-20).reverse();
  const liveOrders = (liveExecution?.execution?.orders ?? []).slice(-20).reverse();
  const livePaperRecords = (liveExecution?.paper_records ?? [])
    .slice(-20)
    .reverse();
  const liveOrderLineage = (liveExecution?.order_lineage ?? [])
    .slice(-20)
    .reverse();
  const lineagePendingDecisionCount = liveOrderLineage.filter((row) => {
    const pendingCount = numCell(row.pending_count);
    return pendingCount != null && pendingCount > 0;
  }).length;
  const lineagePendingOrderCount = liveOrderLineage.reduce((sum, row) => {
    const pendingCount = numCell(row.pending_count) ?? 0;
    return sum + pendingCount;
  }, 0);
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
  const displayedNetCents =
    isRuntimeLiveView && liveRealizedNetCents != null
      ? liveRealizedNetCents
      : summaryNet;
  const tradeRows = (strategyPaper?.trades ?? []).slice(-20).reverse();
  const sourceLabel = isAttributionView
    ? strategyAttribution?.view?.label ?? strategyAttribution?.source ?? "--"
    : strategyPaper?.view?.label ?? strategyPaper?.source ?? "--";
  const resolvedSymbol = strategyPaper?.symbol ?? strategyAttribution?.symbol ?? selectedSymbol;
  const viewTitle = isRuntimePaperView
    ? "实时 Paper"
    : isRuntimeLiveView
    ? "实时 Live"
    : isPaperLedgerView
    ? "Paper 账本"
    : isAttributionView
    ? "执行归因"
    : "回放研究";
  const loadingLabel = strategyLoading ? "计算中..." : viewTitle;
  const viewSummaryText = isRuntimePaperView
    ? "当前展示的是运行中的模拟交易状态与运行期 Paper 交易，不混入回放历史。"
    : isRuntimeLiveView
    ? "当前展示的是运行中的真实执行链；下方 Paper 交易仅作为同一 Runtime 的模拟参考。"
    : isPaperLedgerView
    ? "当前展示的是实时策略真正落下来的模拟账本，不再混入 Replay 重算结果。"
    : isAttributionView
    ? "当前展示的是同一批 intent 的 Paper 预期、真实成交、费用与延迟归因。"
    : "当前展示的是历史样本离线重算结果，用于研究，不代表当前 Runtime 正在交易。";
  const tradeTableTitle = isRuntimePaperView
    ? "运行期 Paper 交易（最近 20 条）"
    : isRuntimeLiveView
    ? "运行期 Paper 参考交易（最近 20 条）"
    : isPaperLedgerView
    ? "Paper 历史账本（最近 20 条）"
    : "回放交易记录（最近 20 条）";
  const attributionCurrent = objCell(strategyAttribution?.current);
  const attributionRuntimeControl = objCell(strategyAttribution?.runtime_control);
  const attributionFillsSummary = objCell(strategyAttribution?.fills_summary);
  const attributionLatency = objCell(strategyAttribution?.latency);
  const attributionSignalToSubmit = objCell(attributionLatency?.signal_to_submit_ms);
  const attributionSubmitToAck = objCell(attributionLatency?.submit_to_ack_ms);
  const attributionOrderLatency = objCell(attributionLatency?.order_latency_ms);
  const attributionActiveSignal = objCell(attributionCurrent?.active_signal);
  const attributionPaperRecords = [...(strategyAttribution?.paper_records ?? [])]
    .slice(-20)
    .reverse();
  const attributionLiveRecords = [...(strategyAttribution?.live_records ?? [])]
    .slice(-20)
    .reverse();
  const attributionOrderLineage = [...(strategyAttribution?.order_lineage ?? [])]
    .slice(-20)
    .reverse();
  const attributionPositionSummary = [...(strategyAttribution?.position_summary ?? [])].slice(
    0,
    20
  );
  const attributionPaperNetTotal = attributionPositionSummary.reduce(
    (sum, row) => sum + (numCell(row.paper_realized_pnl_cents) ?? 0),
    0
  );
  const attributionLiveNetTotal = attributionPositionSummary.reduce(
    (sum, row) => sum + (numCell(row.live_realized_pnl_cents) ?? 0),
    0
  );
  const attributionDriftTotal = attributionPositionSummary.reduce(
    (sum, row) => sum + (numCell(row.execution_drift_cents) ?? 0),
    0
  );
  const attributionUpdatedAt = finite(strategyAttribution?.updated_at_ms);
  const attributionCurrentTs =
    numCell(attributionCurrent?.timestamp_ms) ?? attributionUpdatedAt;
  const attributionAction =
    textCell(attributionActiveSignal?.action ?? attributionCurrent?.suggested_action);
  const attributionRoundId = textCell(
    attributionActiveSignal?.round_id ?? attributionCurrent?.round_id
  );
  const attributionIntentId = textCell(attributionActiveSignal?.intent_id);
  const attributionPositionId = textCell(attributionActiveSignal?.position_id);

  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>策略工作台（{selectedSymbol} · {strategyMarketType}）</h2>
          <p className="muted">{viewSummaryText}</p>
        </div>
        <div className="panel-actions">
          <span className="loading-chip">{loadingLabel}</span>
        </div>
      </header>

      <div className="status-row">
        <span>视图: {viewTitle}</span>
        <span>来源: {sourceLabel}</span>
        <span>symbol: {resolvedSymbol}</span>
        <span>market: {strategyMarketType}</span>
      </div>

      {isAttributionView ? (
        <div className="info-cards compact strategy-matrix">
          <article className="info-card">
            <span>当前意图 / 轮次</span>
            <strong
              className={
                attributionAction.includes("UP")
                  ? "up"
                  : attributionAction.includes("DOWN")
                  ? "down"
                  : ""
              }
            >
              {attributionAction}
            </strong>
            <small>
              {formatTime(attributionCurrentTs, timeMode)} · {shortRoundId(attributionRoundId)}
            </small>
          </article>
          <article className="info-card">
            <span>归因净收益</span>
            <strong className={attributionLiveNetTotal >= 0 ? "up" : "down"}>
              {`${attributionLiveNetTotal.toFixed(2)}¢`}
            </strong>
            <small>
              Paper {attributionPaperNetTotal.toFixed(2)}¢ · Drift {attributionDriftTotal.toFixed(2)}¢
            </small>
          </article>
          <article className="info-card">
            <span>成交覆盖</span>
            <strong>
              {`${numCell(attributionFillsSummary?.fill_decision_count) ?? 0} / ${
                numCell(attributionFillsSummary?.fill_event_count) ?? 0
              }`}
            </strong>
            <small>
              decisions / events · fee{" "}
              {numCell(attributionFillsSummary?.total_fill_cost_cents) != null
                ? `${numCell(attributionFillsSummary?.total_fill_cost_cents)!.toFixed(2)}¢`
                : "--"}
            </small>
          </article>
          <article className="info-card">
            <span>signal → submit</span>
            <strong>
              {numCell(attributionSignalToSubmit?.p50_ms) != null
                ? `${numCell(attributionSignalToSubmit?.p50_ms)!.toFixed(0)}ms`
                : "--"}
            </strong>
            <small>
              p95{" "}
              {numCell(attributionSignalToSubmit?.p95_ms) != null
                ? `${numCell(attributionSignalToSubmit?.p95_ms)!.toFixed(0)}ms`
                : "--"}{" "}
              · count {numCell(attributionSignalToSubmit?.count) ?? 0}
            </small>
          </article>
          <article className="info-card">
            <span>submit → ack</span>
            <strong>
              {numCell(attributionSubmitToAck?.p50_ms) != null
                ? `${numCell(attributionSubmitToAck?.p50_ms)!.toFixed(0)}ms`
                : "--"}
            </strong>
            <small>
              p95{" "}
              {numCell(attributionSubmitToAck?.p95_ms) != null
                ? `${numCell(attributionSubmitToAck?.p95_ms)!.toFixed(0)}ms`
                : "--"}{" "}
              · count {numCell(attributionSubmitToAck?.count) ?? 0}
            </small>
          </article>
          <article className="info-card">
            <span>成交链延迟</span>
            <strong>
              {numCell(attributionOrderLatency?.p50_ms) != null
                ? `${numCell(attributionOrderLatency?.p50_ms)!.toFixed(0)}ms`
                : "--"}
            </strong>
            <small>
              p95{" "}
              {numCell(attributionOrderLatency?.p95_ms) != null
                ? `${numCell(attributionOrderLatency?.p95_ms)!.toFixed(0)}ms`
                : "--"}{" "}
              · 模式 {textCell(attributionRuntimeControl?.mode)}
            </small>
          </article>
        </div>
      ) : (
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
            <strong className={(displayedNetCents ?? 0) >= 0 ? "up" : "down"}>
              {displayedNetCents != null ? `${displayedNetCents.toFixed(2)}¢` : "--"}
            </strong>
            <small>
              交易 {summary?.trade_count ?? 0} · 胜率 {summary ? `${summary.win_rate_pct.toFixed(1)}%` : "--"} · 窗口{" "}
              {strategyPaper?.lookback_minutes ?? "--"}m · 覆盖 {lookbackCoverageLabel}
              {lookbackTruncated ? " (截断)" : ""}
            </small>
            {isRuntimeLiveView ? (
              <small>
                Paper {summaryNet != null ? `${summaryNet.toFixed(2)}¢` : "--"} · Live{" "}
                {liveRealizedNetCents != null ? `${liveRealizedNetCents.toFixed(2)}¢` : "--"}
              </small>
            ) : null}
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
      )}

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
            className={isRuntimePaperView ? "active" : ""}
            onClick={() => setStrategyView("runtimePaper")}
          >
            实时 Paper
          </button>
          <button
            className={isRuntimeLiveView ? "active" : ""}
            onClick={() => setStrategyView("runtimeLive")}
          >
            实时 Live
          </button>
          <button
            className={isPaperLedgerView ? "active" : ""}
            onClick={() => setStrategyView("paperLedger")}
          >
            Paper 账本
          </button>
          <button
            className={isAttributionView ? "active" : ""}
            onClick={() => setStrategyView("attribution")}
          >
            执行归因
          </button>
          <button
            className={isReplayView ? "active" : ""}
            onClick={() => setStrategyView("replayLab")}
          >
            回放研究
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

      {isAttributionView ? (
        <section className="live-execution-wrap">
          <div className="live-block-grid">
            <article className="live-block">
              <header>
                <h3>当前信号锚点</h3>
                <small>intent / position / round</small>
              </header>
              <ul className="diag-list">
                <li>
                  <span>intent_id</span>
                  <strong>{shortRoundId(attributionIntentId)}</strong>
                </li>
                <li>
                  <span>position_id</span>
                  <strong>{shortRoundId(attributionPositionId)}</strong>
                </li>
                <li>
                  <span>round / action</span>
                  <strong>
                    {shortRoundId(attributionRoundId)} / {attributionAction}
                  </strong>
                </li>
                <li>
                  <span>updated</span>
                  <strong>{formatClockTime(attributionUpdatedAt, timeMode)}</strong>
                </li>
                <li>
                  <span>runtime mode</span>
                  <strong>{textCell(attributionRuntimeControl?.mode)}</strong>
                </li>
              </ul>
            </article>
            <article className="live-block">
              <header>
                <h3>归因摘要</h3>
                <small>Paper vs Live</small>
              </header>
              <ul className="diag-list">
                <li>
                  <span>paper realized</span>
                  <strong className={attributionPaperNetTotal >= 0 ? "up" : "down"}>
                    {`${attributionPaperNetTotal.toFixed(2)}¢`}
                  </strong>
                </li>
                <li>
                  <span>live realized</span>
                  <strong className={attributionLiveNetTotal >= 0 ? "up" : "down"}>
                    {`${attributionLiveNetTotal.toFixed(2)}¢`}
                  </strong>
                </li>
                <li>
                  <span>execution drift</span>
                  <strong className={attributionDriftTotal <= 0 ? "up" : "down"}>
                    {`${attributionDriftTotal.toFixed(2)}¢`}
                  </strong>
                </li>
                <li>
                  <span>paper records</span>
                  <strong>{strategyAttribution?.paper_records.length ?? 0}</strong>
                </li>
                <li>
                  <span>live records</span>
                  <strong>{strategyAttribution?.live_records.length ?? 0}</strong>
                </li>
              </ul>
            </article>
          </div>

          <div className="live-tables-grid">
            <div className="table-wrap">
              <h3 className="table-title">真实成交归因（最近 20）</h3>
              <table className="history-table live-paper-fill-table">
                <thead>
                  <tr>
                    <th>动作</th>
                    <th>方向</th>
                    <th>decision_id</th>
                    <th>Paper 预期</th>
                    <th>真实成交</th>
                    <th>费用</th>
                    <th>净盈亏</th>
                  </tr>
                </thead>
                <tbody>
                  {attributionLiveRecords.map((row, idx) => {
                    const paperExec =
                      numCell(row.paper_expected_exec_price_cents) ??
                      numCell(row.paper_exec_price_cents) ??
                      numCell(row.price_cents);
                    const actualFill =
                      numCell(row.actual_fill_price_cents) ??
                      numCell(row.live_fill_price_cents);
                    const actualFee = numCell(row.actual_fee_cents);
                    const liveNet =
                      numCell(row.live_fill_pnl_cents_net) ??
                      numCell(row.realized_pnl_cents);
                    return (
                      <tr key={`attr-live-${idx}`}>
                        <td>{textCell(row.action)}</td>
                        <td>{textCell(row.side)}</td>
                        <td>{shortRoundId(textCell(row.decision_id))}</td>
                        <td>{paperExec != null ? `${paperExec.toFixed(2)}¢` : "--"}</td>
                        <td>{actualFill != null ? `${actualFill.toFixed(2)}¢` : "--"}</td>
                        <td>{actualFee != null ? `${actualFee.toFixed(2)}¢` : "--"}</td>
                        <td className={liveNet != null && liveNet < 0 ? "down" : "up"}>
                          {liveNet != null ? `${liveNet.toFixed(2)}¢` : "--"}
                        </td>
                      </tr>
                    );
                  })}
                  {attributionLiveRecords.length === 0 ? (
                    <tr>
                      <td colSpan={7}>暂无真实成交归因记录</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">Position 汇总</h3>
              <table className="history-table live-lineage-table">
                <thead>
                  <tr>
                    <th>position_id</th>
                    <th>记录数</th>
                    <th>Paper</th>
                    <th>Live</th>
                    <th>Drift</th>
                  </tr>
                </thead>
                <tbody>
                  {attributionPositionSummary.map((row, idx) => {
                    const paperNet = numCell(row.paper_realized_pnl_cents);
                    const liveNet = numCell(row.live_realized_pnl_cents);
                    const drift = numCell(row.execution_drift_cents);
                    return (
                      <tr key={`attr-position-${idx}`}>
                        <td>{shortRoundId(textCell(row.position_id))}</td>
                        <td>{numCell(row.record_count) ?? 0}</td>
                        <td className={paperNet != null && paperNet < 0 ? "down" : "up"}>
                          {paperNet != null ? `${paperNet.toFixed(2)}¢` : "--"}
                        </td>
                        <td className={liveNet != null && liveNet < 0 ? "down" : "up"}>
                          {liveNet != null ? `${liveNet.toFixed(2)}¢` : "--"}
                        </td>
                        <td className={drift != null && drift > 0 ? "down" : "up"}>
                          {drift != null ? `${drift.toFixed(2)}¢` : "--"}
                        </td>
                      </tr>
                    );
                  })}
                  {attributionPositionSummary.length === 0 ? (
                    <tr>
                      <td colSpan={5}>暂无 position 归因汇总</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">Paper 信号账本（最近 20）</h3>
              <table className="history-table live-event-table">
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>动作</th>
                    <th>方向</th>
                    <th>decision_id</th>
                    <th>信号价</th>
                    <th>Paper 预期</th>
                    <th>原因</th>
                  </tr>
                </thead>
                <tbody>
                  {attributionPaperRecords.map((row, idx) => {
                    const signalPrice =
                      numCell(row.signal_price_cents) ?? numCell(row.price_cents);
                    const paperExec =
                      numCell(row.paper_expected_exec_price_cents) ??
                      numCell(row.paper_exec_price_cents);
                    return (
                      <tr key={`attr-paper-${idx}`}>
                        <td>{formatClockTime(numCell(row.ts_ms), timeMode)}</td>
                        <td>{textCell(row.action)}</td>
                        <td>{textCell(row.side)}</td>
                        <td>{shortRoundId(textCell(row.decision_id ?? row.intent_id))}</td>
                        <td>{signalPrice != null ? `${signalPrice.toFixed(2)}¢` : "--"}</td>
                        <td>{paperExec != null ? `${paperExec.toFixed(2)}¢` : "--"}</td>
                        <td>{textCell(row.reason)}</td>
                      </tr>
                    );
                  })}
                  {attributionPaperRecords.length === 0 ? (
                    <tr>
                      <td colSpan={7}>暂无 Paper 信号账本</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">订单映射 Lineage（最近 20）</h3>
              <table className="history-table live-lineage-table">
                <thead>
                  <tr>
                    <th>decision_id</th>
                    <th>order_ids</th>
                    <th>pending</th>
                    <th>状态</th>
                    <th>原因</th>
                    <th>更新时间</th>
                  </tr>
                </thead>
                <tbody>
                  {attributionOrderLineage.map((row, idx) => {
                    const orderIds = Array.isArray(row.order_ids)
                      ? row.order_ids
                          .map((v) => textCell(v))
                          .filter((v) => v !== "--")
                          .join(", ")
                      : "--";
                    const pendingCount = numCell(row.pending_count);
                    return (
                      <tr key={`attr-lineage-${idx}`}>
                        <td>{shortRoundId(textCell(row.decision_id))}</td>
                        <td>{orderIds === "" ? "--" : orderIds}</td>
                        <td className={pendingCount != null && pendingCount > 0 ? "down" : "up"}>
                          {pendingCount != null ? pendingCount : "--"}
                        </td>
                        <td>{textCell(row.last_status)}</td>
                        <td>{textCell(row.last_reason)}</td>
                        <td>{formatClockTime(numCell(row.last_ts_ms), timeMode)}</td>
                      </tr>
                    );
                  })}
                  {attributionOrderLineage.length === 0 ? (
                    <tr>
                      <td colSpan={6}>暂无订单映射数据</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      ) : null}

      {isRuntimeLiveView ? (
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
              <span>真实成交覆盖</span>
              <strong>
                {`${livePaperFillCount ?? 0} / ${liveFillDecisionCount ?? 0}`}
              </strong>
              <small>
                fill events {liveFillEventCount ?? 0} · fill cost{" "}
                {liveFillCostCents != null ? `${liveFillCostCents.toFixed(2)}¢` : "--"}
              </small>
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
                  <span>lineage pending</span>
                  <strong>{`${lineagePendingDecisionCount}/${lineagePendingOrderCount}`}</strong>
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
                    <th>decision_id</th>
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
                    const reqPrice =
                      numCell(finalRequest?.price_cents) ??
                      (numCell(finalRequest?.price) != null
                        ? numCell(finalRequest?.price)! * 100
                        : null);
                    const reqSize =
                      numCell(finalRequest?.size_shares) ??
                      numCell(finalRequest?.size);
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
                        <td>{shortRoundId(textCell(row.decision_id ?? row.decision?.decision_id))}</td>
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
                      <td colSpan={9}>暂无真实下单记录</td>
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
                    <th>decision_id</th>
                    <th>order_id</th>
                    <th>成交价</th>
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
                      <td>{shortRoundId(textCell(row.decision_id))}</td>
                      <td>{textCell(row.order_id)}</td>
                      <td>
                        {numCell(row.fill_price_cents) != null
                          ? `${numCell(row.fill_price_cents)!.toFixed(2)}¢`
                          : "--"}
                      </td>
                      <td>{textCell(row.reason)}</td>
                    </tr>
                  ))}
                  {liveEvents.length === 0 ? (
                    <tr>
                      <td colSpan={8}>暂无链路事件</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">Paper 信号与真实成交对照（最近 20）</h3>
              <table className="history-table live-paper-fill-table">
                <thead>
                  <tr>
                    <th>动作</th>
                    <th>方向</th>
                    <th>decision_id</th>
                    <th>信号价</th>
                    <th>真实成交价</th>
                    <th>真实净盈亏</th>
                  </tr>
                </thead>
                <tbody>
                  {livePaperRecords.map((row, idx) => {
                    const liveFill = objCell(row.live_fill);
                    const paperPrice = numCell(row.price_cents);
                    const fillPrice =
                      numCell(row.live_fill_price_cents) ??
                      numCell(liveFill?.fill_price_cents);
                    const fillNetPnl =
                      numCell(row.live_fill_pnl_cents_net) ??
                      numCell(liveFill?.fill_pnl_cents_net);
                    return (
                      <tr key={`live-paper-fill-${idx}`}>
                        <td>{textCell(row.action)}</td>
                        <td>{textCell(row.side)}</td>
                        <td>{shortRoundId(textCell(row.decision_id))}</td>
                        <td>{paperPrice != null ? `${paperPrice.toFixed(2)}¢` : "--"}</td>
                        <td>{fillPrice != null ? `${fillPrice.toFixed(2)}¢` : "--"}</td>
                        <td className={fillNetPnl != null && fillNetPnl < 0 ? "down" : "up"}>
                          {fillNetPnl != null ? `${fillNetPnl.toFixed(2)}¢` : "--"}
                        </td>
                      </tr>
                    );
                  })}
                  {livePaperRecords.length === 0 ? (
                    <tr>
                      <td colSpan={6}>暂无 Paper / Live 对照数据</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div className="table-wrap">
              <h3 className="table-title">订单映射 Lineage（最近 20）</h3>
              <table className="history-table live-lineage-table">
                <thead>
                  <tr>
                    <th>decision_id</th>
                    <th>order_ids</th>
                    <th>pending</th>
                    <th>状态</th>
                    <th>原因</th>
                    <th>更新时间</th>
                  </tr>
                </thead>
                <tbody>
                  {liveOrderLineage.map((row, idx) => {
                    const orderIds = Array.isArray(row.order_ids)
                      ? row.order_ids
                          .map((v) => textCell(v))
                          .filter((v) => v !== "--")
                          .join(", ")
                      : "--";
                    const pendingCount = numCell(row.pending_count);
                    return (
                      <tr key={`live-lineage-${idx}`}>
                        <td>{shortRoundId(textCell(row.decision_id))}</td>
                        <td>{orderIds === "" ? "--" : orderIds}</td>
                        <td className={pendingCount != null && pendingCount > 0 ? "down" : "up"}>
                          {pendingCount != null ? pendingCount : "--"}
                        </td>
                        <td>{textCell(row.last_status)}</td>
                        <td>{textCell(row.last_reason)}</td>
                        <td>{formatClockTime(numCell(row.last_ts_ms), timeMode)}</td>
                      </tr>
                    );
                  })}
                  {liveOrderLineage.length === 0 ? (
                    <tr>
                      <td colSpan={6}>暂无订单映射数据</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      ) : null}

      {!isAttributionView ? (
        <div className="table-wrap">
          <h3 className="table-title">{tradeTableTitle}</h3>
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
              {tradeRows.map((t) => (
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
              {tradeRows.length === 0 ? (
                <tr>
                  <td colSpan={9}>暂无交易样本</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      ) : null}

      <footer className="status-row">
        <span>
          source: {isAttributionView ? strategyAttribution?.source ?? "--" : strategyPaper?.source ?? "--"}
        </span>
        <span>
          view:{" "}
          {isAttributionView
            ? strategyAttribution?.view?.family ?? "attribution"
            : strategyPaper?.view?.family ?? (isReplayView ? "replay" : isPaperLedgerView ? "ledger" : "runtime")}
        </span>
        <span>symbol: {resolvedSymbol}</span>
        <span>market: {strategyMarketType}</span>
        {!isAttributionView ? (
          <>
            <span>profile: {effectiveProfile}</span>
            <span>profileSource: {profileSource}</span>
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
          </>
        ) : (
          <>
            <span>intent: {shortRoundId(attributionIntentId)}</span>
            <span>position: {shortRoundId(attributionPositionId)}</span>
            <span>control: {textCell(attributionRuntimeControl?.mode)}</span>
            <span>updated: {formatTime(attributionUpdatedAt, timeMode)}</span>
          </>
        )}
        {errorText ? <span className="down">{errorText}</span> : null}
      </footer>
    </section>
  );
}
