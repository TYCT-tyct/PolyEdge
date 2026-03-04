# heatmap_dashboard/src — FRONTEND

## OVERVIEW
React 18 + TypeScript + Vite + uPlot. Served from Ireland `dist/` after `npm run build`. Chinese UI language.

## FILE ROLES
| File | Owns |
|------|------|
| `App.tsx` | Root component: all views, tabs, layout (~1600 lines) |
| `api.ts` | API client: fetch wrappers, caching, WS, deduplication (~1150 lines) |
| `types.ts` | All TypeScript interfaces (frontend mirror of Forge types) |
| `utils.ts` | Date formatting, price formatting, time helpers |
| `main.tsx` | React root mount |
| `components/MarketChart.tsx` | uPlot price/probability chart per market |
| `components/HeatmapGrid.tsx` | Delta% × time-remaining pricing heatmap |
| `components/AccuracyChart.tsx` | Rolling direction accuracy chart |
| `modules/paper/PaperLabPage.tsx` | Paper trading lab: replay mode + live paper mode |

## API CLIENT PATTERNS (api.ts)
- Backend URL from `VITE_FORGE_API_BASE` env var (defaults to same origin)
- Per-endpoint TTL cache (avoid redundant polls)
- In-flight deduplication (single request per key at a time)
- WebSocket: `/ws/live` — real-time market snapshots → dispatched to subscribed components

## DASHBOARD VIEWS
1. **Market Monitor** — 5m/15m charts, live bid/ask, round countdown, velocity metrics
2. **Heatmap** — 72h pricing heatmap by delta% bins × time-remaining bins
3. **Accuracy** — 24h rolling direction accuracy, min/max/avg
4. **Rounds History** — Past 20 rounds: outcome, delta%, target vs settle price
5. **Collector Status** — Feed health, coverage window, 5m/15m status
6. **Paper Lab** — `PaperLabPage`: backtest replay + live paper execution panel

## SYMBOL / TIMEFRAME TOGGLES
- Symbols: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT
- Windows: 5m, 15m, 30m, 1h, 2h, 4h, All
- Timezone: Local / ET

## CONVENTIONS
- All API responses typed in `types.ts` — add new response shapes there first
- No `any` types — use proper interfaces
- Chart components are pure (props in → render out) — no internal API fetching
- `App.tsx` orchestrates all data fetching via `api.ts`

## BUILD
```bash
npm --prefix heatmap_dashboard ci
npm --prefix heatmap_dashboard run build
# dist/ output deployed to Ireland server
```

## ANTI-PATTERNS
- Don't add `@ts-ignore` or `as any` casts
- Don't fetch API data inside chart components — pass data via props
- Don't hard-code backend URL — always use `VITE_FORGE_API_BASE`
