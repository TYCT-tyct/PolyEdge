# Polymarket Trading Workspace

This workspace implements a no-mock, event-driven system for paper-to-live trading:

- Real feeds only (Polymarket + external exchanges)
- Maker strategy with inventory controls
- Risk gates, replay engine, and paper executor
- Control plane via HTTP

## Run

```bash
cargo run -p app_runner
```

On Windows, install prerequisites first:

```powershell
pwsh -File .\scripts\setup_windows.ps1
```

## Control API

- `GET /health`
- `GET /metrics`
- `GET /state/positions`
- `GET /state/pnl`
- `POST /control/pause`
- `POST /control/resume`
- `POST /control/flatten`
