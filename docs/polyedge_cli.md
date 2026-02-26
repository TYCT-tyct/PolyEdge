# polyedge CLI

Unified operation entry for Paper / Live / Forge / Data.

## Install

```powershell
powershell -ExecutionPolicy Bypass -File scripts/install_polyedge_cli.ps1
```

## Commands

```powershell
polyedge paper start
polyedge paper stop
polyedge paper restart

polyedge live start
polyedge live stop
polyedge live restart

polyedge forge start
polyedge forge stop
polyedge forge restart

polyedge data start
polyedge data stop
polyedge data restart

polyedge docker
polyedge --help
poly --help
```

### Live command semantics

- `polyedge live start`: enable real execution (`FORGE_FEV1_LIVE_EXECUTE=true`) and disable drain mode.
- `polyedge live stop`: switch to **DRAIN** mode (no new entries, only exits/reduces) to protect open positions.
- `polyedge live restart`: restart forge services and keep current env mode.

## Config

Default config file path:

`configs/polyedge_cli.toml`

You can override by:

```powershell
polyedge --config E:\Projects\test\configs\polyedge_cli.toml docker
```
