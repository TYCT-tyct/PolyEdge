## FEV1 Runtime Config (Forge-only)

This project runs strategy one as **FEV1** inside `crates/polyedge_forge`.
Paper and Live use the same signal engine; execution path differs by switch.

### Runtime switches

- `FORGE_FEV1_RUNTIME_ENABLED=true`  
  Enable the background paper/runtime loop (default true).

- `FORGE_FEV1_LIVE_EXECUTE=false`  
  Keep live order submission disabled by default.

- `FORGE_FEV1_LIVE_ENABLED=false|true`  
  Hard gate for live mode. Live requires both:
  - `FORGE_FEV1_LIVE_EXECUTE=true`
  - `FORGE_FEV1_LIVE_ENABLED=true`

### Gateway endpoints

- `FORGE_FEV1_GATEWAY_PRIMARY=http://127.0.0.1:9001`
- `FORGE_FEV1_GATEWAY_BACKUP=...` (optional)

### Execution defaults

- `FORGE_FEV1_MIN_QUOTE_USDC=1.0`
- `FORGE_FEV1_ENTRY_SLIPPAGE_BPS=18`
- `FORGE_FEV1_EXIT_SLIPPAGE_BPS=22`

### Notes

- FEV1 is BTC-only in current live target resolution.
- Legacy strategy names (roll/v52/predator) are not used by forge runtime.
