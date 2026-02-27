import type { WindowType } from "./types";

// ------------------------------------------------------------------ //
// windowToMinutes — 消除 App.tsx 和 api.ts 中的重复定义
// ------------------------------------------------------------------ //
export function windowToMinutes(view: WindowType): number {
  switch (view) {
    case "5m": return 5;
    case "15m": return 15;
    case "30m": return 30;
    case "1h": return 60;
    case "2h": return 120;
    case "4h": return 240;
    default: return 0;
  }
}
