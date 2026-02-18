Param(
  [Parameter(Mandatory = $true)]
  [string]$SshHost,

  [Parameter(Mandatory = $true)]
  [string]$KeyPath,

  [string]$User = "ubuntu",

  # Local port for SSH -L tunnel -> remote 127.0.0.1:8080
  [int]$LocalPort = 18080,

  # Remote engine/control API port bound to loopback on the server
  [int]$RemotePort = 8080,

  [string]$RunId = "",

  [int]$SweepRepeats = 3,
  [ValidateSet("quick", "standard", "deep")]
  [string]$SweepProfile = "quick",

  [int]$StormDurationSec = 60,
  [int]$StormConcurrency = 8,
  [int]$StormBurstRps = 20,

  [ValidateSet("maker_first", "taker_first", "taker_only")]
  [string]$PredatorPriority = "taker_first",

  # Optional baseline run dir for automatic regression attribution + gate.
  [string]$BaselineRunDir = "",
  [switch]$RegressionGate = $false,

  # Optional: three-window A/B is more about gate/EV than raw latency; keep opt-in for latency work.
  [switch]$DoThreeWindow = $false
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function UtcDay {
  return (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
}

function NowUtcIso {
  return (Get-Date).ToUniversalTime().ToString("o")
}

function Ensure-RunId {
  if ($RunId -and $RunId.Trim().Length -gt 0) { return }
  $script:RunId = "fullchain-" + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
}

function Write-TextFile([string]$Path, [string]$Content) {
  $dir = Split-Path -Parent $Path
  if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  Set-Content -Path $Path -Value $Content -Encoding utf8
}

function Write-JsonFile([string]$Path, $Obj) {
  $dir = Split-Path -Parent $Path
  if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  ($Obj | ConvertTo-Json -Depth 40) | Set-Content -Path $Path -Encoding utf8
}

function Invoke-RestJson([string]$Method, [string]$Url, $Body = $null, [int]$TimeoutSec = 5) {
  if ($Method -eq "GET") {
    return Invoke-RestMethod -Method Get -Uri $Url -TimeoutSec $TimeoutSec
  }
  if ($Body -eq $null) { $Body = @{} }
  return Invoke-RestMethod -Method Post -Uri $Url -TimeoutSec $TimeoutSec -ContentType "application/json" -Body ($Body | ConvertTo-Json -Compress)
}

function Wait-HttpOk([string]$Url, [int]$Attempts = 40, [int]$SleepMs = 250) {
  for ($i = 0; $i -lt $Attempts; $i++) {
    try {
      $resp = Invoke-WebRequest -Method Get -Uri $Url -TimeoutSec 2
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) { return }
    } catch {}
    Start-Sleep -Milliseconds $SleepMs
  }
  throw "endpoint not reachable: $Url"
}

function Try-JsonSnapshot([string]$Path, [string]$Url) {
  try {
    Write-JsonFile $Path (Invoke-RestJson "GET" $Url $null 5)
    return $true
  } catch {
    Write-TextFile ($Path + ".err.txt") ($_.Exception.Message)
    return $false
  }
}

function Try-TextSnapshot([string]$Path, [string]$Url) {
  try {
    $resp = Invoke-WebRequest -Method Get -Uri $Url -TimeoutSec 10
    Write-TextFile $Path $resp.Content
    return $true
  } catch {
    Write-TextFile ($Path + ".err.txt") ($_.Exception.Message)
    return $false
  }
}

Ensure-RunId

$Day = UtcDay
$BaseUrl = "http://127.0.0.1:$LocalPort"
$RunDir = "datasets/reports/$Day/runs/$RunId"
$SnapDir = "$RunDir/snapshots"

  Write-Host ("run_id=" + $RunId)
  Write-Host ("base_url=" + $BaseUrl)
  Write-Host ("run_dir=" + $RunDir)
  if ($BaselineRunDir) { Write-Host ("baseline_run_dir=" + $BaselineRunDir) }

# Start SSH tunnel in background.
$sshArgs = @(
  "-F", "NUL",
  "-i", $KeyPath,
  "-o", "ExitOnForwardFailure=yes",
  "-o", "ServerAliveInterval=10",
  "-o", "ServerAliveCountMax=3",
  "-N",
  "-L", "$LocalPort`:`127.0.0.1`:$RemotePort",
  "$User@$SshHost"
)

$tunnel = $null
try {
  $tunnel = Start-Process -FilePath "ssh" -ArgumentList $sshArgs -NoNewWindow -PassThru

  # Wait for the tunnelled HTTP endpoint.
  Wait-HttpOk "$BaseUrl/health" 60 250

  # Remote system snapshot (for later attribution).
  $remoteInfo = & ssh -F NUL -i $KeyPath "$User@$SshHost" "bash -lc 'set -e; date -u -Ins; uname -a; (lscpu || true) | head -n 40; (free -m || true); (ss -lntp || true) | head -n 60'"
  Write-TextFile "$SnapDir/remote_env.txt" ($remoteInfo -join "`n")

  # Pre snapshots
  Write-JsonFile "$SnapDir/health_pre.json" (Invoke-RestJson "GET" "$BaseUrl/health" $null 5)
  Write-JsonFile "$SnapDir/shadow_live_pre.json" (Invoke-RestJson "GET" "$BaseUrl/report/shadow/live" $null 5)
  Try-JsonSnapshot "$SnapDir/toxicity_live_pre.json" "$BaseUrl/report/toxicity/live" | Out-Null
  Try-JsonSnapshot "$SnapDir/direction_pre.json" "$BaseUrl/report/direction" | Out-Null
  Try-JsonSnapshot "$SnapDir/router_pre.json" "$BaseUrl/report/router" | Out-Null
  Try-JsonSnapshot "$SnapDir/capital_pre.json" "$BaseUrl/report/capital" | Out-Null
  Try-TextSnapshot "$SnapDir/metrics_pre.prom" "$BaseUrl/metrics" | Out-Null

  # Safety: resume (no-op if already).
  Write-JsonFile "$SnapDir/resume_pre.json" (Invoke-RestJson "POST" "$BaseUrl/control/resume" @{} 5)
  Write-JsonFile "$SnapDir/reset_shadow_pre_sweeps.json" (Invoke-RestJson "POST" "$BaseUrl/control/reset_shadow" @{} 5)
  Start-Sleep -Seconds 5

  # Sweeps (engine series + ws probe; base_url points to remote via tunnel).
  for ($i = 1; $i -le [Math]::Max(1, $SweepRepeats); $i++) {
    if ($i -gt 1) {
      Write-Host ("reset_shadow before sweep " + $i)
      Write-JsonFile "$SnapDir/reset_shadow_before_sweep_$i.json" (Invoke-RestJson "POST" "$BaseUrl/control/reset_shadow" @{} 5)
      Start-Sleep -Seconds 5
    }
    Write-Host ("sweep=" + $i + "/" + $SweepRepeats)
    & python scripts/full_latency_sweep.py --profile $SweepProfile --base-url $BaseUrl --out-root "datasets/reports" --run-id $RunId --skip-ws --reset-shadow --warmup-sec 5
    if ($LASTEXITCODE -ne 0) { throw "full_latency_sweep failed (exit=$LASTEXITCODE)" }
    if ($i -lt $SweepRepeats) {
      Start-Sleep -Seconds 5
    }
  }
  Write-JsonFile "$SnapDir/shadow_live_after_sweeps.json" (Invoke-RestJson "GET" "$BaseUrl/report/shadow/live" $null 5)

  # Storm test (control churn + poll). Writes under runs/<run_id>/ due to --use-run-dir.
  Write-Host "storm_test"
  & python scripts/storm_test.py --base-url $BaseUrl --duration-sec $StormDurationSec --concurrency $StormConcurrency --burst-rps $StormBurstRps --out-root "datasets/reports" --run-id $RunId --use-run-dir
  if ($LASTEXITCODE -ne 0) { throw "storm_test failed (exit=$LASTEXITCODE)" }

  Write-JsonFile "$SnapDir/health_after_storm.json" (Invoke-RestJson "GET" "$BaseUrl/health" $null 5)
  Write-JsonFile "$SnapDir/shadow_live_after_storm.json" (Invoke-RestJson "GET" "$BaseUrl/report/shadow/live" $null 5)

  if ($DoThreeWindow) {
    Write-Host "three_window_verify (ab)"
    & python scripts/three_window_verify.py --base-url $BaseUrl --mode "ab" --predator-priority $PredatorPriority --out-root "datasets/reports" --run-id $RunId
    if ($LASTEXITCODE -ne 0) { throw "three_window_verify failed (exit=$LASTEXITCODE)" }
    Write-JsonFile "$SnapDir/shadow_live_after_threew.json" (Invoke-RestJson "GET" "$BaseUrl/report/shadow/live" $null 5)
    Try-JsonSnapshot "$SnapDir/pnl_by_engine_after_threew.json" "$BaseUrl/report/pnl/by_engine" | Out-Null
  }

  if ($BaselineRunDir -and (Test-Path $BaselineRunDir)) {
    Write-Host "analyze_fullchain_run (with baseline)"
    $analysisJson = "$RunDir/regression_analysis.json"
    $analysisArgs = @(
      "scripts/analyze_fullchain_run.py",
      "--run-dir", $RunDir,
      "--baseline-run-dir", $BaselineRunDir,
      "--json-out", $analysisJson
    )
    if ($RegressionGate) {
      $analysisArgs += "--fail-on-regression"
    }
    & python @analysisArgs
    if ($LASTEXITCODE -ne 0) { throw "analyze_fullchain_run failed (exit=$LASTEXITCODE)" }
  }

  # Post snapshots + safety resume
  Write-JsonFile "$SnapDir/resume_post.json" (Invoke-RestJson "POST" "$BaseUrl/control/resume" @{} 5)
  Write-JsonFile "$SnapDir/health_post.json" (Invoke-RestJson "GET" "$BaseUrl/health" $null 5)
  Try-TextSnapshot "$SnapDir/metrics_post.prom" "$BaseUrl/metrics" | Out-Null

  Write-JsonFile "$RunDir/run_meta.json" @{
    ts_utc = NowUtcIso
    run_id = $RunId
    ssh_user = $User
    ssh_host = $SshHost
    local_port = $LocalPort
    remote_port = $RemotePort
    base_url = $BaseUrl
    sweep_profile = $SweepProfile
    sweep_repeats = $SweepRepeats
    storm = @{
      duration_sec = $StormDurationSec
      concurrency = $StormConcurrency
      burst_rps = $StormBurstRps
    }
    do_three_window = [bool]$DoThreeWindow
    predator_priority = $PredatorPriority
  }

  Write-TextFile "$RunDir/README.md" @"
# Fullchain Benchmark Run

- RunId: $RunId
- Date(UTC): $Day
- BaseUrl (tunnel -> remote 127.0.0.1:$RemotePort): $BaseUrl

## Artifacts
- $RunDir/snapshots/*.json: point-in-time snapshots of /health, /report/*, etc.
- $RunDir/snapshots/*.prom: /metrics before/after.
- datasets/reports/$Day/runs/$RunId/full_latency_sweep_*.json: engine series + ws probe results.
- datasets/reports/$Day/runs/$RunId/storm_test_summary.json + storm_test_trace.jsonl: stress polling + control churn.
- datasets/reports/$Day/runs/$RunId/regression_analysis.json: optional baseline diff + likely cause attribution.

## What To Look At First
- snapshots/shadow_live_pre.json vs snapshots/shadow_live_after_storm.json
  - decision_queue_wait_p99_ms
  - decision_compute_p99_ms
  - local_backlog_p99_ms
- full_latency_sweep_*.json: engine.stats.* time series stability across repeats.
"@

  Write-Host "done"
} finally {
  if ($tunnel -and -not $tunnel.HasExited) {
    try { Stop-Process -Id $tunnel.Id -Force } catch {}
  }
}
