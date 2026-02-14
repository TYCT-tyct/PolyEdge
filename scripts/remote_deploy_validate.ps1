param(
    [Parameter(Mandatory = $true)]
    [string]$RemoteHost,
    [string]$User = "ubuntu",
    [string]$KeyPath = "C:\Users\Shini\Documents\test.pem",
    [string]$RepoUrl = "https://github.com/TYCT-tyct/PolyEdge.git",
    [string]$RepoDir = "~/PolyEdge",
    [string]$Branch = "main",
    [int]$BenchSeconds = 180,
    [int]$RegressionSeconds = 1800,
    [string]$Symbol = "BTCUSDT"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Remote {
    param([Parameter(Mandatory = $true)][string]$Command)
    $sshArgs = @(
        "-F", "NUL",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=12",
        "-i", $KeyPath,
        "$User@$RemoteHost",
        $Command
    )
    & ssh @sshArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Remote command failed: $Command"
    }
}

Write-Host "[1/7] SSH preflight"
Invoke-Remote "echo connected && uname -a && date -u +%Y-%m-%dT%H:%M:%SZ"

Write-Host "[2/7] Ensure repository is up to date"
$repoSyncTemplate = @'
set -e; if [ -d __REPO_DIR__/.git ]; then cd __REPO_DIR__ && git fetch origin && git checkout __BRANCH__ && git pull --ff-only origin __BRANCH__; else git clone --branch __BRANCH__ __REPO_URL__ __REPO_DIR__; fi
'@
$repoSync = $repoSyncTemplate.Replace("__REPO_DIR__", $RepoDir).Replace("__BRANCH__", $Branch).Replace("__REPO_URL__", $RepoUrl)
Invoke-Remote $repoSync

Write-Host "[3/7] Ensure toolchain"
$ensureToolchain = @'
set -e; if ! command -v python3 >/dev/null 2>&1; then sudo apt-get update -y && sudo apt-get install -y python3 python3-pip; fi; if ! command -v cargo >/dev/null 2>&1 && [ ! -x ~/.cargo/bin/cargo ]; then curl https://sh.rustup.rs -sSf | sh -s -- -y; fi
'@
Invoke-Remote $ensureToolchain

Write-Host "[4/7] Build app_runner release"
$buildTemplate = @'
set -e; cd __REPO_DIR__ && source ~/.cargo/env && cargo build -p app_runner --release
'@
$buildCmd = $buildTemplate.Replace("__REPO_DIR__", $RepoDir)
Invoke-Remote $buildCmd

Write-Host "[5/7] Restart runtime"
$restartTemplate = @'
cd __REPO_DIR__ || exit 1; pkill -x app_runner >/dev/null 2>&1 || true; nohup env RUST_LOG=info ./target/release/app_runner >/tmp/polyedge_app.log 2>&1 & true
'@
$restartCmd = $restartTemplate.Replace("__REPO_DIR__", $RepoDir)
Invoke-Remote $restartCmd

Write-Host "[6/7] Health wait"
$healthCmd = @'
set -e; for i in $(seq 1 40); do if curl -fsS http://127.0.0.1:8080/health >/tmp/polyedge_health.json 2>/dev/null; then cat /tmp/polyedge_health.json; exit 0; fi; sleep 2; done; echo "health check timeout"; exit 1
'@
Invoke-Remote $healthCmd

Write-Host "[7/7] Run bounded benchmark + regression"
$runId = "remote-" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$benchTimeout = [Math]::Max(120, $BenchSeconds + 120)
$regTimeout = [Math]::Max(300, $RegressionSeconds + 300)
$validateTemplate = @'
set -e; cd __REPO_DIR__; day=$(date -u +%F); mkdir -p datasets/reports/$day; timeout __BENCH_TIMEOUT__s python3 scripts/e2e_latency_test.py --mode ws-first --base-url http://127.0.0.1:8080 --symbol __SYMBOL__ --seconds __BENCH_SECONDS__ --json-out datasets/reports/$day/server_full_latency_benchmark.json; timeout __REG_TIMEOUT__s python3 scripts/param_regression.py --base-url http://127.0.0.1:8080 --window-sec 300 --max-trials 8 --run-id __RUN_ID__ --max-runtime-sec __REG_SECONDS__ --heartbeat-sec 30 --fail-fast-threshold 3; python3 scripts/runtime_watchdog.py --base-url http://127.0.0.1:8080 --max-cycles 1 --max-runtime-sec 120 --heartbeat-sec 30 --fail-fast-threshold 1; python3 scripts/generate_morning_report.py --day $day; cat datasets/reports/$day/morning_summary.md
'@
$validateCmd = $validateTemplate.Replace("__REPO_DIR__", $RepoDir).Replace("__SYMBOL__", $Symbol).Replace("__BENCH_SECONDS__", [string]$BenchSeconds).Replace("__RUN_ID__", $runId).Replace("__REG_SECONDS__", [string]$RegressionSeconds).Replace("__BENCH_TIMEOUT__", [string]$benchTimeout).Replace("__REG_TIMEOUT__", [string]$regTimeout)
Invoke-Remote $validateCmd

Write-Host "remote deploy+validate done for $RemoteHost"
