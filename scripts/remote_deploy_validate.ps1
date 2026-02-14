param(
    [Parameter(Mandatory = $true)]
    [string]$Host,
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
        "$User@$Host",
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
$repoSync = @"
set -e
if [ -d $RepoDir/.git ]; then
  cd $RepoDir
  git fetch origin
  git checkout $Branch
  git pull --ff-only origin $Branch
else
  git clone --branch $Branch $RepoUrl $RepoDir
fi
"@
Invoke-Remote $repoSync

Write-Host "[3/7] Ensure toolchain"
$ensureToolchain = @"
set -e
if ! command -v python3 >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y python3 python3-pip
fi
if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi
"@
Invoke-Remote $ensureToolchain

Write-Host "[4/7] Build app_runner release"
$buildCmd = @"
set -e
cd $RepoDir
source `$HOME/.cargo/env
cargo build -p app_runner --release
"@
Invoke-Remote $buildCmd

Write-Host "[5/7] Restart runtime"
$restartCmd = @"
set -e
cd $RepoDir
pkill -f target/release/app_runner >/dev/null 2>&1 || true
nohup env RUST_LOG=info ./target/release/app_runner > /tmp/polyedge_app.log 2>&1 &
echo `$! > /tmp/polyedge_app.pid
"@
Invoke-Remote $restartCmd

Write-Host "[6/7] Health wait"
$healthCmd = @"
set -e
for i in `$(seq 1 40); do
  if curl -fsS http://127.0.0.1:8080/health >/tmp/polyedge_health.json 2>/dev/null; then
    cat /tmp/polyedge_health.json
    exit 0
  fi
  sleep 2
done
echo "health check timeout"
exit 1
"@
Invoke-Remote $healthCmd

Write-Host "[7/7] Run bounded benchmark + regression"
$runId = "remote-" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$validateCmd = @"
set -e
cd $RepoDir
day=`$(date -u +%F)
mkdir -p datasets/reports/`$day
python3 scripts/e2e_latency_test.py --mode ws-first --base-url http://127.0.0.1:8080 --symbol $Symbol --seconds $BenchSeconds --json-out datasets/reports/`$day/server_full_latency_benchmark.json
python3 scripts/param_regression.py --base-url http://127.0.0.1:8080 --window-sec 300 --max-trials 8 --run-id $runId --max-runtime-sec $RegressionSeconds --heartbeat-sec 30 --fail-fast-threshold 3
python3 scripts/runtime_watchdog.py --base-url http://127.0.0.1:8080 --max-cycles 1 --max-runtime-sec 120 --heartbeat-sec 30 --fail-fast-threshold 1
python3 scripts/generate_morning_report.py --day `$day
cat datasets/reports/`$day/morning_summary.md
"@
Invoke-Remote $validateCmd

Write-Host "remote deploy+validate done for $Host"
