param(
    [Parameter(Mandatory = $true)]
    [string]$SshHost,

    [Parameter(Mandatory = $true)]
    [string]$KeyPath,

    [string]$User = "ubuntu",
    [int]$LocalPort = 18080,
    [int]$RemotePort = 8080,

    [ValidateSet("quick", "standard", "deep")]
    [string]$Profile = "standard",

    [string]$RunIdPrefix = "optimizer",
    [string]$StateFile = "datasets/reports/champion_state.json",
    [string]$StopFile = "datasets/reports/STOP_OPTIMIZER",
    [string]$OutRoot = "datasets/reports",

    [int]$MaxRuntimeSec = 0,
    [int]$CycleSleepSec = 2,
    [int]$MinOutcomes = 30,
    [double]$MinObjectiveDelta = 0.0005,
    [double]$HttpTimeoutSec = 8.0,
    [int]$MaxSampleReadFailures = 4,
    [bool]$RestartOnFailure = $true,
    [int]$RestartDelaySec = 5,
    [int]$MaxRestarts = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Wait-HttpOk([string]$Url, [int]$Attempts = 60, [int]$SleepMs = 250) {
    for ($i = 0; $i -lt $Attempts; $i++) {
        try {
            $resp = Invoke-WebRequest -Method Get -Uri $Url -TimeoutSec 2
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
                return
            }
        } catch {
        }
        Start-Sleep -Milliseconds $SleepMs
    }
    throw "endpoint not reachable: $Url"
}

$baseUrl = "http://127.0.0.1:$LocalPort"

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
    Write-Host "starting ssh tunnel -> $SshHost ($LocalPort -> 127.0.0.1:$RemotePort)"
    $tunnel = Start-Process -FilePath "ssh" -ArgumentList $sshArgs -NoNewWindow -PassThru
    Wait-HttpOk "$baseUrl/health" 80 250

    Write-Host "base_url=$baseUrl"
    Write-Host "state_file=$StateFile"
    Write-Host "stop_file=$StopFile"
    Write-Host "restart_on_failure=$RestartOnFailure restart_delay_sec=$RestartDelaySec max_restarts=$MaxRestarts"

    $deadlineUtc = $null
    if ($MaxRuntimeSec -gt 0) {
        $deadlineUtc = (Get-Date).ToUniversalTime().AddSeconds($MaxRuntimeSec)
    }

    $restarts = 0
    while ($true) {
        if (Test-Path $StopFile) {
            Write-Host "stop file detected before launch: $StopFile"
            break
        }

        $runId = "$RunIdPrefix-" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") + "-r$restarts"
        $args = @(
            "scripts/long_regression_orchestrator.py",
            "--profile", $Profile,
            "--continuous",
            "--base-url", $baseUrl,
            "--run-id", $runId,
            "--out-root", $OutRoot,
            "--state-file", $StateFile,
            "--stop-file", $StopFile,
            "--cycle-sleep-sec", "$CycleSleepSec",
            "--min-outcomes", "$MinOutcomes",
            "--min-objective-delta", "$MinObjectiveDelta",
            "--http-timeout-sec", "$HttpTimeoutSec",
            "--max-sample-read-failures", "$MaxSampleReadFailures"
        )

        if ($deadlineUtc -ne $null) {
            $remaining = [int][Math]::Floor(($deadlineUtc - (Get-Date).ToUniversalTime()).TotalSeconds)
            if ($remaining -le 0) {
                Write-Host "global max runtime reached"
                break
            }
            $args += @("--max-runtime-sec", "$remaining")
        }

        Write-Host "run_id=$runId"
        & python @args
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            break
        }
        if (-not $RestartOnFailure) {
            throw "long_regression_orchestrator failed (exit=$exitCode)"
        }
        if (Test-Path $StopFile) {
            Write-Host "stop file detected after failure: $StopFile"
            break
        }
        $restarts += 1
        if ($MaxRestarts -gt 0 -and $restarts -gt $MaxRestarts) {
            throw "long_regression_orchestrator failed too many times; restarts=$restarts"
        }
        Write-Host "orchestrator failed (exit=$exitCode), restarting in $RestartDelaySec sec..."
        Start-Sleep -Seconds ([Math]::Max(1, $RestartDelaySec))
    }
} finally {
    if ($tunnel -and -not $tunnel.HasExited) {
        try {
            Stop-Process -Id $tunnel.Id -Force
        } catch {
        }
    }
}
