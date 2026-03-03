param(
  [Parameter(Mandatory = $true)]
  [string]$JobsFile,
  [Parameter(Mandatory = $true)]
  [string]$OutputDir,
  [int]$PollSeconds = 15,
  [int]$MaxMinutes = 240
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $JobsFile)) {
  throw "Jobs file not found: $JobsFile"
}
if (!(Test-Path $OutputDir)) {
  New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

$jobs = Get-Content $JobsFile -Raw | ConvertFrom-Json
$endAt = (Get-Date).AddMinutes($MaxMinutes)
$statusFile = Join-Path $OutputDir "status_table.json"
$summaryFile = Join-Path $OutputDir "summary.json"
$logFile = Join-Path $OutputDir "watch.log"

function Write-Log([string]$line) {
  $ts = (Get-Date).ToString("s")
  Add-Content -Path $logFile -Value "[$ts] $line"
}

Write-Log "watch started, jobs=$($jobs.Count), poll=${PollSeconds}s, maxMinutes=$MaxMinutes"

while ((Get-Date) -lt $endAt) {
  $rows = @()
  $done = 0
  foreach ($j in $jobs) {
    $jobId = $j.job_id
    $uri = "http://127.0.0.1:9820/api/hpo/jobs/$jobId"
    try {
      $r = Invoke-RestMethod -Uri $uri -Method Get -TimeoutSec 30
    } catch {
      $rows += [PSCustomObject]@{
        symbol      = $j.symbol
        market_type = $j.market_type
        job_id      = $jobId
        status      = "unreachable"
        iteration   = 0
        total       = 0
        best_score  = $null
      }
      continue
    }
    $rows += [PSCustomObject]@{
      symbol      = $j.symbol
      market_type = $j.market_type
      job_id      = $jobId
      status      = $r.status
      iteration   = $r.progress.iteration
      total       = $r.progress.total_iterations
      best_score  = $r.progress.best_score
    }
    ($r | ConvertTo-Json -Depth 18) | Set-Content (Join-Path $OutputDir "job_$jobId.json") -Encoding UTF8
    if ($r.status -eq "completed" -or $r.status -eq "failed") {
      $done++
    }
  }

  ($rows | ConvertTo-Json -Depth 8) | Set-Content $statusFile -Encoding UTF8
  Write-Log "tick done=$done/$($jobs.Count)"

  if ($done -eq $jobs.Count) {
    $summary = [PSCustomObject]@{
      finished_at = (Get-Date).ToString("s")
      done        = $done
      total       = $jobs.Count
      status_file = $statusFile
      output_dir  = $OutputDir
    }
    ($summary | ConvertTo-Json -Depth 6) | Set-Content $summaryFile -Encoding UTF8
    Write-Log "all jobs completed"
    exit 0
  }

  Start-Sleep -Seconds $PollSeconds
}

$timeoutSummary = [PSCustomObject]@{
  finished_at = (Get-Date).ToString("s")
  done        = ($rows | Where-Object { $_.status -eq "completed" -or $_.status -eq "failed" }).Count
  total       = $jobs.Count
  timeout     = $true
  status_file = $statusFile
  output_dir  = $OutputDir
}
($timeoutSummary | ConvertTo-Json -Depth 6) | Set-Content $summaryFile -Encoding UTF8
Write-Log "watch timeout"
exit 2

