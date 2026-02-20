# verify_udp_flow.ps1 - Local Validation Script

Write-Host "🚀 Building binaries..."
cargo build --release -p feeder_tokyo --bin sender
cargo build --release --bin bench_feed

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed!"
    exit 1
}

$env:RUST_LOG = "info"
$env:TARGET = "127.0.0.1:7777"
$env:SYMBOL = "BTCUSDT"

Write-Host "Starting Listener (bench_feed)..."
$listener = Start-Job -ScriptBlock {
    Set-Location "e:\Projects\test"
    $env:RUST_LOG = "info"
    ./target/release/bench_feed.exe
}

Start-Sleep -Seconds 2

Write-Host "Starting Sender (feeder_tokyo)..."
$sender = Start-Job -ScriptBlock {
    Set-Location "e:\Projects\test"
    $env:RUST_LOG = "info"
    $env:TARGET = "127.0.0.1:7777"
    $env:SYMBOL = "BTCUSDT"
    ./target/release/sender.exe
}

Write-Host "⏳ Running for 15 seconds..."
Start-Sleep -Seconds 15

Write-Host "🛑 Stopping jobs..."
Stop-Job $sender
Stop-Job $listener

Write-Host "=== Sender Output ==="
Receive-Job $sender
Write-Host "====================="

Write-Host "=== Listener Output ==="
$output = Receive-Job $listener
$output
Write-Host "======================="

$matchCount = ($output | Select-String "STATS").Count
$gapCount = ($output | Select-String "UDP Gap").Count

Write-Host "📊 Summary:"
Write-Host "   - Matches Received: $matchCount"
Write-Host "   - Gaps Detected:    $gapCount"

if ($matchCount -gt 0) {
    Write-Host "✅ verification PASSED: UDP flow confirmed." -ForegroundColor Green
} else {
    Write-Error "❌ verification FAILED: No matches found."
}

Remove-Job $sender
Remove-Job $listener
