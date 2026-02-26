param(
  [string]$RepoRoot = "E:\Projects\test"
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

Write-Host "[polyedge-cli] building release binary..."
cargo build -p polyedge_cli --release

$targetBin = Join-Path $RepoRoot "target\release"
$cargoBin = Join-Path $env:USERPROFILE ".cargo\bin"
New-Item -ItemType Directory -Force -Path $cargoBin | Out-Null

Copy-Item (Join-Path $targetBin "polyedge.exe") (Join-Path $cargoBin "polyedge.exe") -Force
Copy-Item (Join-Path $targetBin "polyedge.exe") (Join-Path $cargoBin "poly.exe") -Force

Write-Host "[polyedge-cli] installed:"
Write-Host "  $cargoBin\polyedge.exe"
Write-Host "  $cargoBin\poly.exe"
Write-Host "[polyedge-cli] try:"
Write-Host "  polyedge --help"
Write-Host "  poly --help"
