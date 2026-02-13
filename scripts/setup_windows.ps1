$ErrorActionPreference = "Stop"

Write-Host "Installing Rust toolchain..."
Invoke-WebRequest -Uri "https://win.rustup.rs/x86_64" -OutFile "rustup-init.exe"
Start-Process -FilePath ".\rustup-init.exe" -ArgumentList "-y --profile minimal --default-toolchain stable" -Wait -NoNewWindow
Remove-Item -Force ".\rustup-init.exe"

Write-Host "Installing Visual Studio C++ build tools (required for link.exe + Windows SDK)..."
Invoke-WebRequest -Uri "https://aka.ms/vs/17/release/vs_BuildTools.exe" -OutFile "vs_BuildTools.exe"
Start-Process -FilePath ".\vs_BuildTools.exe" -ArgumentList "--quiet --wait --norestart --nocache --installPath C:\BuildTools --add Microsoft.VisualStudio.Workload.VCTools --add Microsoft.VisualStudio.Component.Windows11SDK.22621" -Wait -NoNewWindow
Remove-Item -Force ".\vs_BuildTools.exe"

$env:PATH = "$env:USERPROFILE\.cargo\bin;" + $env:PATH
cargo --version
rustc --version

Write-Host "Toolchain install complete. Re-open shell and run: cargo check --workspace"
