@echo off
setlocal
cargo run -p polyedge_cli --bin polyedge -- %*
endlocal
