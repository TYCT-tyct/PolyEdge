#!/bin/bash
set -e

echo "Starting Instance A (Sniper)"
export POLYEDGE_CONTROL_PORT=8001
export POLYEDGE_UDP_PORT=6666
export POLYEDGE_UDP_TRIGGER_PORT=6666
export POLYEDGE_STRATEGY_CONFIG=configs/strategy.toml
export POLYEDGE_PAPER_CAPITAL=10
export POLYEDGE_PAPER_SQLITE=paper_a.sqlite
nohup ./target/release/app_runner > paper_a.log 2>&1 &

echo "Starting Instance B (Kamikaze)"
export POLYEDGE_CONTROL_PORT=8002
export POLYEDGE_UDP_PORT=6667
export POLYEDGE_UDP_TRIGGER_PORT=6667
export POLYEDGE_STRATEGY_CONFIG=configs/strategy_kamikaze.toml
export POLYEDGE_PAPER_CAPITAL=10
export POLYEDGE_PAPER_SQLITE=paper_b.sqlite
nohup ./target/release/app_runner > paper_b.log 2>&1 &

echo "Starting Instance C (Maker)"
export POLYEDGE_CONTROL_PORT=8003
export POLYEDGE_UDP_PORT=6668
export POLYEDGE_UDP_TRIGGER_PORT=6668
export POLYEDGE_STRATEGY_CONFIG=configs/strategy_maker.toml
export POLYEDGE_PAPER_CAPITAL=10
export POLYEDGE_PAPER_SQLITE=paper_c.sqlite
nohup ./target/release/app_runner > paper_c.log 2>&1 &

echo "All 3 paper instances started."
