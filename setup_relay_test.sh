#!/bin/bash
set -e

# PolyEdge Relay Setup Script
# Usage: ./setup_relay_test.sh [sender|receiver] [target_ip]

ROLE=$1
TARGET_IP=${2:-"10.0.3.123"} # Default to Ireland Private IP
SYMBOL=${3:-"btcusdt"}

echo "🚀 PolyEdge Setup: Role=$ROLE, Target=$TARGET_IP"

# 1. Update Codebase
if [ -d "PolyEdge" ]; then
    echo "📂 Updating PolyEdge..."
    cd PolyEdge
    git fetch origin
    git checkout feat/p1-wire-dual
    git pull origin feat/p1-wire-dual
else
    echo "📂 Cloning PolyEdge..."
    git clone https://github.com/TYCT-tyct/PolyEdge.git
    cd PolyEdge
    git checkout feat/p1-wire-dual
fi

# 2. Build Rust Binaries
source "$HOME/.cargo/env" || true
echo "🛠️ Building Release Binaries..."

if [ "$ROLE" == "sender" ]; then
    cargo build --release --bin feeder_tokyo
    echo "✅ Sender Built."

    echo "🏃 Starting Feeder (Tokyo)..."
    export TARGET="${TARGET_IP}:6666"
    export SYMBOL="$SYMBOL"
    pkill -f feeder_tokyo || true
    nohup ./target/release/feeder_tokyo > feeder.log 2>&1 &
    echo "🔥 Sender running! PID: $!"
    echo "See logs: tail -f PolyEdge/feeder.log"

elif [ "$ROLE" == "receiver" ]; then
    cargo build --release --bin bench_feed
    echo "✅ Receiver Built."

    echo "🎧 Starting Receiver (Ireland)..."
    pkill -f bench_feed || true
    nohup ./target/release/bench_feed > latency_report.csv 2>&1 &
    echo "🔥 Receiver running! PID: $!"
    echo "See logs: tail -f PolyEdge/latency_report.csv"

else
    echo "❌ Unknown Role. Use 'sender' or 'receiver'."
    exit 1
fi

