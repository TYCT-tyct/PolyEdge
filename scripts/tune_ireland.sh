#!/usr/bin/env bash
# =============================================================================
# tune_ireland.sh — 爱尔兰服务器 (54.77.232.166) 网络栈 + 进程调优
# =============================================================================
# 用法: ssh ubuntu@54.77.232.166 'bash -s' < scripts/tune_ireland.sh
# 效果: 持久化到 /etc/sysctl.d/99-polyedge.conf，重启后生效
# =============================================================================
set -euo pipefail

# -----------------------------------------------------------------------
# F1-1: 网络栈调优 — 减少 TCP 缓冲区延迟 + 启用 busy_poll
# -----------------------------------------------------------------------
SYSCTL_CONF=/etc/sysctl.d/99-polyedge.conf

cat > "$SYSCTL_CONF" << 'EOF'
# PolyEdge 网络栈调优 — 爱尔兰节点

# TCP 接收/发送缓冲区 (8MB)
net.core.rmem_max = 8388608
net.core.wmem_max = 8388608
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
net.ipv4.tcp_rmem = 4096 1048576 8388608
net.ipv4.tcp_wmem = 4096 1048576 8388608

# Busy poll — 减少 epoll 唤醒延迟（50μs）
# 适用于低延迟 WebSocket 连接（Polymarket CLOB）
net.core.busy_read = 50
net.core.busy_poll = 50

# 增大 socket 队列深度
net.core.netdev_max_backlog = 65536
net.core.somaxconn = 65536

# TCP 快速重传 + 低超时
net.ipv4.tcp_syn_retries = 2
net.ipv4.tcp_synack_retries = 2
net.ipv4.tcp_fin_timeout = 15

# 禁用 TCP 时间戳（减少 CPU 开销）
net.ipv4.tcp_timestamps = 0

# 启用 TCP BBR 拥塞控制（低延迟场景更优）
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
EOF

# 立即应用
sysctl -p "$SYSCTL_CONF"
echo "[F1] sysctl 调优完成"

# 验证 busy_poll
BUSY_READ=$(sysctl -n net.core.busy_read)
if [ "$BUSY_READ" = "50" ]; then
    echo "[F1] ✅ net.core.busy_read = 50"
else
    echo "[F1] ⚠️  net.core.busy_read = $BUSY_READ (期望 50)"
fi

# -----------------------------------------------------------------------
# F1-2: 进程实时优先级 — 给 polyedge 进程设置 SCHED_FIFO
# -----------------------------------------------------------------------
POLYEDGE_PID=$(pgrep -f "polyedge\|app_runner" | head -1 || true)

if [ -n "$POLYEDGE_PID" ]; then
    chrt -f -p 90 "$POLYEDGE_PID"
    echo "[F1] ✅ polyedge PID=$POLYEDGE_PID 设置为 SCHED_FIFO priority=90"
else
    echo "[F1] ℹ️  polyedge 进程未运行，跳过实时优先级设置"
    echo "[F1]    启动后执行: chrt -f -p 90 \$(pgrep -f polyedge)"
fi

# -----------------------------------------------------------------------
# F1-3: CPU 亲和性 — 将 polyedge 绑定到核心 0-3（避免 NUMA 跨节点）
# -----------------------------------------------------------------------
if [ -n "$POLYEDGE_PID" ]; then
    taskset -cp 0-3 "$POLYEDGE_PID"
    echo "[F1] ✅ polyedge 绑定到 CPU 0-3"
fi

# -----------------------------------------------------------------------
# F1-4: 禁用 CPU 频率调节（保持最高频率）
# -----------------------------------------------------------------------
if command -v cpupower &>/dev/null; then
    cpupower frequency-set -g performance || true
    echo "[F1] ✅ CPU governor 设置为 performance"
elif [ -d /sys/devices/system/cpu/cpu0/cpufreq ]; then
    for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
        echo performance > "$cpu" 2>/dev/null || true
    done
    echo "[F1] ✅ CPU governor 设置为 performance (via sysfs)"
fi

# -----------------------------------------------------------------------
# F1-5: IRQ 亲和性 — 将网卡中断绑定到核心 4-7（与 polyedge 分离）
# -----------------------------------------------------------------------
NIC=$(ip route | grep default | awk '{print $5}' | head -1)
if [ -n "$NIC" ]; then
    for IRQ_FILE in /proc/irq/*/smp_affinity_list; do
        IRQ_DIR=$(dirname "$IRQ_FILE")
        IRQ_NAME=$(cat "$IRQ_DIR/actions" 2>/dev/null || true)
        if echo "$IRQ_NAME" | grep -q "$NIC"; then
            echo "4-7" > "$IRQ_FILE" 2>/dev/null || true
        fi
    done
    echo "[F1] ✅ NIC=$NIC IRQ 亲和性设置为核心 4-7"
fi

echo ""
echo "============================================================"
echo "[F1] 爱尔兰服务器调优完成"
echo "     重启后 sysctl 设置自动生效"
echo "     polyedge 重启后需重新执行 chrt + taskset"
echo "============================================================"
