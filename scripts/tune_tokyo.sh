#!/usr/bin/env bash
# =============================================================================
# tune_tokyo.sh — 东京服务器 (57.180.89.145) UDP 发送器调优
# =============================================================================
# 用法: ssh ubuntu@57.180.89.145 'bash -s' < scripts/tune_tokyo.sh
# 效果: 设置 UDP 冗余发送 + 缓冲区 + CPU 亲和性 + 进程优先级
# =============================================================================
set -euo pipefail

# -----------------------------------------------------------------------
# F2-1: 网络栈调优 — UDP 大缓冲区 + 减少丢包
# -----------------------------------------------------------------------
SYSCTL_CONF=/etc/sysctl.d/99-polyedge-tokyo.conf

cat > "$SYSCTL_CONF" << 'EOF'
# PolyEdge 网络栈调优 — 东京 UDP 发送节点

# UDP 接收/发送缓冲区 (4MB) — 防止 UDP 丢包
net.core.rmem_max = 4194304
net.core.wmem_max = 4194304
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576

# UDP socket 缓冲区
net.ipv4.udp_rmem_min = 65536
net.ipv4.udp_wmem_min = 65536

# 增大 socket 队列深度（防止 UDP 丢包）
net.core.netdev_max_backlog = 65536

# Busy poll — 减少 UDP 发送延迟
net.core.busy_read = 50
net.core.busy_poll = 50

# 禁用 TCP 时间戳（减少 CPU 开销）
net.ipv4.tcp_timestamps = 0

# 启用 BBR
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
EOF

sysctl -p "$SYSCTL_CONF"
echo "[F2] sysctl 调优完成"

# 验证 UDP 缓冲区
WME=$(sysctl -n net.core.wmem_max)
echo "[F2] net.core.wmem_max = $WME"

# -----------------------------------------------------------------------
# F2-2: 环境变量持久化 — UDP 冗余发送 + 缓冲区配置
# -----------------------------------------------------------------------
ENV_FILE=/etc/polyedge/env.conf
mkdir -p /etc/polyedge

cat > "$ENV_FILE" << 'EOF'
# PolyEdge 东京 UDP 发送器配置

# UDP 冗余发送 — 每个数据包发送 2 次，减少丢包概率
POLYEDGE_UDP_REDUNDANCY=2

# UDP 发送缓冲区 4MB
POLYEDGE_UDP_SNDBUF_BYTES=4194304

# UDP 发送器绑定到核心 2（与接收器分离）
POLYEDGE_SENDER_PIN_CORE=2

# 东京 → 爱尔兰 UDP 目标（爱尔兰服务器 IP）
POLYEDGE_UDP_TARGET_HOST=54.77.232.166
EOF

echo "[F2] 环境变量写入 $ENV_FILE"

# -----------------------------------------------------------------------
# F2-3: 进程实时优先级
# -----------------------------------------------------------------------
POLYEDGE_PID=$(pgrep -f "polyedge\|feeder_tokyo\|feed_udp" | head -1 || true)

if [ -n "$POLYEDGE_PID" ]; then
    chrt -f -p 90 "$POLYEDGE_PID"
    echo "[F2] ✅ polyedge PID=$POLYEDGE_PID 设置为 SCHED_FIFO priority=90"
else
    echo "[F2] ℹ️  polyedge 进程未运行，跳过实时优先级设置"
    echo "[F2]    启动后执行: chrt -f -p 90 \$(pgrep -f feeder_tokyo)"
fi

# -----------------------------------------------------------------------
# F2-4: UDP 发送器 CPU 亲和性 — 绑定到核心 2
# -----------------------------------------------------------------------
if [ -n "$POLYEDGE_PID" ]; then
    taskset -cp 2 "$POLYEDGE_PID"
    echo "[F2] ✅ UDP 发送器绑定到 CPU 2"
fi

# -----------------------------------------------------------------------
# F2-5: CPU governor
# -----------------------------------------------------------------------
if command -v cpupower &>/dev/null; then
    cpupower frequency-set -g performance
    echo "[F2] ✅ CPU governor 设置为 performance"
elif [ -d /sys/devices/system/cpu/cpu0/cpufreq ]; then
    for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
        echo performance > "$cpu" 2>/dev/null || true
    done
    echo "[F2] ✅ CPU governor 设置为 performance (via sysfs)"
fi

# -----------------------------------------------------------------------
# F2-6: 验证 UDP 冗余设置
# -----------------------------------------------------------------------
echo ""
echo "============================================================"
echo "[F2] 东京服务器调优完成"
echo ""
echo "     重启 feeder_tokyo 时加载环境变量:"
echo "     source /etc/polyedge/env.conf && ./feeder_tokyo"
echo ""
echo "     验证冗余发送 (启动后查看日志):"
echo "     journalctl -u feeder_tokyo | grep 'redundancy'"
echo "============================================================"
