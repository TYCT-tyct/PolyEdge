#!/usr/bin/env python3
"""
PolyEdge 实时网络监控工具
显示实时网络状态和延迟统计

使用:
    # 基本监控
    python3 scripts/network_monitor.py

    # 详细监控 (显示所有统计)
    python3 scripts/network_monitor.py --detail

    # 持续运行
    python3 scripts/network_monitor.py --interval 5 --duration 300
"""

import asyncio
import socket
import struct
import time
import argparse
import statistics
import os
from datetime import datetime
from typing import List, Dict, Optional
import json


class NetworkMonitor:
    """网络监控器"""

    def __init__(self, target: str = "172.31.44.26", port: int = 6666):
        self.target = target
        self.port = port
        self.latencies: List[float] = []
        self.max_samples = 1000
        self.last_seq = 0
        self.gaps = 0
        self.total_received = 0
        self.errors = 0
        self.start_time = time.time()

    def update_latency(self, seq: int, send_ts: int):
        """更新延迟统计"""
        recv_ts = time.perf_counter_ns()
        rtt_ns = recv_ts - send_ts
        one_way_ns = rtt_ns // 2
        latency_ms = one_way_ns / 1e6

        self.latencies.append(latency_ms)
        if len(self.latencies) > self.max_samples:
            self.latencies = self.latencies[-self.max_samples:]

        # Gap 检测
        if self.last_seq > 0 and seq > self.last_seq + 1:
            self.gaps += (seq - self.last_seq - 1)
        self.last_seq = seq
        self.total_received += 1

    def get_stats(self) -> Dict:
        """获取当前统计"""
        if not self.latencies:
            return {
                "samples": 0,
                "avg_ms": 0,
                "p50_ms": 0,
                "p95_ms": 0,
                "p99_ms": 0,
                "min_ms": 0,
                "max_ms": 0,
                "jitter_ms": 0,
                "gaps": self.gaps,
                "received": self.total_received,
                "errors": self.errors,
                "uptime_sec": time.time() - self.start_time
            }

        sorted_latencies = sorted(self.latencies)
        n = len(sorted_latencies)

        jitter = 0
        if n > 1:
            diffs = [abs(sorted_latencies[i] - sorted_latencies[i-1])
                    for i in range(1, n)]
            jitter = statistics.median(diffs)

        return {
            "samples": n,
            "avg_ms": statistics.mean(self.latencies),
            "p50_ms": sorted_latencies[int(n * 0.5)],
            "p95_ms": sorted_latencies[int(n * 0.95)] if n >= 20 else sorted_latencies[-1],
            "p99_ms": sorted_latencies[int(n * 0.99)] if n >= 100 else sorted_latencies[-1],
            "min_ms": sorted_latencies[0],
            "max_ms": sorted_latencies[-1],
            "jitter_ms": jitter,
            "gaps": self.gaps,
            "received": self.total_received,
            "errors": self.errors,
            "uptime_sec": time.time() - self.start_time
        }

    def print_stats(self, detail: bool = False):
        """打印统计信息"""
        stats = self.get_stats()

        # 清除屏幕
        os.system('cls' if os.name == 'nt' else 'clear')

        print(f"\n{'='*70}")
        print(f"📡 PolyEdge 网络监控")
        print(f"{'='*70}")
        print(f"目标: {self.target}:{self.port}")
        print(f"运行时间: {stats['uptime_sec']:.0f}s")
        print(f"{'='*70}")

        # 延迟统计
        print(f"\n⏱️  延迟统计 (最近 {stats['samples']} 样本)")
        print(f"   最小:     {stats['min_ms']:>8.3f} ms")
        print(f"   最大:     {stats['max_ms']:>8.3f} ms")
        print(f"   平均:     {stats['avg_ms']:>8.3f} ms")
        print(f"   P50:      {stats['p50_ms']:>8.3f} ms")
        print(f"   P95:      {stats['p95_ms']:>8.3f} ms")
        print(f"   P99:      {stats['p99_ms']:>8.3f} ms")
        print(f"   抖动:     {stats['jitter_ms']:>8.3f} ms")

        # 网络质量
        print(f"\n📶 网络质量")
        loss_rate = (stats['gaps'] / stats['received'] * 100) if stats['received'] > 0 else 0
        print(f"   接收消息: {stats['received']:,}")
        print(f"   Gap 数:   {stats['gaps']:,} ({loss_rate:.4f}%)")
        print(f"   错误数:   {stats['errors']:,}")

        # 吞吐量估算
        if stats['uptime_sec'] > 0:
            msg_rate = stats['received'] / stats['uptime_sec']
            print(f"   消息率:   {msg_rate:,.0f} msg/s")

        if detail:
            # 详细统计
            print(f"\n📊 详细统计")
            print(f"   样本缓冲区: {len(self.latencies)}/{self.max_samples}")

            # 最近 10 个延迟
            recent = self.latencies[-10:] if self.latencies else []
            if recent:
                print(f"   最近延迟: {[f'{x:.2f}' for x in recent]}")

            # 延迟分布
            if stats['samples'] > 0:
                ranges = [
                    (0, 1, "0-1ms"),
                    (1, 2, "1-2ms"),
                    (2, 5, "2-5ms"),
                    (5, 10, "5-10ms"),
                    (10, 100, "10ms+")
                ]
                print(f"   延迟分布:")
                for low, high, label in ranges:
                    count = sum(1 for x in self.latencies if low <= x < high)
                    pct = count / len(self.latencies) * 100
                    bar = "█" * int(pct / 2)
                    print(f"      {label:>8}: {count:>5} ({pct:>5.1f}%) {bar}")

        print(f"\n{'='*70}")

        return stats


async def ping_target(target: str, port: int) -> Optional[float]:
    """Ping 目标获取 RTT"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1)

        send_ts = time.perf_counter_ns()
        sock.sendto(b"ping", (target, port))
        sock.recvfrom(1024)
        recv_ts = time.perf_counter_ns()

        sock.close()
        return (recv_ts - send_ts) / 1e6  # ms

    except Exception:
        return None


async def monitor_loop(target: str, port: int, interval: int, detail: bool):
    """监控循环"""
    monitor = NetworkMonitor(target, port)

    # 创建 UDP 监听 (简单 echo 响应测试)
    print("启动监控...")
    print(f"目标: {target}:{port}")

    # 启动 ping 任务
    ping_latency = 0

    async def ping_task():
        nonlocal ping_latency
        while True:
            rtt = await ping_target(target, port)
            if rtt:
                ping_latency = rtt
            await asyncio.sleep(interval)

    # 启动 ping
    ping_task_handle = asyncio.create_task(ping_task())

    try:
        while True:
            # 打印统计
            stats = monitor.print_stats(detail)

            # 打印 ping 结果
            if ping_latency > 0:
                print(f"   TCP RTT:   {ping_latency:>8.3f} ms")

            # 状态评估
            print(f"\n🔍 状态评估:")
            if stats['samples'] > 0:
                if stats['p99_ms'] < 5:
                    print("   ✅ 优秀 - P99 < 5ms")
                elif stats['p99_ms'] < 10:
                    print("   ✅ 良好 - P99 < 10ms")
                elif stats['p99_ms'] < 20:
                    print("   ⚠️ 一般 - P99 < 20ms")
                else:
                    print("   ❌ 较差 - P99 > 20ms")

                if stats['jitter_ms'] < 1:
                    print("   ✅ 低抖动")
                elif stats['jitter_ms'] < 3:
                    print("   ⚠️ 中等抖动")
                else:
                    print("   ❌ 高抖动")

                loss_rate = (stats['gaps'] / stats['received'] * 100) if stats['received'] > 0 else 0
                if loss_rate < 0.01:
                    print("   ✅ 无丢包")
                elif loss_rate < 0.1:
                    print("   ⚠️ 轻微丢包")
                else:
                    print("   ❌ 严重丢包")

            await asyncio.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n停止监控...")
        ping_task_handle.cancel()


async def quick_test(target: str, port: int, count: int = 100):
    """快速延迟测试"""
    print(f"\n{'='*60}")
    print(f"⚡ 快速延迟测试: {target}:{port} ({count} 次)")
    print(f"{'='*60}")

    latencies = []

    for i in range(count):
        rtt = await ping_target(target, port)
        if rtt:
            latencies.append(rtt)
            print(f"\r进度: {i+1}/{count}", end="", flush=True)
        await asyncio.sleep(0.1)

    print(f"\n\n完成! 有效样本: {len(latencies)}/{count}")

    if latencies:
        latencies.sort()
        n = len(latencies)
        print(f"\n📊 延迟统计:")
        print(f"   最小:  {min(latencies):.3f} ms")
        print(f"   最大:  {max(latencies):.3f} ms")
        print(f"   平均:  {statistics.mean(latencies):.3f} ms")
        print(f"   P50:   {latencies[int(n*0.5)]:.3f} ms")
        print(f"   P95:   {latencies[int(n*0.95)]:.3f} ms")
        print(f"   P99:   {latencies[int(n*0.99)]:.3f} ms")


async def main():
    parser = argparse.ArgumentParser(description="PolyEdge 网络监控")
    parser.add_argument("--target", default="172.31.44.26", help="目标地址 (东京)")
    parser.add_argument("--port", type=int, default=6666, help="目标端口")
    parser.add_argument("--interval", type=int, default=3, help="刷新间隔(秒)")
    parser.add_argument("--duration", type=int, help="运行持续时间(秒)")
    parser.add_argument("--detail", action="store_true", help="显示详细信息")
    parser.add_argument("--quick", action="store_true", help="快速测试模式")

    args = parser.parse_args()

    if args.quick:
        await quick_test(args.target, args.port)
    else:
        await monitor_loop(args.target, args.port, args.interval, args.detail)


if __name__ == "__main__":
    asyncio.run(main())
