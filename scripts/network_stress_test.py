#!/usr/bin/env python3
"""
PolyEdge 网络压测工具
用于测试东京 -> 爱尔兰 VPC 网络性能

功能:
1. UDP 延迟测试 (RTT, 单程)
2. 吞吐量测试 (消息/秒)
3. 丢包率测试
4. 抖动测试
5. 对比公网直连

使用:
    # 基本延迟测试
    python3 scripts/network_stress_test.py --mode latency

    # 吞吐量测试
    python3 scripts/network_stress_test.py --mode throughput --target 10.0.3.123 --port 6666

    # 丢包率测试
    python3 scripts/network_stress_test.py --mode packet-loss --target 10.0.3.123 --port 6666

    # 综合压测
    python3 scripts/network_stress_test.py --mode stress --target 10.0.3.123 --port 6666 --duration 60
"""

import asyncio
import socket
import struct
import time
import argparse
import statistics
import json
import random
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional
import numpy as np

PROBE_FMT = "!QQ"
PROBE_SIZE = struct.calcsize(PROBE_FMT)


# ============================================================
# 数据结构
# ============================================================

@dataclass
class LatencyResult:
    """延迟测试结果"""
    min_ms: float
    max_ms: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    std_ms: float
    jitter_ms: float
    samples: int


@dataclass
class ThroughputResult:
    """吞吐量测试结果"""
    total_msgs: int
    total_bytes: int
    duration_sec: float
    msgs_per_sec: float
    mbps: float


@dataclass
class PacketLossResult:
    """丢包测试结果"""
    sent: int
    received: int
    lost: int
    loss_rate_percent: float
    avg_rtt_ms: float


@dataclass
class StressResult:
    """压力测试结果"""
    duration_sec: float
    total_packets: int
    total_bytes: int
    latency: LatencyResult
    packet_loss: PacketLossResult
    errors: int


# ============================================================
# UDP 延迟测试
# ============================================================

async def test_latency_udp(target: str, port: int, duration_sec: int = 30,
                          rate_hz: int = 100) -> LatencyResult:
    """
    UDP 延迟测试
    发送带时间戳的探测包，测量 RTT
    """
    print(f"\n{'='*60}")
    print(f"🔬 UDP 延迟测试: {target}:{port}")
    print(f"   持续: {duration_sec}s, 频率: {rate_hz}Hz")
    print(f"{'='*60}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)

    latencies = []
    errors = 0
    start_time = time.time()

    # 探测包格式: seq(8) + send_ts_nanos(8)
    seq = 0
    packet_interval = 1.0 / rate_hz

    try:
        while time.time() - start_time < duration_sec:
            loop_start = time.perf_counter()

            try:
                seq += 1
                send_ts = time.perf_counter_ns()

                # 打包: 序列号 + 发送时间
                payload = struct.pack(PROBE_FMT, seq, send_ts)
                sock.sendto(payload, (target, port))

                # 等待响应 (简单 echo)
                try:
                    resp, _ = sock.recvfrom(1024)
                    recv_ts = time.perf_counter_ns()

                    if len(resp) >= PROBE_SIZE:
                        resp_seq, resp_ts = struct.unpack(PROBE_FMT, resp[:PROBE_SIZE])
                        if resp_seq == seq:
                            # RTT = recv - send (纳秒)
                            rtt_ns = recv_ts - send_ts
                            # 单程延迟 = RTT / 2 (假设对称)
                            one_way_ns = rtt_ns // 2
                            latencies.append(one_way_ns / 1e6)  # 转换为毫秒
                except socket.timeout:
                    errors += 1

            except Exception:
                raise  # Linus: Fail loudly and explicitly
            # 速率控制
            elapsed = time.perf_counter() - loop_start
            sleep_time = packet_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        sock.close()

    if not latencies:
        return LatencyResult(0, 0, 0, 0, 0, 0, 0, 0, 0)

    latencies.sort()
    n = len(latencies)

    return LatencyResult(
        min_ms=latencies[0],
        max_ms=latencies[-1],
        avg_ms=statistics.mean(latencies),
        p50_ms=latencies[int(n * 0.5)],
        p95_ms=latencies[int(n * 0.95)],
        p99_ms=latencies[int(n * 0.99)],
        std_ms=statistics.stdev(latencies) if n > 1 else 0,
        jitter_ms=statistics.median([abs(latencies[i] - latencies[i-1])
                                     for i in range(1, n)]) if n > 1 else 0,
        samples=n
    )


# ============================================================
# 吞吐量测试
# ============================================================

async def test_throughput_udp(target: str, port: int, duration_sec: int = 10,
                              msg_size: int = 64, rate_hz: int = 10000) -> ThroughputResult:
    """
    UDP 吞吐量测试
    发送大量数据包，测量吞吐量
    """
    print(f"\n{'='*60}")
    print(f"📈 UDP 吞吐量测试: {target}:{port}")
    print(f"   持续: {duration_sec}s, 包大小: {msg_size}B, 频率: {rate_hz}Hz")
    print(f"{'='*60}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1)

    # 构造固定大小数据包
    payload = struct.pack(PROBE_FMT, 0, 0) + b'X' * (msg_size - PROBE_SIZE)

    total_bytes = 0
    total_msgs = 0
    start_time = time.time()
    packet_interval = 1.0 / rate_hz

    try:
        while time.time() - start_time < duration_sec:
            loop_start = time.perf_counter()

            try:
                seq = total_msgs + 1
                send_ts = time.perf_counter_ns()
                packet = struct.pack(PROBE_FMT, seq, send_ts) + payload[:msg_size-PROBE_SIZE]

                sock.sendto(packet, (target, port))
                total_bytes += len(packet)
                total_msgs += 1

            except Exception:
                raise  # Linus: Fail loudly and explicitly
            # 速率控制
            elapsed = time.perf_counter() - loop_start
            sleep_time = packet_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        sock.close()

    actual_duration = time.time() - start_time

    return ThroughputResult(
        total_msgs=total_msgs,
        total_bytes=total_bytes,
        duration_sec=actual_duration,
        msgs_per_sec=total_msgs / actual_duration,
        mbps=total_bytes * 8 / actual_duration / 1e6
    )


# ============================================================
# 丢包率测试
# ============================================================

async def test_packet_loss_udp(target: str, port: int, duration_sec: int = 30,
                               rate_hz: int = 100) -> PacketLossResult:
    """
    UDP 丢包率测试
    发送序列号数据包，检测丢包
    """
    print(f"\n{'='*60}")
    print(f"📦 UDP 丢包率测试: {target}:{port}")
    print(f"   持续: {duration_sec}s, 频率: {rate_hz}Hz")
    print(f"{'='*60}")

    # 需要在目标端运行 echo 服务器
    # 这里使用简单模式: 发送但不等待响应

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.5)

    sent = 0
    received = 0
    rtts = []
    start_time = time.time()
    packet_interval = 1.0 / rate_hz

    try:
        while time.time() - start_time < duration_sec:
            loop_start = time.perf_counter()

            try:
                sent += 1
                send_ts = time.perf_counter_ns()

                payload = struct.pack(PROBE_FMT, sent, send_ts)
                sock.sendto(payload, (target, port))

                # 尝试接收响应
                try:
                    resp, _ = sock.recvfrom(1024)
                    recv_ts = time.perf_counter_ns()

                    if len(resp) >= PROBE_SIZE:
                        seq, _ = struct.unpack(PROBE_FMT, resp[:PROBE_SIZE])
                        received += 1
                        rtt = (recv_ts - send_ts) / 1e6
                        rtts.append(rtt)
                except socket.timeout:
                    pass

            except Exception:
                raise  # Linus: Fail loudly and explicitly
            elapsed = time.perf_counter() - loop_start
            sleep_time = packet_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        sock.close()

    lost = sent - received
    loss_rate = (lost / sent * 100) if sent > 0 else 0

    return PacketLossResult(
        sent=sent,
        received=received,
        lost=lost,
        loss_rate_percent=loss_rate,
        avg_rtt_ms=statistics.mean(rtts) if rtts else 0
    )


# ============================================================
# 综合压力测试
# ============================================================

async def test_stress(target: str, port: int, duration_sec: int = 60) -> StressResult:
    """
    综合压力测试
    同时测试延迟、吞吐、丢包
    """
    print(f"\n{'='*60}")
    print(f"💪 综合压力测试: {target}:{port}")
    print(f"   持续: {duration_sec}s")
    print(f"{'='*60}")

    # 并行运行多个测试
    latency_task = asyncio.create_task(
        test_latency_udp(target, port, duration_sec, rate_hz=50)
    )
    throughput_task = asyncio.create_task(
        test_throughput_udp(target, port, min(duration_sec, 10), msg_size=64, rate_hz=5000)
    )
    packet_loss_task = asyncio.create_task(
        test_packet_loss_udp(target, port, duration_sec, rate_hz=50)
    )

    # 等待所有任务完成
    latency, throughput, packet_loss = await asyncio.gather(
        latency_task, throughput_task, packet_loss_task,
        return_exceptions=True
    )

    # 处理异常
    errors = 0
    if isinstance(latency, Exception):
        errors += 1
        latency = LatencyResult(0, 0, 0, 0, 0, 0, 0, 0, 0)
    if isinstance(throughput, Exception):
        errors += 1
        throughput = ThroughputResult(0, 0, 0, 0, 0)
    if isinstance(packet_loss, Exception):
        errors += 1
        packet_loss = PacketLossResult(0, 0, 0, 0, 0)

    return StressResult(
        duration_sec=duration_sec,
        total_packets=throughput.total_msgs,
        total_bytes=throughput.total_bytes,
        latency=latency,
        packet_loss=packet_loss,
        errors=errors
    )


# ============================================================
# 公网直连测试
# ============================================================

async def test_direct_binance():
    """
    测试公网直连延迟 (爱尔兰 -> Binance)
    """
    print(f"\n{'='*60}")
    print(f"🌐 公网直连测试: 爱尔兰 -> Binance")
    print(f"{'='*60}")

    import websockets

    latencies = []
    ws = None

    try:
        ws = await websockets.connect("wss://stream.binance.com:9443/ws/btcusdt@trade")

        for i in range(100):
            msg = await ws.recv()
            data = json.loads(msg)

            # Binance 事件时间戳 (毫秒)
            exchange_ts = int(data['E'])
            recv_ts = int(time.time() * 1000)

            latency = recv_ts - exchange_ts
            latencies.append(latency)

            if i >= 99:
                break

    finally:
        if ws:
            await ws.close()

    if not latencies:
        return None

    latencies.sort()
    n = len(latencies)

    return LatencyResult(
        min_ms=latencies[0],
        max_ms=latencies[-1],
        avg_ms=statistics.mean(latencies),
        p50_ms=latencies[int(n * 0.5)],
        p95_ms=latencies[int(n * 0.95)],
        p99_ms=latencies[int(n * 0.99)],
        std_ms=statistics.stdev(latencies) if n > 1 else 0,
        jitter_ms=statistics.median([abs(latencies[i] - latencies[i-1])
                                     for i in range(1, n)]) if n > 1 else 0,
        samples=n
    )


# ============================================================
# 输出格式
# ============================================================

def print_latency_result(name: str, result: LatencyResult):
    """打印延迟测试结果"""
    print(f"\n📊 {name}")
    print(f"   样本数:   {result.samples}")
    print(f"   最小:     {result.min_ms:.3f} ms")
    print(f"   最大:     {result.max_ms:.3f} ms")
    print(f"   平均:     {result.avg_ms:.3f} ms")
    print(f"   P50:      {result.p50_ms:.3f} ms")
    print(f"   P95:      {result.p95_ms:.3f} ms")
    print(f"   P99:      {result.p99_ms:.3f} ms")
    print(f"   抖动:     {result.jitter_ms:.3f} ms")
    print(f"   标准差:   {result.std_ms:.3f} ms")


def print_throughput_result(result: ThroughputResult):
    """打印吞吐量测试结果"""
    print(f"\n📈 吞吐量")
    print(f"   总消息数: {result.total_msgs:,}")
    print(f"   总字节:   {result.total_bytes:,} ({result.total_bytes/1024/1024:.2f} MB)")
    print(f"   持续时间: {result.duration_sec:.2f} s")
    print(f"   消息/秒:  {result.msgs_per_sec:,.0f}")
    print(f"   吞吐量:   {result.mbps:.2f} Mbps")


def print_packet_loss_result(result: PacketLossResult):
    """打印丢包测试结果"""
    print(f"\n📦 丢包率")
    print(f"   发送:     {result.sent:,}")
    print(f"   接收:     {result.received:,}")
    print(f"   丢包:     {result.lost:,}")
    print(f"   丢包率:   {result.loss_rate_percent:.2f}%")
    print(f"   平均RTT:  {result.avg_rtt_ms:.2f} ms")


def save_results_to_json(result, filename: str):
    """保存结果到 JSON 文件"""
    with open(filename, 'w') as f:
        json.dump(result, f, indent=2, default=lambda x: asdict(x) if hasattr(x, '__dict__') else str(x))
    print(f"\n💾 结果已保存到: {filename}")


# ============================================================
# 主函数
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="PolyEdge 网络压测工具")
    parser.add_argument("--mode", choices=["latency", "throughput", "packet-loss",
                                           "stress", "compare", "direct"],
                       default="latency", help="测试模式")
    parser.add_argument("--target", default="10.0.3.123", help="目标地址 (VPC)")
    parser.add_argument("--port", type=int, default=6666, help="目标端口")
    parser.add_argument("--duration", type=int, default=30, help="测试持续时间(秒)")
    parser.add_argument("--rate", type=int, default=100, help="发包频率(Hz)")
    parser.add_argument("--output", help="输出 JSON 文件")
    parser.add_argument("--direct", action="store_true", help="同时测试公网直连")

    args = parser.parse_args()

    results = {}

    if args.mode == "latency":
        result = await test_latency_udp(args.target, args.port, args.duration, args.rate)
        print_latency_result("UDP 延迟测试", result)
        results["latency"] = asdict(result)

    elif args.mode == "throughput":
        result = await test_throughput_udp(args.target, args.port,
                                          min(args.duration, 10), rate_hz=args.rate)
        print_throughput_result(result)
        results["throughput"] = asdict(result)

    elif args.mode == "packet-loss":
        result = await test_packet_loss_udp(args.target, args.port, args.duration, args.rate)
        print_packet_loss_result(result)
        results["packet_loss"] = asdict(result)

    elif args.mode == "stress":
        result = await test_stress(args.target, args.port, args.duration)
        print_latency_result("延迟", result.latency)
        print_throughput_result(ThroughputResult(
            result.total_packets, result.total_bytes, result.duration_sec,
            result.total_packets / result.duration_sec,
            result.total_bytes * 8 / result.duration_sec / 1e6
        ))
        print_packet_loss_result(result.packet_loss)
        print(f"\n   错误数: {result.errors}")
        results["stress"] = {
            "duration_sec": result.duration_sec,
            "latency": asdict(result.latency),
            "throughput": {
                "total_msgs": result.total_packets,
                "total_bytes": result.total_bytes,
                "msgs_per_sec": result.total_packets / result.duration_sec,
                "mbps": result.total_bytes * 8 / result.duration_sec / 1e6
            },
            "packet_loss": asdict(result.packet_loss),
            "errors": result.errors
        }

    elif args.mode == "direct":
        result = await test_direct_binance()
        if result:
            print_latency_result("公网直连 (爱尔兰 -> Binance)", result)
            results["direct"] = asdict(result)

    elif args.mode == "compare":
        print("\n" + "="*60)
        print("🔬 VPC vs 公网直连 对比测试")
        print("="*60)

        # 1. VPC 延迟测试
        vpc_result = await test_latency_udp(args.target, args.port, 30, 50)
        print_latency_result("VPC 内网 (东京 -> 爱尔兰)", vpc_result)
        results["vpc"] = asdict(vpc_result)

        # 2. 公网直连测试
        if args.direct:
            direct_result = await test_direct_binance()
            if direct_result:
                print_latency_result("公网直连 (爱尔兰 -> Binance)", direct_result)
                results["direct"] = asdict(direct_result)

                # 对比
                print("\n" + "="*60)
                print("📊 对比结果")
                print("="*60)
                print(f"   VPC 平均延迟:   {vpc_result.avg_ms:.2f} ms")
                print(f"   公网平均延迟:   {direct_result.avg_ms:.2f} ms")
                comparison = {
                    "vpc_avg_ms": vpc_result.avg_ms,
                    "vpc_samples": vpc_result.samples,
                    "direct_avg_ms": direct_result.avg_ms,
                    "direct_samples": direct_result.samples,
                    "speedup_x": None,
                    "comparable": False,
                    "reason": None,
                }
                if vpc_result.samples <= 0 or vpc_result.avg_ms <= 0:
                    comparison["reason"] = "vpc_latency_invalid_or_empty"
                    print("   速度提升:       N/A (VPC 样本无效或为空)")
                elif direct_result.samples <= 0 or direct_result.avg_ms <= 0:
                    comparison["reason"] = "direct_latency_invalid_or_empty"
                    print("   速度提升:       N/A (公网样本无效或为空)")
                else:
                    speedup_x = direct_result.avg_ms / vpc_result.avg_ms
                    comparison["speedup_x"] = speedup_x
                    comparison["comparable"] = True
                    print(f"   速度提升:       {speedup_x:.1f}x")
                results["compare"] = comparison

    # 保存结果
    if args.output:
        save_results_to_json(results, args.output)
    else:
        # 自动保存
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stress_test_{args.mode}_{timestamp}.json"
        save_results_to_json(results, filename)


if __name__ == "__main__":
    asyncio.run(main())
