#!/usr/bin/env python3
"""
延迟对比测试工具
比较 VPC 内网转发 vs 公网直连的延迟

使用:
    # 测试 VPC 内网 (需要先在东京运行 relay)
    python3 scripts/latency_test.py --mode vpc

    # 测试公网直连 (从爱尔兰直接连接 Binance)
    python3 scripts/latency_test.py --mode direct

    # 对比测试
    python3 scripts/latency_test.py --mode compare
"""

import asyncio
import json
import time
import argparse
import statistics
from datetime import datetime
from dataclasses import dataclass
from typing import List

BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"


@dataclass
class LatencySample:
    """单次延迟采样"""
    recv_time: float      # 本地接收时间 (纳秒)
    exchange_time: float  # 交易所时间 (纳秒)
    latency_ms: float     # 延迟 (毫秒)


class LatencyTester:
    def __init__(self, mode: str):
        self.mode = mode
        self.samples: List[LatencySample] = []
        self.running = True

    async def connect_binance(self):
        """连接 Binance WebSocket"""
        import websockets
        self.ws = await websockets.connect(BINANCE_WS)
        print(f"[{datetime.now()}] ✅ 连接到 Binance")

    async def measure_direct(self):
        """测量直连延迟: 爱尔兰 -> Binance"""
        await self.connect_binance()

        print(f"[{datetime.now()}] 开始测量直连延迟...")
        count = 0

        async for msg in self.ws:
            data = json.loads(msg)

            # Binance 时间戳 (毫秒)
            exchange_ts = int(data['E'])
            recv_ts = int(time.time() * 1000)

            latency = recv_ts - exchange_ts
            self.samples.append(LatencySample(
                recv_time=recv_ts * 1_000_000,
                exchange_time=exchange_ts * 1_000_000,
                latency_ms=latency
            ))

            count += 1
            if count >= 100:  # 采集 100 个样本
                break

        await self.ws.close()
        self.print_stats("直连 (爱尔兰 -> Binance)")

    async def measure_vpc(self, tokyo_host: str = "172.31.44.26"):
        """测量 VPC 内网延迟: 爱尔兰 -> 东京 -> Binance"""
        # 这里需要东京运行 relay 转发
        # 暂时通过模拟方式测试 VPC 连通性

        import socket

        print(f"[{datetime.now()}] 测试 VPC 连通性: {tokyo_host}:6666")

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)
            sock.connect((tokyo_host, 6666))

            # 发送探测包
            start = time.perf_counter_ns()
            sock.send(b"ping")
            resp = sock.recv(1024)
            end = time.perf_counter_ns()

            rtt_ns = end - start
            one_way = rtt_ns / 2  # 假设对称

            print(f"[{datetime.now()}] ✅ VPC 连通正常, RTT: {rtt_ns/1e6:.2f}ms (单程: {one_way/1e6:.2f}ms)")
            print(f"[{datetime.now()}] ℹ️  完整路径: Binance -> 东京({tokyo_host}) -> VPC -> 爱尔兰")
            print(f"[{datetime.now()}] ℹ️  预估延迟: ~7ms (5ms + 1ms + 1ms)")

        except Exception as e:
            print(f"[{datetime.now()}] ❌ VPC 连接失败: {e}")
            print(f"[{datetime.now()}] 请确保东京服务器已启动 relay")

    async def run_relay(self, ireland_ip: str = "10.0.3.123"):
        """在东京运行 relay (需要部署到东京服务器)"""
        print(f"[{datetime.now()}] ℹ️  此模式需要在东京服务器运行 relay")
        print(
            f"[{datetime.now()}] ℹ️  建议部署: cargo run -p feeder_tokyo --bin sender --release"
        )
        print(f"[{datetime.now()}] ℹ️  TARGET={ireland_ip}:6666")

    def print_stats(self, label: str):
        """打印延迟统计"""
        if not self.samples:
            print("无数据")
            return

        latencies = [s.latency_ms for s in self.samples]

        print(f"\n{'='*50}")
        print(f"📊 {label} 延迟统计 (n={len(latencies)})")
        print(f"{'='*50}")
        print(f"  最小:  {min(latencies):.2f} ms")
        print(f"  最大:  {max(latencies):.2f} ms")
        print(f"  平均:  {statistics.mean(latencies):.2f} ms")
        print(f"  中位数: {statistics.median(latencies):.2f} ms")
        print(f"  P95:   {sorted(latencies)[int(len(latencies)*0.95)]:.2f} ms")
        print(f"  P99:   {sorted(latencies)[int(len(latencies)*0.99)]:.2f} ms")
        print(f"  标准差: {statistics.stdev(latencies):.2f} ms")
        print(f"{'='*50}\n")


async def run_tcp_latency_test(target: str, port: int, duration_sec: int = 30):
    """
    TCP 延迟测试 - 发送探测包测量 RTT

    使用场景:
    - 测试东京 -> 爱尔兰 VPC 延迟
    - 对比不同网络路径
    """
    import socket
    import struct

    print(f"\n{'='*50}")
    print(f"🔬 TCP 延迟测试: {target}:{port}")
    print(f"{'='*50}")

    # 构造探测包 (包含时间戳)
    probe_data = struct.pack('!d', time.time())

    latencies = []

    for i in range(duration_sec * 10):  # 每秒 10 次
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)

            # 测量连接时间
            start = time.perf_counter_ns()
            sock.connect((target, port))
            conn_time = time.perf_counter_ns()

            # 发送探测
            sock.send(probe_data)
            sock.recv(1024)  # 简单响应

            end = time.perf_counter_ns()

            rtt_ns = end - start
            latencies.append(rtt_ns / 1e6)  # 转换为 ms

            sock.close()

        except Exception as e:
            print(f"错误: {e}")

        await asyncio.sleep(0.1)

    if latencies:
        print(f"\n📊 TCP 延迟统计:")
        print(f"  平均: {statistics.mean(latencies):.2f} ms")
        print(f"  P50:  {statistics.median(latencies):.2f} ms")
        print(f"  P95:  {sorted(latencies)[int(len(latencies)*0.95)]:.2f} ms")
        print(f"  P99:  {sorted(latencies)[int(len(latencies)*0.99)]:.2f} ms")
        print(f"  抖动: {statistics.stdev(latencies):.2f} ms")


async def run_udp_latency_test(target: str, port: int, duration_sec: int = 30):
    """
    UDP 延迟测试 - 发送探测包测量延迟

    这是核心测试，用于验证 VPC 网络质量
    """
    import socket

    print(f"\n{'='*50}")
    print(f"🔬 UDP 延迟测试: {target}:{port}")
    print(f"{'='*50}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1)

    latencies = []
    seq = 0

    try:
        for i in range(duration_sec * 100):  # 每秒 100 次
            # 构造探测包: 序列号 + 时间戳
            seq += 1
            send_ts = time.perf_counter_ns()
            probe_data = struct.pack('!IQ', seq, send_ts)

            sock.sendto(probe_data, (target, port))

            try:
                resp, _ = sock.recvfrom(1024)
                recv_ts = time.perf_counter_ns()

                if len(resp) >= 16:
                    seq_resp, send_ts_resp = struct.unpack('!IQ', resp[:16])
                    if seq_resp == seq:
                        latency_ns = recv_ts - send_ts
                        latencies.append(latency_ns / 1e6)

            except socket.timeout:
                pass  # 超时忽略

            await asyncio.sleep(0.01)  # 10ms 间隔

    finally:
        sock.close()

    if latencies:
        print(f"\n📊 UDP 延迟统计:")
        print(f"  样本数: {len(latencies)}")
        print(f"  丢包率: {(duration_sec * 100 - len(latencies)) / (duration_sec * 100) * 100:.1f}%")
        print(f"  平均: {statistics.mean(latencies):.2f} ms")
        print(f"  P50:  {statistics.median(latencies):.2f} ms")
        print(f"  P95:  {sorted(latencies)[int(len(latencies)*0.95)]:.2f} ms")
        print(f"  P99:  {sorted(latencies)[int(len(latencies)*0.99)]:.2f} ms")
        print(f"  抖动: {statistics.stdev(latencies):.2f} ms")
        print(f"  最小: {min(latencies):.2f} ms")
        print(f"  最大: {max(latencies):.2f} ms")
    else:
        print("❌ 无有效响应，请检查连接")


async def main():
    parser = argparse.ArgumentParser(description="延迟测试工具")
    parser.add_argument("--mode", choices=["direct", "vpc", "compare", "tcp", "udp"],
                       default="direct", help="测试模式")
    parser.add_argument("--target", default="172.31.44.26", help="目标地址")
    parser.add_argument("--port", type=int, default=6666, help="目标端口")
    parser.add_argument("--duration", type=int, default=30, help="测试时长(秒)")

    args = parser.parse_args()

    if args.mode == "direct":
        tester = LatencyTester("direct")
        await tester.measure_direct()

    elif args.mode == "vpc":
        tester = LatencyTester("vpc")
        await tester.measure_vpc(args.target)

    elif args.mode == "tcp":
        await run_tcp_latency_test(args.target, args.port, args.duration)

    elif args.mode == "udp":
        await run_udp_latency_test(args.target, args.port, args.duration)

    elif args.mode == "compare":
        print("="*60)
        print("🔬 延迟对比测试")
        print("="*60)

        # 测试公网直连
        print("\n[1/2] 测试公网直连延迟...")
        tester = LatencyTester("direct")
        direct_samples = tester.samples.copy()

        # 测试 VPC
        print("[2/2] 测试 VPC 内网延迟...")
        tester2 = LatencyTester("vpc")
        await tester2.measure_vpc(args.target)


if __name__ == "__main__":
    import struct
    asyncio.run(main())
