import time
import requests
import statistics
import os
import socket
import socks  # Requires PySocks

# Configuration
TARGET_URL = "https://api.binance.com/api/v3/ping"
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 1080
ITERATIONS = 50

def get_latency(use_proxy=False):
    session = requests.Session()
    if use_proxy:
        session.proxies = {
            'http': f'socks5://{PROXY_HOST}:{PROXY_PORT}',
            'https': f'socks5://{PROXY_HOST}:{PROXY_PORT}'
        }

    start = time.perf_counter()
    try:
        resp = session.get(TARGET_URL, timeout=5)
        resp.raise_for_status()
        end = time.perf_counter()
        return (end - start) * 1000  # ms
    except Exception as e:
        return None

def run_test(name, use_proxy):
    print(f"\n>>> Running Test: {name} (Proxy={use_proxy})...")
    latencies = []
    errors = 0

    for i in range(ITERATIONS):
        lat = get_latency(use_proxy)
        if lat:
            latencies.append(lat)
            print(f"#{i+1}: {lat:.2f} ms", end="\r")
        else:
            errors += 1
            print(f"#{i+1}: ERROR", end="\r")
        time.sleep(0.1)

    print("\n" + "-"*30)
    if not latencies:
        print("All failed.")
        return

    avg = statistics.mean(latencies)
    median = statistics.median(latencies)
    stdev = statistics.stdev(latencies) if len(latencies) > 1 else 0
    min_lat = min(latencies)
    max_lat = max(latencies)

    print(f"Results for {name}:")
    print(f"  Samples: {len(latencies)}/{ITERATIONS}")
    print(f"  Errors : {errors}")
    print(f"  Min    : {min_lat:.2f} ms")
    print(f"  Max    : {max_lat:.2f} ms")
    print(f"  Avg    : {avg:.2f} ms")
    print(f"  Jitter : {stdev:.2f} ms (Lower is Better)")

if __name__ == "__main__":
    print("Installing requirements...")
    os.system("pip install requests PySocks -q")

    print("=== BENCHMARK START ===")

    # 1. Direct (Optimized Kernel)
    run_test("Ireland -> Public Internet -> Binance", False)

    # 2. Tunnel (Backbone)
    run_test("Ireland -> AWS Backbone -> Tokyo -> Binance", True)

    print("\n=== BENCHMARK END ===")
