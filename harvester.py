#!/usr/bin/env python3
import asyncio
import httpx
import logging
import os
import balancer

logger = logging.getLogger("harvester")

TARGET_9ROUTER_URL = os.environ.get("TARGET_9ROUTER_URL", "http://127.0.0.1:20130/v1/chat/completions")
INTERNAL_KEY = "sk-internal-heroku-bridge"

TARGET_POOL_SIZE = int(os.environ.get("TARGET_POOL_SIZE", "35"))
MAX_CONCURRENT_WORKERS = int(os.environ.get("HARVESTER_CONCURRENCY", "12"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "2.5"))

SOURCES = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=3000&country=all",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"
]

is_scanning = False

async def fetch_candidate_batch(batch_size=100) -> list[tuple[str, int]]:
    candidates = set()
    current_active = set(balancer.ACTIVE_PROXIES)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for url in SOURCES:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    for line in resp.text.splitlines():
                        line = line.strip()
                        if line and ":" in line and not line.startswith("#"):
                            parts = line.split(":")
                            if len(parts) == 2 and parts[1].isdigit():
                                item = (parts[0], int(parts[1]))
                                if item not in current_active and item not in candidates:
                                    candidates.add(item)
                                    if len(candidates) >= batch_size:
                                        break
            except Exception as e:
                logger.debug(f"Source fetch error {url}: {e}")
            if len(candidates) >= batch_size:
                break

    return list(candidates)

async def test_candidate(host: str, port: int, probe_lock: asyncio.Lock, http_client: httpx.AsyncClient) -> bool:
    async with probe_lock:
        balancer.set_probe_target(host, port)
        try:
            req_body = {
                "model": "oc/deepseek-v4.1-flash-free",
                "messages": [{"role": "user", "content": "1"}],
                "max_tokens": 1,
                "stream": False
            }
            headers = {
                "Authorization": f"Bearer {INTERNAL_KEY}",
                "Content-Type": "application/json",
                "x-connection-id": "probe-opencode"
            }
            resp = await http_client.post(
                TARGET_9ROUTER_URL,
                json=req_body,
                headers=headers,
                timeout=PROBE_TIMEOUT
            )
            if resp.status_code == 200:
                logger.info(f"🎯 [Harvester] Verified Zen Quota Active: {host}:{port}")
                balancer.add_proxy(host, port)
                return True
        except Exception:
            pass
        return False

async def run_harvester_cycle(active_concurrency_getter=None):
    global is_scanning
    if is_scanning:
        return
    is_scanning = True

    try:
        current_count = len(balancer.ACTIVE_PROXIES)
        if current_count >= TARGET_POOL_SIZE:
            return

        needed = TARGET_POOL_SIZE - current_count
        batch_size = max(50, min(120, needed * 4))
        candidates = await fetch_candidate_batch(batch_size=batch_size)
        if not candidates:
            return

        probe_lock = asyncio.Lock()
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            for host, port in candidates:
                # 1. User priority guardrail: if real users are active, pause scanning
                if active_concurrency_getter:
                    active = active_concurrency_getter()
                    if active and active > 0:
                        await asyncio.sleep(1.0)
                        continue

                # 2. Check if pool target reached
                if len(balancer.ACTIVE_PROXIES) >= TARGET_POOL_SIZE:
                    break

                # 3. Test candidate
                await test_candidate(host, port, probe_lock, http_client)
                await asyncio.sleep(0.05)
    finally:
        is_scanning = False

async def harvester_daemon(active_concurrency_getter=None):
    logger.info("🌾 [Harvester] Background proxy harvester daemon started")
    # Initial quick burst
    await asyncio.sleep(3.0)
    while True:
        try:
            current_count = len(balancer.ACTIVE_PROXIES)
            if current_count < TARGET_POOL_SIZE:
                await run_harvester_cycle(active_concurrency_getter)
            else:
                # Pool is healthy, sleep 30s before keeping fresh
                await asyncio.sleep(30.0)
        except Exception as e:
            logger.error(f"Harvester cycle error: {e}")
            await asyncio.sleep(10.0)
