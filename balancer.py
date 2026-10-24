#!/usr/bin/env python3
import asyncio
import json
import os
import logging

logger = logging.getLogger("balancer")

LISTEN_HOST = "127.0.0.1"
PROD_PORT = int(os.environ.get("BALANCER_PORT", "40000"))
PROBE_PORT = int(os.environ.get("PROBE_PORT", "40099"))
STATE_FILE = os.environ.get("PROXY_STATE_FILE", "active_proxies.json")

# In-memory pool of verified active proxies: list of (host, port)
ACTIVE_PROXIES: list[tuple[str, int]] = []
CURRENT_PROBE_TARGET: tuple[str, int] | None = None
current_rr_idx = 0

def load_saved_proxies():
    global ACTIVE_PROXIES
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    ACTIVE_PROXIES = [(p[0], int(p[1])) for p in data if len(p) == 2]
                    logger.info(f"💾 Loaded {len(ACTIVE_PROXIES)} proxies from {STATE_FILE}")
    except Exception as e:
        logger.warning(f"Failed to load {STATE_FILE}: {e}")

def save_active_proxies():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(ACTIVE_PROXIES, f, indent=2)
    except Exception:
        pass

def add_proxy(host: str, port: int) -> bool:
    item = (host, int(port))
    if item not in ACTIVE_PROXIES:
        ACTIVE_PROXIES.append(item)
        save_active_proxies()
        logger.info(f"✨ [Pool] Added active proxy {host}:{port} (Total: {len(ACTIVE_PROXIES)})")
        return True
    return False

def evict_proxy(host: str, port: int):
    item = (host, int(port))
    if item in ACTIVE_PROXIES:
        ACTIVE_PROXIES.remove(item)
        save_active_proxies()
        logger.info(f"🗑️ [Pool] Evicted proxy {host}:{port} (Remaining: {len(ACTIVE_PROXIES)})")

def set_probe_target(host: str, port: int):
    global CURRENT_PROBE_TARGET
    CURRENT_PROBE_TARGET = (host, int(port))

def get_pool_status():
    return {
        "active_count": len(ACTIVE_PROXIES),
        "proxies": [f"{h}:{p}" for h, p in ACTIVE_PROXIES[:10]],
        "current_probe": f"{CURRENT_PROBE_TARGET[0]}:{CURRENT_PROBE_TARGET[1]}" if CURRENT_PROBE_TARGET else None
    }

async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def handle_prod_client(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter):
    global current_rr_idx
    if not ACTIVE_PROXIES:
        client_w.close()
        return

    connected = False
    up_r, up_w = None, None
    for attempt in range(min(3, len(ACTIVE_PROXIES))):
        proxy = ACTIVE_PROXIES[current_rr_idx % len(ACTIVE_PROXIES)]
        current_rr_idx += 1
        host, port = proxy
        try:
            up_r, up_w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3.0)
            connected = True
            break
        except Exception:
            # Drop dead proxy immediately
            evict_proxy(host, port)
            continue

    if not connected or not up_r or not up_w:
        client_w.close()
        return

    await asyncio.gather(pipe(client_r, up_w), pipe(up_r, client_w))

async def handle_probe_client(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter):
    global CURRENT_PROBE_TARGET
    if not CURRENT_PROBE_TARGET:
        client_w.close()
        return

    host, port = CURRENT_PROBE_TARGET
    try:
        up_r, up_w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3.0)
    except Exception:
        client_w.close()
        return

    await asyncio.gather(pipe(client_r, up_w), pipe(up_r, client_w))

async def start_balancer():
    load_saved_proxies()
    logger.info(f"🚀 [Balancer] Starting Production SOCKS5 Balancer on {LISTEN_HOST}:{PROD_PORT}")
    prod_server = await asyncio.start_server(handle_prod_client, LISTEN_HOST, PROD_PORT, reuse_address=True)
    logger.info(f"🔬 [Balancer] Starting Probe SOCKS5 Slot on {LISTEN_HOST}:{PROBE_PORT}")
    probe_server = await asyncio.start_server(handle_probe_client, LISTEN_HOST, PROBE_PORT, reuse_address=True)
    return prod_server, probe_server

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    async def main():
        prod_s, probe_s = await start_balancer()
        async with prod_s, probe_s:
            await asyncio.gather(prod_s.serve_forever(), probe_s.serve_forever())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
