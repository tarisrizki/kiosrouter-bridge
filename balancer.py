#!/usr/bin/env python3
import asyncio
import json
import os
import sys

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = int(os.environ.get("BALANCER_PORT", "40000"))
PORTS_FILE = os.environ.get("PORTS_FILE", "warp_ports.json")

def get_ports():
    try:
        with open(PORTS_FILE) as f:
            ports = json.load(f)
            if ports and isinstance(ports, list):
                return ports
    except Exception:
        pass
    # Fallback default 12 ports
    return list(range(40001, 40013))

ports = get_ports()
current_idx = 0

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

async def handle_client(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter):
    global current_idx
    target_ports = get_ports()
    
    # Try up to 3 ports in case one is temporarily recycling
    connected = False
    for attempt in range(len(target_ports)):
        port = target_ports[current_idx % len(target_ports)]
        current_idx += 1
        try:
            up_r, up_w = await asyncio.open_connection("127.0.0.1", port)
            connected = True
            break
        except Exception:
            continue

    if not connected:
        client_w.close()
        return

    await asyncio.gather(
        pipe(client_r, up_w),
        pipe(up_r, client_w)
    )

async def main():
    print(f"🔄 [Balancer] Starting SOCKS5 round-robin balancer on {LISTEN_HOST}:{LISTEN_PORT} -> {ports}")
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
