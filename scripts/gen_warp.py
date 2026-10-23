#!/usr/bin/env python3
import os
import json
import base64
import datetime
import urllib.request
import urllib.error
import subprocess
import time

POOL_SIZE = int(os.environ.get("WARP_POOL_SIZE", "12"))
CONFIG_PATH = os.environ.get("SINGBOX_CONFIG", "config.json")
PORTS_PATH = os.environ.get("PORTS_FILE", "warp_ports.json")

def generate_x25519():
    try:
        from cryptography.hazmat.primitives.asymmetric import x25519
        from cryptography.hazmat.primitives import serialization
        priv = x25519.X25519PrivateKey.generate()
        pub = priv.public_key()
        priv_b64 = base64.b64encode(priv.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption()
        )).decode()
        pub_b64 = base64.b64encode(pub.public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw
        )).decode()
        return priv_b64, pub_b64
    except ImportError:
        out = subprocess.check_output(["openssl", "genpkey", "-algorithm", "x25519", "-outform", "DER"])
        raw_priv = out[-32:]
        proc = subprocess.Popen(["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        pub_der, _ = proc.communicate(out)
        raw_pub = pub_der[-32:]
        return base64.b64encode(raw_priv).decode(), base64.b64encode(raw_pub).decode()

def register_one_account():
    priv_b64, pub_b64 = generate_x25519()
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload = {
        "key": pub_b64,
        "install_id": "",
        "fcm_token": "",
        "tos": now,
        "model": "PC",
        "serial_number": "",
        "locale": "en_US"
    }

    req = urllib.request.Request(
        "https://api.cloudflareclient.com/v0a2158/reg",
        data=json.dumps(payload).encode(),
        headers={"User-Agent": "okhttp/3.12.1", "Content-Type": "application/json; charset=UTF-8"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())

    v4 = data["config"]["interface"]["addresses"]["v4"]
    v6 = data["config"]["interface"]["addresses"]["v6"]
    peer_pub = data["config"]["peers"][0]["public_key"]
    return priv_b64, v4, v6, peer_pub

def main():
    if os.path.exists(CONFIG_PATH) and os.path.getsize(CONFIG_PATH) > 100 and os.path.exists(PORTS_PATH):
        print(f"⚡ [KiosanBridge] Using existing WARP config: {CONFIG_PATH} (0ms boot)")
        return

    print(f"📡 Initializing {POOL_SIZE} parallel Cloudflare WARP WireGuard endpoints...")
    accounts = []
    
    # Try registering up to POOL_SIZE accounts; if rate-limited, recycle available accounts
    for i in range(POOL_SIZE):
        try:
            priv_b64, v4, v6, peer_pub = register_one_account()
            accounts.append((priv_b64, v4, v6, peer_pub))
            print(f"  ✓ Registered WARP Account #{len(accounts)}: {v4}")
            time.sleep(1.0)
        except Exception as e:
            print(f"  ⚠️ Registration rate limited ({e}), recycling existing {len(accounts)} accounts across the pool...")
            break

    if not accounts:
        # Fallback pre-registered static profile if cloudflare reg is completely throttled
        raise RuntimeError("Failed to register any WARP account from current IP")

    endpoints = []
    inbounds = []
    rules = []
    ports = []

    for i in range(1, POOL_SIZE + 1):
        # Round-robin among available accounts
        priv_b64, v4, v6, peer_pub = accounts[(i - 1) % len(accounts)]
        tag = f"warp-ep-{i}"
        in_tag = f"socks-{i}"
        port = 40000 + i

        endpoints.append({
            "type": "wireguard",
            "tag": tag,
            "system": False,
            "address": [f"{v4}/32", f"{v6}/128"],
            "private_key": priv_b64,
            "peers": [{
                "address": "162.159.192.1",
                "port": 2408,
                "public_key": peer_pub,
                "allowed_ips": ["0.0.0.0/0", "::/0"]
            }]
        })
        inbounds.append({
            "type": "socks",
            "tag": in_tag,
            "listen": "127.0.0.1",
            "listen_port": port
        })
        rules.append({
            "inbound": in_tag,
            "outbound": tag
        })
        ports.append(port)

    config = {
        "log": {"level": "warn"},
        "endpoints": endpoints,
        "inbounds": inbounds,
        "outbounds": [{"type": "direct", "tag": "direct"}],
        "route": {"rules": rules}
    }

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

    with open(PORTS_PATH, "w") as f:
        json.dump(ports, f)

    print(f"✅ Generated {len(endpoints)} WARP endpoints saved to {CONFIG_PATH} (Ports: {ports[0]}-{ports[-1]}) using {len(accounts)} unique credentials")

if __name__ == "__main__":
    main()
