#!/usr/bin/env bash
set -e

chmod +x ./bin/sing-box ./bin/9router-go 2>/dev/null || true

echo "=========================================================="
echo "🚀 [KiosanBridge] Starting 12-IP Userspace WARP Cloud Bridge"
echo "=========================================================="

# 1. Register 12 Cloudflare WARP Accounts & Generate sing-box config
echo "📡 [KiosanBridge] Generating 12 parallel WARP accounts..."
python3 scripts/gen_warp.py

# 2. Launch sing-box userspace gVisor netstack
echo "⚡ [KiosanBridge] Starting sing-box WireGuard netstack..."
./bin/sing-box run -c config.json > singbox.log 2>&1 &
SB_PID=$!
sleep 2

# 3. Launch SOCKS5 round-robin balancer on :40000
echo "🔄 [KiosanBridge] Starting SOCKS5 round-robin balancer on :40000..."
python3 balancer.py > balancer.log 2>&1 &
BAL_PID=$!
sleep 1

# Reset stale locks in sqlite3
python3 -c "
import sqlite3, json
try:
    con = sqlite3.connect('data/data.sqlite3')
    cur = con.cursor()
    row = cur.execute('SELECT data FROM providerConnections WHERE id=\"7931c4bc-e3cb-42d5-b244-2acc79a195bd\"').fetchone()
    if row:
        data = json.loads(row[0])
        data['rateLimitedUntil'] = None
        data['lastError'] = None
        data['status'] = 'active'
        data['backoffLevel'] = 0
        for k in list(data.keys()):
            if k.startswith('modelLock_'):
                data[k] = None
        cur.execute('UPDATE providerConnections SET data = ? WHERE id=\"7931c4bc-e3cb-42d5-b244-2acc79a195bd\"', (json.dumps(data),))
        con.commit()
except Exception as e:
    print('Reset warning:', e)
"

# 4. Launch 9router-go AI gateway on :20130
echo "🛡️ [KiosanBridge] Starting 9router-go gateway on :20130..."
DATA_DIR="./data" \
DB_PATH="./data/data.sqlite3" \
PORT=20130 \
HOST=127.0.0.1 \
BIND_ADDR=127.0.0.1 \
RTK_ENABLED=false \
CAVEMAN_ENABLED=false \
./bin/9router-go > 9router.log 2>&1 &
R_PID=$!
sleep 2

# 5. Health verification of local 9router
echo "🩺 [KiosanBridge] Verifying internal pipeline..."
curl -s http://127.0.0.1:20130/models > /dev/null && echo "✅ 9router-go is online!" || echo "⚠️ 9router-go warmup pending"

# 6. Launch FastAPI on public $PORT
TARGET_PORT="${PORT:-8000}"
echo "🌟 [KiosanBridge] Launching FastAPI ingress on port $TARGET_PORT..."
exec python3 -m uvicorn app:app --host 0.0.0.0 --port "$TARGET_PORT"
