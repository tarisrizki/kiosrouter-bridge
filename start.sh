#!/usr/bin/env bash
set -e

chmod +x ./bin/9router-go 2>/dev/null || true

echo "=========================================================="
echo "🚀 [KiosanBridge] Starting Mnet Live Quota AI Cloud Bridge"
echo "=========================================================="

# 1. Reset stale locks in sqlite3 and ensure probe connection exists
python3 -c "
import sqlite3, json
try:
    con = sqlite3.connect('data/data.sqlite3')
    cur = con.cursor()
    
    # Reset main connection
    row = cur.execute('SELECT data FROM providerConnections WHERE id=\"7931c4bc-e3cb-42d5-b244-2acc79a195bd\"').fetchone()
    if row:
        d = json.loads(row[0])
        d['rateLimitedUntil'] = None
        d['lastError'] = None
        d['status'] = 'active'
        d['backoffLevel'] = 0
        for k in list(d.keys()):
            if k.startswith('modelLock_'):
                d[k] = None
        cur.execute('UPDATE providerConnections SET data = ?, priority = 100, isActive = 1 WHERE id=\"7931c4bc-e3cb-42d5-b244-2acc79a195bd\"', (json.dumps(d),))
    
    # Ensure probe connection exists on :40099
    probe_row = cur.execute('SELECT id FROM providerConnections WHERE id = \"probe-opencode\"').fetchone()
    probe_d = {
        'backoffLevel': 0,
        'rateLimitedUntil': None,
        'status': 'active',
        'connectionProxyEnabled': True,
        'connectionProxyUrl': 'socks5://127.0.0.1:40099',
        'proxyPoolId': 'probe-socks5'
    }
    if not probe_row:
        cur.execute('''
            INSERT INTO providerConnections (id, provider, authType, name, priority, isActive, data, createdAt, updatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ''', ('probe-opencode', 'opencode', 'apikey', 'OpenCode Probe', 1, 1, json.dumps(probe_d)))
    else:
        cur.execute('UPDATE providerConnections SET data = ?, priority = 1, isActive = 1 WHERE id = \"probe-opencode\"', (json.dumps(probe_d),))
    
    # Set proxy pools
    cur.execute('INSERT OR REPLACE INTO proxyPools (id, isActive, testStatus, data, createdAt, updatedAt) VALUES (?, ?, ?, ?, datetime(\"now\"), datetime(\"now\"))',
                ('warp-socks5', 1, 'active', json.dumps({'name':'Main SOCKS5','proxyUrl':'socks5://127.0.0.1:40000'})))
    cur.execute('INSERT OR REPLACE INTO proxyPools (id, isActive, testStatus, data, createdAt, updatedAt) VALUES (?, ?, ?, ?, datetime(\"now\"), datetime(\"now\"))',
                ('probe-socks5', 1, 'active', json.dumps({'name':'Probe SOCKS5','proxyUrl':'socks5://127.0.0.1:40099'})))
    con.commit()
    con.close()
    print('SQLite initialized successfully!')
except Exception as e:
    print('DB init warning:', e)
"

# 2. Launch 9router-go AI gateway on :20130
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

# 3. Health verification of local 9router
echo "🩺 [KiosanBridge] Verifying internal pipeline..."
curl -s http://127.0.0.1:20130/models > /dev/null && echo "✅ 9router-go is online!" || echo "⚠️ 9router-go warmup pending"

# 4. Launch FastAPI on public $PORT (Single worker to prevent port collision on 40000)
TARGET_PORT="${PORT:-8000}"
export WEB_CONCURRENCY=1
echo "🌟 [KiosanBridge] Launching FastAPI ingress on port $TARGET_PORT (1 worker)..."
exec python3 -m uvicorn app:app --host 0.0.0.0 --port "$TARGET_PORT" --workers 1
