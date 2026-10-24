#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "🚀 [KiosanBridge] Starting High-Concurrency Stream Buffer"
echo "=========================================================="

TARGET_PORT="${PORT:-8000}"
WORKERS="${WEB_CONCURRENCY:-1}"
echo "🌟 [KiosanBridge] Launching FastAPI ingress on port $TARGET_PORT ($WORKERS worker(s))..."
exec python3 -m uvicorn app:app --host 0.0.0.0 --port "$TARGET_PORT" --workers "$WORKERS"
