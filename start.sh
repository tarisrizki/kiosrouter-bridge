#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "🚀 [KiosanBridge] Starting High-Concurrency Stream Buffer"
echo "=========================================================="

TARGET_PORT="${PORT:-8000}"
export WEB_CONCURRENCY=2
echo "🌟 [KiosanBridge] Launching FastAPI ingress on port $TARGET_PORT (2 workers)..."
exec python3 -m uvicorn app:app --host 0.0.0.0 --port "$TARGET_PORT" --workers 2
