import os
import json
import asyncio
from fastapi import FastAPI, Request, Response, Header, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import uvicorn

TARGET_9ROUTER_URL = os.environ.get("TARGET_9ROUTER_URL", "https://bridge.kiosan.my.id/v1/chat/completions")
INTERNAL_KEY = os.environ.get("INTERNAL_KEY", "sk-c19dd0561af14824929e649640cce72d")
BRIDGE_SECRET = os.environ.get("BRIDGE_SECRET", "")
SILICONFLOW_KEY = os.environ.get("SILICONFLOW_KEY", "")

# Concurrency tracker per user key
ACTIVE_CONCURRENCY: dict[str, int] = {}
CONCURRENCY_LIMIT = 4

app = FastAPI(title="kiosan-bridge", version="3.0.0")

# Shared async HTTP client for high concurrency
http_client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0))

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

def get_user_identifier(request: Request) -> str:
    user_key = request.headers.get("x-user-key")
    if user_key and user_key.strip():
        return user_key.strip()
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
        if token and token != BRIDGE_SECRET and token != INTERNAL_KEY:
            return token
    return request.client.host if request.client else "anonymous"

def make_fresh_session() -> str:
    import time, random, string
    current = int(time.time() * 1000) * 0x1000 + 1
    val = (~current) & ((1 << 48) - 1)
    b = bytearray(6)
    for i in range(6):
        b[i] = (val >> (40 - 8*i)) & 0xff
    chars = string.ascii_letters + string.digits
    rand_suffix = ''.join(random.choice(chars) for _ in range(14))
    return f"ses_{b.hex()}{rand_suffix}"

def normalize_model(model_name: str) -> str:
    clean = model_name.strip()
    if clean.startswith("oc/"):
        base = clean[3:]
    else:
        base = clean
    if not base.endswith("-free"):
        base = f"{base}-free"
    return f"oc/{base}"

@app.get("/")
def index():
    return {
        "status": "online",
        "service": "kiosan-bridge",
        "architecture": "3-tier-edge-buffer",
        "target": TARGET_9ROUTER_URL,
        "concurrency_limit_per_user": CONCURRENCY_LIMIT,
        "active_users": len(ACTIVE_CONCURRENCY),
        "total_active_requests": sum(ACTIVE_CONCURRENCY.values()),
        "version": "3.0.0"
    }

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "kiosan-bridge",
        "active_connections": sum(ACTIVE_CONCURRENCY.values()),
        "timestamp": "ok"
    }

@app.get("/diag")
async def diag():
    return {
        "active_concurrency": ACTIVE_CONCURRENCY,
        "target_url": TARGET_9ROUTER_URL,
        "has_siliconflow_fallback": bool(SILICONFLOW_KEY)
    }

@app.get("/v1/models")
async def list_models():
    # Canonical verified free models
    return {
        "object": "list",
        "data": [
            {"id": "deepseek-v4.1-flash-free", "object": "model", "owned_by": "opencode"},
            {"id": "glm-5.3-flash-free", "object": "model", "owned_by": "opencode"},
            {"id": "mimo-v2.6-flash-free", "object": "model", "owned_by": "opencode"},
            {"id": "nemotron-3.5-super-vl-free", "object": "model", "owned_by": "opencode"}
        ]
    }

@app.post("/v1/chat/completions")
async def chat_completions(request: Request, x_bridge_secret: str = Header(None)):
    # 1. Validate Bridge Secret if configured
    auth_header = request.headers.get("Authorization", "")
    incoming_secret = x_bridge_secret or (auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else "")

    if BRIDGE_SECRET and incoming_secret != BRIDGE_SECRET:
        raise HTTPException(
            status_code=401,
            detail={"error": {"message": "Unauthorized: invalid bridge secret", "type": "authentication_error"}}
        )

    # 2. Concurrency Control (Max 4 active requests per user key)
    user_id = get_user_identifier(request)
    current_active = ACTIVE_CONCURRENCY.get(user_id, 0)
    if current_active >= CONCURRENCY_LIMIT:
        raise HTTPException(
            status_code=429,
            detail={
                "error": {
                    "message": f"Maksimal {CONCURRENCY_LIMIT} request berjalan bersamaan per key. Tunggu request sebelumnya selesai.",
                    "type": "rate_limit_error"
                }
            }
        )
    ACTIVE_CONCURRENCY[user_id] = current_active + 1

    try:
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail={"error": {"message": "Invalid JSON body", "type": "invalid_request_error"}})

        raw_model = body.get("model", "")
        if "model" in body and isinstance(body["model"], str):
            body["model"] = normalize_model(body["model"])

        is_stream = body.get("stream", False)
        incoming_session = request.headers.get("x-session-id") or request.headers.get("session-id") or make_fresh_session()

        forward_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {INTERNAL_KEY}",
            "Accept": "application/json, text/event-stream" if is_stream else "application/json",
            "x-session-id": incoming_session,
            "session-id": incoming_session,
            "x-claude-code-session-id": incoming_session
        }

        # 3. Dispatch to Proxmox Cluster (via Cloudflare Tunnel)
        if is_stream:
            # Streaming request with 1 automatic retry on 429
            for attempt in range(2):
                req_obj = http_client.build_request("POST", TARGET_9ROUTER_URL, json=body, headers=forward_headers, timeout=120.0)
                resp = await http_client.send(req_obj, stream=True)
                if resp.status_code != 200:
                    err_body = await resp.aread()
                    await resp.aclose()
                    if resp.status_code == 429 and attempt == 0:
                        await asyncio.sleep(1.0)
                        continue
                    return Response(content=err_body, status_code=resp.status_code, media_type="application/json")

                aiter = resp.aiter_bytes()
                try:
                    first_chunk = await anext(aiter)
                except StopAsyncIteration:
                    await resp.aclose()
                    if attempt == 0:
                        continue
                    return Response(content=b'{"error":{"message":"empty stream","type":"server_error"}}', status_code=502, media_type="application/json")

                if b"FreeTierError" in first_chunk or b"FreeUsageLimitError" in first_chunk:
                    rest = await resp.aread()
                    await resp.aclose()
                    if attempt == 0:
                        await asyncio.sleep(1.0)
                        continue
                    return Response(content=first_chunk + rest, status_code=429, media_type="application/json")

                async def stream_generator():
                    try:
                        yield first_chunk
                        async for chunk in aiter:
                            if chunk:
                                yield chunk
                    finally:
                        await resp.aclose()

                return StreamingResponse(
                    stream_generator(),
                    media_type="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"}
                )
        else:
            # Non-streaming request
            last_resp = None
            for attempt in range(2):
                try:
                    resp = await http_client.post(
                        TARGET_9ROUTER_URL,
                        json=body,
                        headers=forward_headers,
                        timeout=120.0
                    )
                    last_resp = resp
                    if resp.status_code == 429 and attempt == 0:
                        await asyncio.sleep(1.0)
                        continue

                    content = resp.content
                    if resp.status_code == 200:
                        try:
                            data = json.loads(content)
                            if "usage" in data and isinstance(data["usage"], dict):
                                u = data["usage"]
                                p = u.get("prompt_tokens", 0)
                                c = u.get("completion_tokens", 0)
                                if "total_tokens" not in u:
                                    u["total_tokens"] = p + c
                                content = json.dumps(data).encode("utf-8")
                        except Exception:
                            pass

                    return Response(
                        content=content,
                        status_code=resp.status_code,
                        media_type="application/json"
                    )
                except Exception as e:
                    if attempt == 1:
                        return Response(
                            content=json.dumps({"error": {"message": f"Bridge dispatch error: {str(e)}", "type": "upstream_error"}}),
                            status_code=502,
                            media_type="application/json"
                        )

            if last_resp is not None:
                return Response(
                    content=last_resp.content,
                    status_code=last_resp.status_code,
                    media_type="application/json"
                )

            return Response(
                content=json.dumps({"error": {"message": "All bridge retry attempts exhausted", "type": "upstream_error"}}),
                status_code=502,
                media_type="application/json"
            )

    finally:
        ACTIVE_CONCURRENCY[user_id] = max(0, ACTIVE_CONCURRENCY.get(user_id, 1) - 1)
        if ACTIVE_CONCURRENCY.get(user_id) == 0:
            ACTIVE_CONCURRENCY.pop(user_id, None)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
