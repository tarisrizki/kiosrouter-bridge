import os
import json
import time
import string
import random
import asyncio
import hmac
import logging
from typing import Optional, Dict
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, Header, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import uvicorn

# Configuration
TARGET_9ROUTER_URL = os.environ.get("TARGET_9ROUTER_URL", "https://bridge.kiosan.my.id/v1/chat/completions")
INTERNAL_KEY = os.environ.get("INTERNAL_KEY", "sk-c19dd0561af14824929e649640cce72d")
BRIDGE_SECRET = os.environ.get("BRIDGE_SECRET", "")
CONCURRENCY_LIMIT = int(os.environ.get("CONCURRENCY_LIMIT", "4"))
RETRY_DELAY_MIN = float(os.environ.get("RETRY_DELAY_MIN", "1.5"))
RETRY_DELAY_MAX = float(os.environ.get("RETRY_DELAY_MAX", "2.0"))

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kiosan-bridge")

# Thread/Task-safe Concurrency Tracker per User Key
class ConcurrencyTracker:
    def __init__(self, limit: int = 4, counts: Optional[Dict[str, int]] = None):
        self.limit = limit
        self._lock = asyncio.Lock()
        self._counts = counts if counts is not None else {}

    async def acquire(self, key: str) -> bool:
        async with self._lock:
            current = self._counts.get(key, 0)
            if current >= self.limit:
                return False
            self._counts[key] = current + 1
            return True

    async def release(self, key: str):
        async with self._lock:
            if key in self._counts:
                self._counts[key] -= 1
                if self._counts[key] <= 0:
                    self._counts.pop(key, None)

    async def get_active(self) -> Dict[str, int]:
        async with self._lock:
            return dict(self._counts)

    async def total_active(self) -> int:
        async with self._lock:
            return sum(self._counts.values())

    async def user_active(self, key: str) -> int:
        async with self._lock:
            return self._counts.get(key, 0)

# Idempotent lease handle to guarantee single release under all conditions
class SlotLease:
    def __init__(self, tracker: ConcurrencyTracker, key: str):
        self.tracker = tracker
        self.key = key
        self.released = False
        self._lock = asyncio.Lock()

    async def release(self):
        async with self._lock:
            if not self.released:
                self.released = True
                await self.tracker.release(self.key)

# StreamingResponse subclass ensuring body iterator is explicitly closed on client disconnect
class SafeStreamingResponse(StreamingResponse):
    async def stream_response(self, send) -> None:
        try:
            await super().stream_response(send)
        finally:
            if hasattr(self.body_iterator, "aclose"):
                try:
                    await self.body_iterator.aclose()
                except Exception:
                    pass

# Shared in-memory concurrency storage
ACTIVE_CONCURRENCY: Dict[str, int] = {}
tracker = ConcurrencyTracker(limit=CONCURRENCY_LIMIT, counts=ACTIVE_CONCURRENCY)

# Shared async HTTP client pool tuned for high concurrency
CLIENT_LIMITS = httpx.Limits(
    max_connections=1000,
    max_keepalive_connections=100,
    keepalive_expiry=30.0
)
CLIENT_TIMEOUT = httpx.Timeout(120.0, connect=10.0, read=120.0, write=30.0, pool=10.0)

http_client = httpx.AsyncClient(limits=CLIENT_LIMITS, timeout=CLIENT_TIMEOUT)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    if http_client is None or http_client.is_closed:
        http_client = httpx.AsyncClient(limits=CLIENT_LIMITS, timeout=CLIENT_TIMEOUT)
    yield
    if http_client and not http_client.is_closed:
        await http_client.aclose()

app = FastAPI(title="kiosan-bridge", version="3.2.0", lifespan=lifespan)

def get_user_identifier(request: Request) -> str:
    user_key = request.headers.get("x-user-key")
    if user_key and user_key.strip():
        return user_key.strip()
    api_key = request.headers.get("x-api-key")
    if api_key and api_key.strip():
        return api_key.strip()
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token and token != BRIDGE_SECRET and token != INTERNAL_KEY:
            return token
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded and forwarded.strip():
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "anonymous"

def make_fresh_session() -> str:
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

async def wait_retry_delay():
    delay = random.uniform(RETRY_DELAY_MIN, RETRY_DELAY_MAX)
    logger.info(f"Retrying upstream dispatch after {delay:.2f}s...")
    await asyncio.sleep(delay)

def build_forward_headers(is_stream: bool, session_id: Optional[str] = None) -> dict:
    session = session_id or make_fresh_session()
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {INTERNAL_KEY}",
        "Accept": "application/json, text/event-stream" if is_stream else "application/json",
        "x-session-id": session,
        "session-id": session,
        "x-claude-code-session-id": session
    }

@app.get("/")
async def index():
    active_map = await tracker.get_active()
    return {
        "status": "online",
        "service": "kiosan-bridge",
        "architecture": "3-tier-edge-buffer",
        "target": TARGET_9ROUTER_URL,
        "concurrency_limit_per_user": CONCURRENCY_LIMIT,
        "active_users": len(active_map),
        "total_active_requests": sum(active_map.values()),
        "version": "3.2.0"
    }

@app.get("/health")
async def health():
    total_active = await tracker.total_active()
    return {
        "status": "healthy",
        "service": "kiosan-bridge",
        "active_connections": total_active,
        "timestamp": "ok"
    }

@app.get("/diag")
async def diag():
    active_map = await tracker.get_active()
    return {
        "active_concurrency": active_map,
        "target_url": TARGET_9ROUTER_URL,
        "mode": "100%-pure-opencode-free"
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
@app.post("/chat/completions")
async def chat_completions(request: Request, x_bridge_secret: Optional[str] = Header(None)):
    # 1. Validate Bridge Secret if configured
    if BRIDGE_SECRET:
        auth_header = request.headers.get("Authorization", "")
        incoming_secret = x_bridge_secret or (auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else "")
        if not incoming_secret or not hmac.compare_digest(incoming_secret, BRIDGE_SECRET):
            raise HTTPException(
                status_code=401,
                detail={"error": {"message": "Unauthorized: invalid bridge secret", "type": "authentication_error"}}
            )

    # 2. Concurrency Control (Max 4 active requests per user key)
    user_id = get_user_identifier(request)
    acquired = await tracker.acquire(user_id)
    if not acquired:
        raise HTTPException(
            status_code=429,
            detail={
                "error": {
                    "message": f"Maksimal {CONCURRENCY_LIMIT} request berjalan bersamaan per key. Tunggu request sebelumnya selesai.",
                    "type": "rate_limit_error"
                }
            }
        )

    lease = SlotLease(tracker, user_id)
    # Slot release responsibility: True = released in handler finally; False = handed to stream generator
    slot_held_by_route = True

    try:
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(
                status_code=400,
                detail={"error": {"message": "Invalid JSON body", "type": "invalid_request_error"}}
            )

        if "model" in body and isinstance(body["model"], str):
            body["model"] = normalize_model(body["model"])

        is_stream = bool(body.get("stream", False))
        incoming_session = request.headers.get("x-session-id") or request.headers.get("session-id")

        # 3. Dispatch with Transparent Single Retry on 429, 502, 503, 504, 52x or network fault
        if is_stream:
            for attempt in range(2):
                session_to_use = incoming_session if (incoming_session and attempt == 0) else make_fresh_session()
                forward_headers = build_forward_headers(is_stream=True, session_id=session_to_use)
                req_obj = http_client.build_request(
                    "POST",
                    TARGET_9ROUTER_URL,
                    json=body,
                    headers=forward_headers,
                    timeout=CLIENT_TIMEOUT
                )

                try:
                    resp = await http_client.send(req_obj, stream=True)
                except (httpx.RequestError, httpx.HTTPError) as exc:
                    logger.warning(f"Streaming attempt {attempt} failed with network error: {exc}")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(
                        content=json.dumps({"error": {"message": f"Bridge upstream network error: {str(exc)}", "type": "upstream_error"}}).encode("utf-8"),
                        status_code=502,
                        media_type="application/json"
                    )

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    err_body = await resp.aread()
                    await resp.aclose()
                    logger.warning(f"Streaming attempt {attempt} received upstream status {resp.status_code}")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(content=err_body, status_code=resp.status_code, media_type="application/json")

                if resp.status_code != 200:
                    err_body = await resp.aread()
                    await resp.aclose()
                    return Response(content=err_body, status_code=resp.status_code, media_type="application/json")

                # If status is 200, check content type
                content_type = resp.headers.get("content-type", "").lower()
                if "application/json" in content_type:
                    # Upstream returned a non-streamed JSON (e.g. disguised error)
                    json_body = await resp.aread()
                    await resp.aclose()
                    is_disguised_limit = (
                        b"FreeTierError" in json_body
                        or b"FreeUsageLimitError" in json_body
                        or b"rate_limit_exceeded" in json_body
                    )
                    if is_disguised_limit:
                        logger.warning(f"Streaming attempt {attempt} detected disguised rate limit JSON")
                        if attempt == 0:
                            await wait_retry_delay()
                            continue
                        return Response(content=json_body, status_code=429, media_type="application/json")
                    return Response(content=json_body, status_code=200, media_type="application/json")

                # Peek first chunk for disguised errors or empty stream
                aiter = resp.aiter_bytes()
                try:
                    first_chunk = await anext(aiter)
                except StopAsyncIteration:
                    await resp.aclose()
                    logger.warning(f"Streaming attempt {attempt} received empty stream")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(
                        content=b'{"error":{"message":"Empty stream from upstream backend","type":"upstream_error"}}',
                        status_code=502,
                        media_type="application/json"
                    )
                except Exception as exc:
                    await resp.aclose()
                    logger.warning(f"Streaming attempt {attempt} read error: {exc}")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(
                        content=json.dumps({"error": {"message": f"Stream read error: {str(exc)}", "type": "upstream_error"}}).encode("utf-8"),
                        status_code=502,
                        media_type="application/json"
                    )

                if b"FreeTierError" in first_chunk or b"FreeUsageLimitError" in first_chunk or b"rate_limit_exceeded" in first_chunk:
                    rest_chunks = []
                    try:
                        async for chunk in aiter:
                            rest_chunks.append(chunk)
                    except Exception:
                        pass
                    await resp.aclose()
                    full_err = first_chunk + b"".join(rest_chunks)
                    logger.warning(f"Streaming attempt {attempt} detected disguised rate limit in stream")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(content=full_err, status_code=429, media_type="application/json")

                # Successful stream initialization: transfer slot ownership to stream generator
                slot_held_by_route = False

                async def stream_generator():
                    try:
                        yield first_chunk
                        async for chunk in aiter:
                            if chunk:
                                yield chunk
                    except (httpx.RequestError, httpx.HTTPError, asyncio.CancelledError) as exc:
                        logger.warning(f"Stream interrupted for user {user_id}: {exc}")
                    finally:
                        await resp.aclose()
                        await lease.release()

                return SafeStreamingResponse(
                    stream_generator(),
                    media_type="text/event-stream",
                    headers={
                        "X-Accel-Buffering": "no",
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive"
                    }
                )

        else:
            # Non-streaming request with transparent retry
            for attempt in range(2):
                session_to_use = incoming_session if (incoming_session and attempt == 0) else make_fresh_session()
                forward_headers = build_forward_headers(is_stream=False, session_id=session_to_use)

                try:
                    resp = await http_client.post(
                        TARGET_9ROUTER_URL,
                        json=body,
                        headers=forward_headers,
                        timeout=CLIENT_TIMEOUT
                    )
                except (httpx.RequestError, httpx.HTTPError) as exc:
                    logger.warning(f"Non-streaming attempt {attempt} failed with network error: {exc}")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(
                        content=json.dumps({"error": {"message": f"Bridge upstream network error: {str(exc)}", "type": "upstream_error"}}).encode("utf-8"),
                        status_code=502,
                        media_type="application/json"
                    )

                content = resp.content

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    logger.warning(f"Non-streaming attempt {attempt} received upstream status {resp.status_code}")
                    if attempt == 0:
                        await wait_retry_delay()
                        continue
                    return Response(content=content, status_code=resp.status_code, media_type="application/json")

                if resp.status_code == 200:
                    if b"FreeTierError" in content or b"FreeUsageLimitError" in content or b"rate_limit_exceeded" in content:
                        logger.warning(f"Non-streaming attempt {attempt} detected disguised rate limit error")
                        if attempt == 0:
                            await wait_retry_delay()
                            continue
                        return Response(content=content, status_code=429, media_type="application/json")

                    # Standardize usage statistics for Pydantic / OpenAI clients
                    try:
                        data = json.loads(content)
                        if isinstance(data, dict) and "usage" in data and isinstance(data["usage"], dict):
                            u = data["usage"]
                            p = u.get("prompt_tokens", 0)
                            c = u.get("completion_tokens", 0)
                            if "total_tokens" not in u:
                                u["total_tokens"] = p + c
                            content = json.dumps(data).encode("utf-8")
                    except Exception:
                        pass

                    return Response(content=content, status_code=200, media_type="application/json")

                # Any other non-retryable response (e.g. 400 Bad Request, 401 Unauthorized)
                return Response(content=content, status_code=resp.status_code, media_type="application/json")

    finally:
        if slot_held_by_route:
            await lease.release()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
