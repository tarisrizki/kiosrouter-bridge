import os
import json
from fastapi import FastAPI, Request, Response, Header, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import uvicorn

app = FastAPI(title="KiosanRouter Egress Bridge")
BRIDGE_SECRET = os.environ.get("BRIDGE_SECRET", "")
TARGET_9ROUTER_URL = os.environ.get("TARGET_9ROUTER_URL", "http://127.0.0.1:20130/v1/chat/completions")
INTERNAL_KEY = "sk-internal-heroku-bridge"

# Shared async HTTP client for high concurrency
http_client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0))

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

@app.get("/")
def index():
    return {
        "status": "online",
        "service": "kiosan-bridge",
        "pool": "12-warp-nodes",
        "version": "2.0.0"
    }

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": "ok"
    }

@app.get("/diag")
async def diag():
    import subprocess
    def tail_file(fpath):
        try:
            with open(fpath) as f:
                return f.readlines()[-40:]
        except Exception as e:
            return [str(e)]

    socks_probe = "not run"
    try:
        proc = subprocess.run(
            ["curl", "-m", "4", "-s", "--socks5", "127.0.0.1:40001", "https://cloudflare.com/cdn-cgi/trace"],
            capture_output=True, text=True, timeout=5
        )
        socks_probe = proc.stdout if proc.returncode == 0 else f"ERR code {proc.returncode}: {proc.stderr}"
    except Exception as e:
        socks_probe = str(e)

    router_probe = "not run"
    try:
        proc = subprocess.run(
            ["curl", "-m", "4", "-s", "http://127.0.0.1:20130/models"],
            capture_output=True, text=True, timeout=5
        )
        router_probe = proc.stdout[:200] if proc.returncode == 0 else f"ERR: {proc.stderr}"
    except Exception as e:
        router_probe = str(e)

    return {
        "singbox_log": tail_file("singbox.log"),
        "balancer_log": tail_file("balancer.log"),
        "router_log": tail_file("9router.log"),
        "socks_probe_40001": socks_probe,
        "router_probe_20130": router_probe
    }

def normalize_model(model_name: str) -> str:
    clean = model_name.strip()
    if clean.startswith("oc/"):
        base = clean[3:]
    else:
        base = clean
    if not base.endswith("-free"):
        base = f"{base}-free"
    return f"oc/{base}"

@app.post("/v1/chat/completions")
async def chat_completions(request: Request, x_bridge_secret: str = Header(None)):
    # 1. Validate Secret if configured
    auth_header = request.headers.get("Authorization", "")
    incoming_secret = x_bridge_secret or (auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else "")

    if BRIDGE_SECRET and incoming_secret != BRIDGE_SECRET:
        raise HTTPException(
            status_code=401,
            detail={"error": {"message": "Unauthorized: invalid bridge secret", "type": "authentication_error"}}
        )

    # 2. Parse request body
    body = await request.json()
    if "model" in body and isinstance(body["model"], str):
        body["model"] = normalize_model(body["model"])

    is_stream = body.get("stream", False)

    forward_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {INTERNAL_KEY}",
        "Accept": "application/json, text/event-stream" if is_stream else "application/json"
    }

    # 3. Dispatch to local 9router-go (which uses sing-box 12-WARP pool)
    if is_stream:
        async def stream_generator():
            try:
                async with http_client.stream(
                    "POST",
                    TARGET_9ROUTER_URL,
                    json=body,
                    headers=forward_headers,
                    timeout=120.0
                ) as resp:
                    async for chunk in resp.aiter_bytes():
                        if chunk:
                            yield chunk
            except Exception as e:
                err_payload = json.dumps({"error": {"message": f"Bridge stream error: {str(e)}", "type": "upstream_error"}})
                yield f"data: {err_payload}\n\ndata: [DONE]\n\n".encode("utf-8")

        return StreamingResponse(
            stream_generator(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"}
        )
    else:
        # Non-streaming request with automatic single retry on 429
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
                    # Rate limit encountered on this node, retry instantly to hit next node in pool
                    continue
                
                # Format usage for Strix AI / Pydantic compatibility
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
