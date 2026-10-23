import os
import json
from fastapi import FastAPI, Request, Response, Header, HTTPException
from fastapi.responses import StreamingResponse
from curl_cffi import requests
import uvicorn

app = FastAPI(title="KiosanRouter Egress Bridge")
BRIDGE_SECRET = os.environ.get("BRIDGE_SECRET", "")

OPENCODE_URL = "https://opencode.ai/inference/openai/v1/chat/completions"

@app.get("/")
def index():
    return {
        "status": "online",
        "service": "kiosrouter-egress-bridge",
        "version": "1.0.0",
        "engine": "curl_cffi_chrome124_utls"
    }

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": "ok"
    }

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
    body["anonymous"] = True

    if "model" in body and isinstance(body["model"], str):
        clean_model = body["model"].rstrip("-free")
        body["model"] = f"{clean_model}-free"

    is_stream = body.get("stream", False)

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.19.4",
        "x-opencode-client": "cli",
        "Accept": "application/json, text/event-stream"
    }

    # 3. Dispatch with Chrome 124 stealth TLS fingerprint (bypasses Cloudflare WAF)
    if is_stream:
        def stream_generator():
            try:
                with requests.Session(impersonate="chrome124") as s:
                    with s.post(OPENCODE_URL, json=body, headers=headers, stream=True, timeout=120) as resp:
                        for chunk in resp.iter_content(chunk_size=1024):
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
        try:
            with requests.Session(impersonate="chrome124") as s:
                resp = s.post(OPENCODE_URL, json=body, headers=headers, timeout=120)
                return Response(
                    content=resp.content,
                    status_code=resp.status_code,
                    media_type="application/json"
                )
        except Exception as e:
            return Response(
                content=json.dumps({"error": {"message": f"Bridge dispatch error: {str(e)}", "type": "upstream_error"}}),
                status_code=502,
                media_type="application/json"
            )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
