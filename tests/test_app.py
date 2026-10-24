import os
import json
import asyncio
import time
import pytest
import httpx
from unittest.mock import AsyncMock, patch

import app as app_module
from app import app, tracker, normalize_model, get_user_identifier

@pytest.fixture(autouse=True)
def setup_teardown():
    app_module.ACTIVE_CONCURRENCY.clear()
    app_module.BRIDGE_SECRET = ""
    app_module.RETRY_DELAY_MIN = 0.05
    app_module.RETRY_DELAY_MAX = 0.08
    yield
    app_module.ACTIVE_CONCURRENCY.clear()

def test_endpoints_metadata():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            res = await client.get("/")
            assert res.status_code == 200
            data = res.json()
            assert data["status"] == "online"
            assert data["service"] == "kiosan-bridge"
            assert data["concurrency_limit_per_user"] == 4
            assert data["version"] == "3.1.0"

            res = await client.get("/health")
            assert res.status_code == 200
            assert res.json()["status"] == "healthy"

            res = await client.get("/diag")
            assert res.status_code == 200
            assert "active_concurrency" in res.json()

            res = await client.get("/v1/models")
            assert res.status_code == 200
            model_ids = [m["id"] for m in res.json()["data"]]
            assert "deepseek-v4.1-flash-free" in model_ids
            assert "glm-5.3-flash-free" in model_ids
    asyncio.run(_test())

def test_bridge_secret_auth():
    async def _test():
        app_module.BRIDGE_SECRET = "top-secret-token"
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # No auth
            res = await client.post("/v1/chat/completions", json={"model": "test", "messages": []})
            assert res.status_code == 401
            assert res.json()["detail"]["error"]["type"] == "authentication_error"

            # Wrong auth
            res = await client.post(
                "/v1/chat/completions",
                json={"model": "test", "messages": []},
                headers={"Authorization": "Bearer wrong-token"}
            )
            assert res.status_code == 401

            # Correct via X-Bridge-Secret
            with patch.object(app_module.http_client, "post", new_callable=AsyncMock) as mock_post:
                mock_post.return_value = httpx.Response(200, json={"choices": []})
                res = await client.post(
                    "/v1/chat/completions",
                    json={"model": "test", "messages": []},
                    headers={"X-Bridge-Secret": "top-secret-token", "x-user-key": "u1"}
                )
                assert res.status_code == 200
    asyncio.run(_test())

def test_concurrency_limit_rock_solid():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            unblock_event = asyncio.Event()

            async def fake_upstream(*args, **kwargs):
                await unblock_event.wait()
                return httpx.Response(200, json={"choices": []})

            with patch.object(app_module.http_client, "post", side_effect=fake_upstream):
                # Launch 4 requests for user-A
                tasks = [
                    asyncio.create_task(
                        client.post(
                            "/v1/chat/completions",
                            json={"model": "test", "messages": []},
                            headers={"x-user-key": "user-A"}
                        )
                    )
                    for _ in range(4)
                ]
                await asyncio.sleep(0.05)

                assert await tracker.user_active("user-A") == 4

                # 5th request for user-A must fail with 429
                res5 = await client.post(
                    "/v1/chat/completions",
                    json={"model": "test", "messages": []},
                    headers={"x-user-key": "user-A"}
                )
                assert res5.status_code == 429
                err_data = res5.json()
                assert "Maksimal 4 request berjalan bersamaan" in err_data["detail"]["error"]["message"]

                # Meanwhile, user-B should be able to make a request without being blocked
                task_b = asyncio.create_task(
                    client.post(
                        "/v1/chat/completions",
                        json={"model": "test", "messages": []},
                        headers={"x-user-key": "user-B"}
                    )
                )
                await asyncio.sleep(0.05)
                assert await tracker.user_active("user-B") == 1

                # Unblock all upstream responses
                unblock_event.set()
                responses = await asyncio.gather(*tasks)
                for r in responses:
                    assert r.status_code == 200

                res_b = await task_b
                assert res_b.status_code == 200

                # After completion, all active counts must be 0
                assert await tracker.user_active("user-A") == 0
                assert await tracker.user_active("user-B") == 0
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_streaming_concurrency_slot_held_until_complete():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            active_during_stream = []

            class MockStreamResponse:
                status_code = 200
                headers = {"content-type": "text/event-stream"}

                async def aiter_bytes(self):
                    active_during_stream.append(await tracker.user_active("stream-user"))
                    yield b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n"
                    active_during_stream.append(await tracker.user_active("stream-user"))
                    yield b"data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n"
                    yield b"data: [DONE]\n\n"

                async def aclose(self):
                    pass

            with patch.object(app_module.http_client, "send", new_callable=AsyncMock) as mock_send:
                mock_send.return_value = MockStreamResponse()

                req = client.build_request(
                    "POST",
                    "/v1/chat/completions",
                    json={"model": "test", "messages": [], "stream": True},
                    headers={"x-user-key": "stream-user"}
                )
                resp = await client.send(req, stream=True)
                assert resp.status_code == 200

                lines = []
                async for line in resp.aiter_lines():
                    lines.append(line)
                await resp.aclose()

                assert any("first" in l for l in lines)
                assert any("done" in l for l in lines)
                assert active_during_stream == [1, 1]

                assert await tracker.user_active("stream-user") == 0
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_transparent_retry_429_success():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            async def mock_post(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return httpx.Response(429, json={"error": {"message": "Rate limit rotating proxy"}})
                return httpx.Response(200, json={"choices": [{"message": {"content": "Success after retry"}}], "usage": {"prompt_tokens": 10, "completion_tokens": 5}})

            with patch.object(app_module.http_client, "post", side_effect=mock_post):
                start = time.time()
                res = await client.post(
                    "/v1/chat/completions",
                    json={"model": "deepseek-v4.1-flash", "messages": []},
                    headers={"x-user-key": "retry-user"}
                )
                duration = time.time() - start

                assert res.status_code == 200
                assert call_count == 2
                assert duration >= 0.05
                data = res.json()
                assert data["choices"][0]["message"]["content"] == "Success after retry"
                assert data["usage"]["total_tokens"] == 15
    asyncio.run(_test())

def test_transparent_retry_502_success():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            async def mock_post(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return httpx.Response(502, content=b"Bad Gateway: upstream socks5 timeout")
                return httpx.Response(200, json={"choices": [{"message": {"content": "Recovered from 502"}}]})

            with patch.object(app_module.http_client, "post", side_effect=mock_post):
                res = await client.post(
                    "/v1/chat/completions",
                    json={"model": "glm-5.3-flash", "messages": []},
                    headers={"x-user-key": "retry-user-502"}
                )
                assert res.status_code == 200
                assert call_count == 2
                assert res.json()["choices"][0]["message"]["content"] == "Recovered from 502"
    asyncio.run(_test())

def test_transparent_retry_502_exhausted():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            async def mock_post(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                return httpx.Response(502, json={"error": {"message": "Persistent 502"}})

            with patch.object(app_module.http_client, "post", side_effect=mock_post):
                res = await client.post(
                    "/v1/chat/completions",
                    json={"model": "mimo-v2.6-flash", "messages": []},
                    headers={"x-user-key": "exhausted-user"}
                )
                assert res.status_code == 502
                assert call_count == 2
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_transparent_retry_transport_error():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            async def mock_post(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise httpx.ConnectError("Connection reset by peer")
                return httpx.Response(200, json={"choices": [{"message": {"content": "Connected on retry"}}]})

            with patch.object(app_module.http_client, "post", side_effect=mock_post):
                res = await client.post(
                    "/v1/chat/completions",
                    json={"model": "nemotron-3.5-super-vl", "messages": []},
                    headers={"x-user-key": "transport-user"}
                )
                assert res.status_code == 200
                assert call_count == 2
                assert res.json()["choices"][0]["message"]["content"] == "Connected on retry"
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_disguised_free_usage_limit_error_non_streaming():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            async def mock_post(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return httpx.Response(200, json={"error": {"message": "FreeUsageLimitError", "type": "FreeTierError"}})
                return httpx.Response(200, json={"choices": [{"message": {"content": "Clean response"}}]})

            with patch.object(app_module.http_client, "post", side_effect=mock_post):
                res = await client.post(
                    "/v1/chat/completions",
                    json={"model": "deepseek-v4.1-flash", "messages": []},
                    headers={"x-user-key": "disguise-user"}
                )
                assert res.status_code == 200
                assert call_count == 2
                assert res.json()["choices"][0]["message"]["content"] == "Clean response"
    asyncio.run(_test())

def test_streaming_disguised_error_retry():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            class FailStreamResponse:
                status_code = 200
                headers = {"content-type": "text/event-stream"}
                async def aiter_bytes(self):
                    yield b'{"error":{"message":"FreeUsageLimitError","type":"FreeTierError"}}'
                async def aread(self):
                    return b""
                async def aclose(self):
                    pass

            class GoodStreamResponse:
                status_code = 200
                headers = {"content-type": "text/event-stream"}
                async def aiter_bytes(self):
                    yield b'data: {"choices":[{"delta":{"content":"recovered stream"}}]}\n\n'
                    yield b'data: [DONE]\n\n'
                async def aclose(self):
                    pass

            async def mock_send(req, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return FailStreamResponse()
                return GoodStreamResponse()

            with patch.object(app_module.http_client, "send", side_effect=mock_send):
                req = client.build_request(
                    "POST",
                    "/v1/chat/completions",
                    json={"model": "deepseek-v4.1-flash", "messages": [], "stream": True},
                    headers={"x-user-key": "stream-disguise-user"}
                )
                resp = await client.send(req, stream=True)
                assert resp.status_code == 200
                assert call_count == 2
                lines = []
                async for l in resp.aiter_lines():
                    lines.append(l)
                await resp.aclose()
                assert any("recovered stream" in l for l in lines)
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_streaming_empty_stream_retry():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            call_count = 0

            class EmptyStreamResponse:
                status_code = 200
                headers = {"content-type": "text/event-stream"}
                async def aiter_bytes(self):
                    if False:
                        yield b""
                async def aclose(self):
                    pass

            class GoodStreamResponse:
                status_code = 200
                headers = {"content-type": "text/event-stream"}
                async def aiter_bytes(self):
                    yield b'data: {"choices":[{"delta":{"content":"recovered from empty"}}]}\n\n'
                    yield b'data: [DONE]\n\n'
                async def aclose(self):
                    pass

            async def mock_send(req, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return EmptyStreamResponse()
                return GoodStreamResponse()

            with patch.object(app_module.http_client, "send", side_effect=mock_send):
                req = client.build_request(
                    "POST",
                    "/v1/chat/completions",
                    json={"model": "mimo-v2.6-flash", "messages": [], "stream": True},
                    headers={"x-user-key": "empty-stream-user"}
                )
                resp = await client.send(req, stream=True)
                assert resp.status_code == 200
                assert call_count == 2
                lines = [l async for l in resp.aiter_lines()]
                await resp.aclose()
                assert any("recovered from empty" in l for l in lines)
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_invalid_json_releases_concurrency():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            res = await client.post(
                "/v1/chat/completions",
                content=b"invalid-json-content",
                headers={"x-user-key": "bad-json-user", "content-type": "application/json"}
            )
            assert res.status_code == 400
            assert await tracker.user_active("bad-json-user") == 0
            assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_chat_completions_route_alias():
    async def _test():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch.object(app_module.http_client, "post", new_callable=AsyncMock) as mock_post:
                mock_post.return_value = httpx.Response(200, json={"choices": []})
                res = await client.post(
                    "/chat/completions",  # Route alias without /v1
                    json={"model": "test", "messages": []},
                    headers={"x-user-key": "alias-user"}
                )
                assert res.status_code == 200
                assert await tracker.total_active() == 0
    asyncio.run(_test())

def test_model_normalization_and_headers():
    assert normalize_model("deepseek-v4.1-flash") == "oc/deepseek-v4.1-flash-free"
    assert normalize_model("oc/glm-5.3-flash") == "oc/glm-5.3-flash-free"
    assert normalize_model("oc/mimo-v2.6-flash-free") == "oc/mimo-v2.6-flash-free"
    assert normalize_model("nemotron-3.5-super-vl-free") == "oc/nemotron-3.5-super-vl-free"
