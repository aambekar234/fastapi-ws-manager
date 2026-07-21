from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient

from fastapi_ws_manager import (
    SupabaseTokenVerifier,
    TokenClaims,
    TokenVerificationError,
    WebSocketManager,
    WebSocketManagerConfig,
)


SECRET = "super-secret-key-with-at-least-thirty-two-bytes"


def _token(subject: str = "user-1") -> str:
    now = datetime.now(UTC)
    return jwt.encode(
        {
            "sub": subject,
            "aud": "authenticated",
            "iss": "https://example.supabase.co/auth/v1",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
        },
        SECRET,
        algorithm="HS256",
    )


def _build_app(
    verifier,
    max_connections: int = 2,
    client_timeout_seconds: float = 30.0,
) -> FastAPI:
    app = FastAPI()
    manager = WebSocketManager(
        verifier=verifier,
        config=WebSocketManagerConfig(
            max_connections=max_connections,
            client_timeout_seconds=client_timeout_seconds,
        ),
    )

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        async def handler(connection):
            while True:
                message = await connection.receive_json()
                if message["type"] == "ping":
                    await connection.send_json({"type": "pong"})
                if message["type"] == "echo":
                    await connection.send_json(
                        {"type": "echo", "user_id": connection.user_id}
                    )
                if message["type"] == "hold":
                    await connection.send_json({"type": "holding"})
                    await connection.receive_text()

        await manager.fastapi_handler(websocket, handler)

    return app


def _app(max_connections: int = 2, client_timeout_seconds: float = 30.0) -> FastAPI:
    verifier = SupabaseTokenVerifier(
        key=SECRET,
        audience="authenticated",
        issuer="https://example.supabase.co/auth/v1",
    )
    return _build_app(
        verifier,
        max_connections=max_connections,
        client_timeout_seconds=client_timeout_seconds,
    )


def test_valid_token_connects_and_echoes() -> None:
    with TestClient(_app()) as client:
        with client.websocket_connect(f"/ws?token={_token()}") as websocket:
            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {"type": "echo", "user_id": "user-1"}


def test_authorization_header_token_connects() -> None:
    with TestClient(_app()) as client:
        with client.websocket_connect(
            "/ws",
            headers={"Authorization": f"Bearer {_token('header-user')}"},
        ) as websocket:
            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {
                "type": "echo",
                "user_id": "header-user",
            }


def test_invalid_token_is_rejected() -> None:
    with TestClient(_app()) as client:
        with client.websocket_connect("/ws?token=invalid") as websocket:
            assert websocket.receive_json() == {"detail": "Invalid auth token"}
            with pytest.raises(WebSocketDisconnect) as exc:
                websocket.receive_json()

        assert exc.value.code == 1008


def test_max_connections_returns_busy() -> None:
    with TestClient(_app(max_connections=1)) as client:
        release_event = threading.Event()

        def hold_connection() -> None:
            with client.websocket_connect(f"/ws?token={_token('holder')}") as websocket:
                websocket.send_json({"type": "hold"})
                assert websocket.receive_json() == {"type": "holding"}
                release_event.wait(timeout=1)
                websocket.send_text("release")

        thread = threading.Thread(target=hold_connection)
        thread.start()
        time.sleep(0.1)

        with client.websocket_connect(f"/ws?token={_token('blocked')}") as websocket:
            assert websocket.receive_json() == {"detail": "Server busy"}
            with pytest.raises(WebSocketDisconnect) as exc:
                websocket.receive_json()

        release_event.set()
        thread.join(timeout=1)
        assert exc.value.code == 1013


def test_idle_client_is_dropped() -> None:
    with TestClient(_app(client_timeout_seconds=0.2)) as client:
        with client.websocket_connect(f"/ws?token={_token()}") as websocket:
            # Client stays silent; the server should evict it once the
            # liveness timeout elapses.
            with pytest.raises(WebSocketDisconnect) as exc:
                websocket.receive_json()
        assert exc.value.code == 4408


def test_disabled_liveness_keeps_connection_open() -> None:
    with TestClient(_app(client_timeout_seconds=0)) as client:
        with client.websocket_connect(f"/ws?token={_token()}") as websocket:
            # Longer than any would-be timeout window; with liveness disabled
            # the silent connection must survive and still serve requests.
            time.sleep(0.3)
            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {"type": "echo", "user_id": "user-1"}


def test_active_client_stays_connected() -> None:
    with TestClient(_app(client_timeout_seconds=0.3)) as client:
        with client.websocket_connect(f"/ws?token={_token()}") as websocket:
            # Send pings spaced under the timeout; total elapsed time exceeds
            # the timeout, proving each message resets the liveness window.
            for _ in range(3):
                time.sleep(0.15)
                websocket.send_json({"type": "ping"})
                assert websocket.receive_json() == {"type": "pong"}

            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {"type": "echo", "user_id": "user-1"}


def test_idle_eviction_frees_pool_slot() -> None:
    with TestClient(_app(max_connections=1, client_timeout_seconds=0.2)) as client:
        # First client connects, then goes idle and gets evicted.
        with client.websocket_connect(f"/ws?token={_token('first')}") as websocket:
            with pytest.raises(WebSocketDisconnect):
                websocket.receive_json()

        # The freed slot lets a second client connect successfully.
        with client.websocket_connect(f"/ws?token={_token('second')}") as websocket:
            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {"type": "echo", "user_id": "second"}


class _StaticSyncVerifier:
    """Minimal custom verifier: accepts any non-'bad' token."""

    def verify(self, token: str) -> TokenClaims:
        if token == "bad":
            raise TokenVerificationError("nope")
        return TokenClaims(subject=f"sync-{token}", raw_claims={"sub": f"sync-{token}"})


class _AsyncVerifier:
    """Custom verifier doing async work (e.g. a network-backed lookup)."""

    async def verify(self, token: str) -> TokenClaims:
        await asyncio.sleep(0)
        if token == "bad":
            raise TokenVerificationError("nope")
        return TokenClaims(subject=f"async-{token}", raw_claims={"sub": f"async-{token}"})


def test_custom_sync_verifier_connects() -> None:
    with TestClient(_build_app(_StaticSyncVerifier())) as client:
        with client.websocket_connect("/ws?token=abc") as websocket:
            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {"type": "echo", "user_id": "sync-abc"}


def test_async_verifier_connects() -> None:
    with TestClient(_build_app(_AsyncVerifier())) as client:
        with client.websocket_connect("/ws?token=abc") as websocket:
            websocket.send_json({"type": "echo"})
            assert websocket.receive_json() == {"type": "echo", "user_id": "async-abc"}


def test_custom_verifier_rejection() -> None:
    with TestClient(_build_app(_StaticSyncVerifier())) as client:
        with client.websocket_connect("/ws?token=bad") as websocket:
            assert websocket.receive_json() == {"detail": "nope"}
            with pytest.raises(WebSocketDisconnect) as exc:
                websocket.receive_json()
        assert exc.value.code == 1008
