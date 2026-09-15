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
    SUBJECT_LABEL,
    InvalidLabelError,
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


def _label_app(
    *,
    max_connections: int = 10,
    limits: dict[str, int] | None = None,
    supersede_drain_seconds: float = 0.0,
    teardown_delay: float = 0.0,
) -> tuple[FastAPI, WebSocketManager, list[list]]:
    """App whose handler claims on demand and records its own teardown.

    Returns the manager as well (``_build_app`` keeps it in a closure), plus a
    ``finished`` list each handler appends ``[user_id, superseded]`` to as it
    tears down. Liveness is off so nothing here depends on idle timeouts.
    """
    app = FastAPI()
    manager = WebSocketManager(
        verifier=SupabaseTokenVerifier(
            key=SECRET,
            audience="authenticated",
            issuer="https://example.supabase.co/auth/v1",
        ),
        config=WebSocketManagerConfig(
            max_connections=max_connections,
            client_timeout_seconds=0,
            limits=limits or {},
            supersede_drain_seconds=supersede_drain_seconds,
        ),
    )
    finished: list[list] = []

    async def handler(connection) -> None:
        try:
            while True:
                message = await connection.receive_json()
                if message["type"] == "echo":
                    await connection.send_json(
                        {"type": "echo", "user_id": connection.user_id}
                    )
                if message["type"] == "claim":
                    displaced = await manager.claim(connection)
                    # Sampled inside the handler, right after claim() returns,
                    # so the assertions need no sleeps on the client side.
                    await connection.send_json(
                        {
                            "type": "claimed",
                            "displaced": [c.user_id for c in displaced],
                            "active": manager.active_connections,
                            "finished": [list(entry) for entry in finished],
                        }
                    )
        finally:
            if teardown_delay:
                await asyncio.sleep(teardown_delay)
            finished.append([connection.user_id, connection.superseded])

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        labels: dict = {}
        for name in ("document-id", "device-id", "room"):
            value = websocket.query_params.get(name)
            if value is not None:
                labels[name] = value

        # Deliberate misuse, to prove the manager rejects it.
        bad = websocket.query_params.get("bad")
        if bad == "subject":
            labels[SUBJECT_LABEL] = "spoofed"
        elif bad == "key":
            labels[1] = "doc-1"
        elif bad == "value":
            labels["document-id"] = 2

        await manager.fastapi_handler(websocket, handler, labels=labels)

    return app, manager, finished


def _url(subject: str, labels: dict[str, str] | None = None, **extra: str) -> str:
    query = {"token": _token(subject), **(labels or {}), **extra}
    return "/ws?" + "&".join(f"{key}={value}" for key, value in query.items())


def _claim(websocket) -> dict:
    websocket.send_json({"type": "claim"})
    return websocket.receive_json()


def test_labels_without_limits_never_displace() -> None:
    app, _, _ = _label_app()
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-1"})
            ) as second:
                assert _claim(second)["displaced"] == []

                # Same label value, no configured limit: both stay up.
                first.send_json({"type": "echo"})
                assert first.receive_json() == {"type": "echo", "user_id": "first"}


def test_limit_of_one_displaces_the_earlier_claimant() -> None:
    app, _, _ = _label_app(limits={"document-id": 1})
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-1"})
            ) as second:
                assert _claim(second)["displaced"] == ["first"]

                with pytest.raises(WebSocketDisconnect) as exc:
                    first.receive_json()
                assert exc.value.code == 4429


def test_different_label_values_coexist() -> None:
    app, _, _ = _label_app(limits={"document-id": 1})
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-2"})
            ) as second:
                assert _claim(second)["displaced"] == []

                first.send_json({"type": "echo"})
                assert first.receive_json() == {"type": "echo", "user_id": "first"}


def test_unclaimed_connection_is_neither_displaced_nor_counted() -> None:
    app, _, _ = _label_app(limits={"document-id": 1})
    with TestClient(app) as client:
        # Colliding label value, but this connection never claims, so it is
        # invisible to the limit in both directions.
        with client.websocket_connect(
            _url("lurker", {"document-id": "doc-1"})
        ) as lurker:
            with client.websocket_connect(
                _url("claimant", {"document-id": "doc-1"})
            ) as claimant:
                assert _claim(claimant)["displaced"] == []

                lurker.send_json({"type": "echo"})
                assert lurker.receive_json() == {"type": "echo", "user_id": "lurker"}
                claimant.send_json({"type": "echo"})
                assert claimant.receive_json() == {
                    "type": "echo",
                    "user_id": "claimant",
                }


def test_subject_limit_displaces_oldest_connection_of_that_subject() -> None:
    app, _, _ = _label_app(limits={SUBJECT_LABEL: 1})
    with TestClient(app) as client:
        with client.websocket_connect(_url("alice")) as alice_first:
            assert _claim(alice_first)["displaced"] == []

            with client.websocket_connect(_url("bob")) as bob:
                assert _claim(bob)["displaced"] == []

                with client.websocket_connect(_url("alice")) as alice_second:
                    assert _claim(alice_second)["displaced"] == ["alice"]

                    with pytest.raises(WebSocketDisconnect) as exc:
                        alice_first.receive_json()
                    assert exc.value.code == 4429

                    # A different subject is untouched.
                    bob.send_json({"type": "echo"})
                    assert bob.receive_json() == {"type": "echo", "user_id": "bob"}


def test_claim_is_idempotent_and_returns_what_it_displaced() -> None:
    app, _, _ = _label_app(limits={"document-id": 1})
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-1"})
            ) as second:
                assert _claim(second)["displaced"] == ["first"]
                # Claiming again displaces nothing and reports nothing.
                assert _claim(second)["displaced"] == []


def test_reserved_subject_label_is_not_reported_as_an_auth_rejection() -> None:
    app, _, _ = _label_app()
    with TestClient(app) as client:
        # An auth rejection would connect, deliver {"detail": ...} and close
        # with 1008. A library misuse must not be laundered into that, so the
        # error propagates out of fastapi_handler instead.
        with pytest.raises(InvalidLabelError):
            with client.websocket_connect(_url("first", None, bad="subject")):
                pass


def test_non_string_label_key_is_rejected() -> None:
    app, _, _ = _label_app()
    with TestClient(app) as client:
        with pytest.raises(InvalidLabelError):
            with client.websocket_connect(_url("first", None, bad="key")):
                pass


def test_non_string_label_value_is_rejected() -> None:
    app, _, _ = _label_app()
    with TestClient(app) as client:
        with pytest.raises(InvalidLabelError):
            with client.websocket_connect(_url("first", None, bad="value")):
                pass


def test_superseded_connection_frees_its_pool_slot() -> None:
    app, _, _ = _label_app(
        max_connections=2,
        limits={"document-id": 1},
        supersede_drain_seconds=1.0,
    )
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-1"})
            ) as second:
                reply = _claim(second)
                assert reply["displaced"] == ["first"]
                # claim() waited for the drain, so the slot is already back.
                assert reply["active"] == 1

                with pytest.raises(WebSocketDisconnect):
                    first.receive_json()

                # The pool holds two; without the reclaimed slot this would be
                # rejected as busy.
                with client.websocket_connect(
                    _url("third", {"document-id": "doc-2"})
                ) as third:
                    third.send_json({"type": "echo"})
                    assert third.receive_json() == {"type": "echo", "user_id": "third"}


def test_drain_waits_for_the_displaced_handler_to_finish() -> None:
    app, _, _ = _label_app(
        limits={"document-id": 1},
        supersede_drain_seconds=1.0,
        teardown_delay=0.2,
    )
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-1"})
            ) as second:
                reply = _claim(second)
                assert reply["displaced"] == ["first"]
                # The displaced handler had already run its teardown, and knew
                # it was taken over rather than dropped by the client.
                assert reply["finished"] == [["first", True]]


def test_without_drain_claim_does_not_wait_for_the_displaced_handler() -> None:
    app, _, _ = _label_app(
        limits={"document-id": 1},
        supersede_drain_seconds=0.0,
        teardown_delay=0.2,
    )
    with TestClient(app) as client:
        with client.websocket_connect(_url("first", {"document-id": "doc-1"})) as first:
            assert _claim(first)["displaced"] == []

            with client.websocket_connect(
                _url("second", {"document-id": "doc-1"})
            ) as second:
                reply = _claim(second)
                assert reply["displaced"] == ["first"]
                # Counterpart to the test above: with no drain configured,
                # claim() returns before the displaced handler tears down.
                assert reply["finished"] == []
