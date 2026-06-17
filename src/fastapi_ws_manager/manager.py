from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import json
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import WebSocket, WebSocketDisconnect, status

from .auth import TokenClaims, TokenVerifier

Handler = Callable[["ManagedWebSocket"], Awaitable[None]]

_active_manager: contextvars.ContextVar["WebSocketManager | None"] = (
    contextvars.ContextVar(
        "active_websocket_manager",
        default=None,
    )
)


@dataclass(slots=True)
class WebSocketManagerConfig:
    max_connections: int = 100
    # Drop a connection if the client sends nothing within this window.
    # Clients are expected to send a periodic ping (or any message) to stay
    # alive. Set to 0 (or less) to disable liveness checks entirely.
    client_timeout_seconds: float = 30.0
    client_timeout_close_code: int = 4408
    busy_close_code: int = status.WS_1013_TRY_AGAIN_LATER
    auth_close_code: int = status.WS_1008_POLICY_VIOLATION
    auth_query_param: str = "token"
    auth_header_name: str = "authorization"
    auth_header_prefix: str = "Bearer "


class ManagedWebSocket:
    def __init__(
        self,
        *,
        websocket: WebSocket,
        connection_id: str,
        claims: TokenClaims,
    ) -> None:
        self.websocket = websocket
        self.connection_id = connection_id
        self.claims = claims
        # Monotonic timestamp of the last message received from the client.
        self.last_seen: float = 0.0
        self.touch()

    @property
    def user_id(self) -> str:
        return self.claims.subject

    def touch(self) -> None:
        """Record that we just heard from the client."""
        try:
            self.last_seen = asyncio.get_running_loop().time()
        except RuntimeError:
            self.last_seen = 0.0

    async def send_json(self, payload: dict[str, Any]) -> None:
        await self.websocket.send_json(payload)

    async def send_text(self, payload: str) -> None:
        await self.websocket.send_text(payload)

    async def receive_json(self) -> dict[str, Any]:
        return json.loads(await self.receive_text())

    async def receive_text(self) -> str:
        data = await self.websocket.receive_text()
        self.touch()
        return data


class WebSocketManager:
    def __init__(
        self,
        *,
        verifier: TokenVerifier,
        config: WebSocketManagerConfig | None = None,
    ) -> None:
        self._verifier = verifier
        self.config = config or WebSocketManagerConfig()
        self._connections: dict[str, ManagedWebSocket] = {}
        self._active_count = 0
        self._lock = asyncio.Lock()

    @property
    def active_connections(self) -> int:
        return self._active_count

    def get_connection(self, connection_id: str) -> ManagedWebSocket | None:
        return self._connections.get(connection_id)

    def endpoint(self, handler: Handler) -> Callable[[WebSocket], Awaitable[None]]:
        async def websocket_endpoint(websocket: WebSocket) -> None:
            await self.fastapi_handler(websocket, handler)

        return websocket_endpoint

    async def run(self, websocket: WebSocket, handler: Handler) -> None:
        claims = await self._authenticate(websocket)

        await self._reserve_slot()
        try:
            await websocket.accept()
            managed = ManagedWebSocket(
                websocket=websocket,
                connection_id=str(uuid.uuid4()),
                claims=claims,
            )
            self._connections[managed.connection_id] = managed
            token = _active_manager.set(self)

            handler_task = asyncio.create_task(handler(managed))
            monitor_task = asyncio.create_task(self._monitor_liveness(managed))
            try:
                done, pending = await asyncio.wait(
                    {handler_task, monitor_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                # Surface a genuine handler error (a clean client disconnect is
                # expected, so it is swallowed).
                if handler_task in done:
                    exc = handler_task.exception()
                    if exc is not None and not isinstance(exc, WebSocketDisconnect):
                        raise exc
            finally:
                _active_manager.reset(token)
                self._connections.pop(managed.connection_id, None)
        finally:
            await self._release_slot()

    async def broadcast_json(self, payload: dict[str, Any]) -> None:
        tasks = [
            connection.send_json(payload) for connection in self._connections.values()
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def send_json(self, connection_id: str, payload: dict[str, Any]) -> None:
        connection = self._connections[connection_id]
        await connection.send_json(payload)

    async def close_all(
        self, code: int = status.WS_1001_GOING_AWAY, reason: str = "Server shutdown"
    ) -> None:
        tasks = [
            connection.websocket.close(code=code, reason=reason)
            for connection in self._connections.values()
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def _monitor_liveness(self, managed: ManagedWebSocket) -> None:
        """Close the connection if the client stops sending messages.

        The client is expected to send a periodic ping (or any message). Each
        received message refreshes ``managed.last_seen`` via ``touch()``; if the
        gap since the last message exceeds ``client_timeout_seconds`` the socket
        is closed and the slot is reclaimed.
        """
        timeout = self.config.client_timeout_seconds
        if timeout <= 0:
            return  # liveness checks disabled
        loop = asyncio.get_running_loop()
        while True:
            remaining = timeout - (loop.time() - managed.last_seen)
            if remaining <= 0:
                with contextlib.suppress(Exception):
                    await managed.websocket.close(
                        code=self.config.client_timeout_close_code,
                        reason="Client timeout",
                    )
                return
            await asyncio.sleep(remaining)

    async def _authenticate(self, websocket: WebSocket) -> TokenClaims:
        token = self._extract_token(websocket)
        if not token:
            raise ValueError("Missing auth token")
        result = self._verifier.verify(token)
        if inspect.isawaitable(result):
            result = await result
        return result

    def _extract_token(self, websocket: WebSocket) -> str | None:
        query_token = websocket.query_params.get(self.config.auth_query_param)
        if query_token:
            return query_token

        header_value = websocket.headers.get(self.config.auth_header_name)
        if not header_value:
            return None
        if header_value.startswith(self.config.auth_header_prefix):
            return header_value[len(self.config.auth_header_prefix) :].strip()
        return header_value.strip()

    async def _reserve_slot(self) -> None:
        async with self._lock:
            if self.active_connections >= self.config.max_connections:
                raise ConnectionLimitError()
            self._active_count += 1

    async def _release_slot(self) -> None:
        async with self._lock:
            self._active_count -= 1

    async def reject_busy(self, websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json({"detail": "Server busy"})
        await websocket.close(code=self.config.busy_close_code, reason="Server busy")

    async def reject_unauthorized(self, websocket: WebSocket, reason: str) -> None:
        await websocket.accept()
        await websocket.send_json({"detail": reason})
        await websocket.close(code=self.config.auth_close_code, reason=reason)

    async def fastapi_handler(self, websocket: WebSocket, handler: Handler) -> None:
        try:
            await self.run(websocket, handler)
        except ConnectionLimitError:
            await self.reject_busy(websocket)
        except ValueError as exc:
            await self.reject_unauthorized(websocket, str(exc))


class ConnectionLimitError(Exception):
    pass
