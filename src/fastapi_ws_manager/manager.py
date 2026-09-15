from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping

from fastapi import WebSocket, WebSocketDisconnect, status

from .auth import TokenClaims, TokenVerifier

Handler = Callable[["ManagedWebSocket"], Awaitable[None]]

# Close codes are this library's wire contract, not a deployment preference:
# clients are written against these numbers, so they are fixed.
WS_AUTH_FAILED = 1008
WS_SERVER_BUSY = 1013
WS_CLIENT_TIMEOUT = 4408
WS_SUPERSEDED = 4429

# Reserved label. The manager sets it from the verified token subject, so a
# per-user limit is expressed like any other limit and cannot be spoofed by a
# caller.
SUBJECT_LABEL = "subject"

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
    # Label name -> maximum concurrent connections sharing a value for that
    # label. Enforced only for connections whose handler calls ``claim()``.
    limits: Mapping[str, int] = field(default_factory=dict)
    # How long ``claim()`` waits for each connection it displaces to finish
    # tearing down, letting a stateful handler persist before its replacement
    # reads the same state. 0 disables the wait.
    supersede_drain_seconds: float = 0.0
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
        labels: Mapping[str, str] | None = None,
    ) -> None:
        self.websocket = websocket
        self.connection_id = connection_id
        self.claims = claims
        # Grouping keys for label limits, including SUBJECT_LABEL. These are
        # not credentials: the manager cannot verify a label value, which is
        # why enforcement waits for the handler to call ``claim()``.
        self.labels: Mapping[str, str] = dict(labels or {})
        # Set by ``claim()``: this connection asked for the configured limits
        # to be applied on its behalf.
        self.claimed: bool = False
        # Set by ``claim()`` on a connection it displaces. A handler reads this
        # on teardown to tell "taken over" from "the client vanished".
        self.superseded: bool = False
        # Set once ``run()`` has dropped this connection and returned its slot.
        self.drained: asyncio.Event = asyncio.Event()
        # Set by ``claim()`` to make ``run()`` stop this connection. Closing the
        # socket does not wake a handler parked in receive(); see ``run()``.
        self._evict: asyncio.Event = asyncio.Event()
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


def _resolve_labels(labels: Mapping[str, str] | None) -> dict[str, str]:
    """Copy and check caller-supplied labels, or raise ``InvalidLabelError``."""
    resolved: dict[str, str] = {}
    for key, value in (labels or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise InvalidLabelError("Label keys and values must be strings")
        if key == SUBJECT_LABEL:
            raise InvalidLabelError(
                f"{SUBJECT_LABEL!r} is reserved and set by the manager"
            )
        resolved[key] = value
    return resolved


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
        """Wrap a handler as a websocket route.

        Labels are per-connection, so a route that needs them must call
        ``run`` or ``fastapi_handler`` directly instead of using this.
        """

        async def websocket_endpoint(websocket: WebSocket) -> None:
            await self.fastapi_handler(websocket, handler)

        return websocket_endpoint

    async def run(
        self,
        websocket: WebSocket,
        handler: Handler,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        # Validate first: before authenticating, so a programming error fails
        # the same way with or without a valid token, and before reserving a
        # slot, so it cannot leak one.
        resolved_labels = _resolve_labels(labels)
        claims = await self._authenticate(websocket)
        resolved_labels[SUBJECT_LABEL] = claims.subject

        await self._reserve_slot()
        managed: ManagedWebSocket | None = None
        try:
            await websocket.accept()
            managed = ManagedWebSocket(
                websocket=websocket,
                connection_id=str(uuid.uuid4()),
                claims=claims,
                labels=resolved_labels,
            )
            self._connections[managed.connection_id] = managed
            token = _active_manager.set(self)

            handler_task = asyncio.create_task(handler(managed))
            # The evict waiter is how ``claim()`` stops this connection: a
            # handler parked in receive() is deaf to its socket being closed,
            # but finishing any task in this set cancels it via FIRST_COMPLETED
            # below. It never resolves on its own.
            wait_set: set[asyncio.Task] = {
                handler_task,
                asyncio.create_task(managed._evict.wait()),
            }
            # With liveness disabled the monitor would return immediately and,
            # via FIRST_COMPLETED below, cancel the handler — so don't spawn it.
            if self.config.client_timeout_seconds > 0:
                wait_set.add(asyncio.create_task(self._monitor_liveness(managed)))
            try:
                done, pending = await asyncio.wait(
                    wait_set,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                # Surface a genuine handler error. A clean client disconnect is
                # expected, so it is swallowed; so is the fallout of a
                # supersede, which closes the socket under a running handler.
                if handler_task in done:
                    exc = handler_task.exception()
                    if (
                        exc is not None
                        and not isinstance(exc, WebSocketDisconnect)
                        and not managed.superseded
                    ):
                        raise exc
            finally:
                _active_manager.reset(token)
                self._connections.pop(managed.connection_id, None)
        finally:
            try:
                await self._release_slot()
            finally:
                # Only now is the connection fully gone: dropped from
                # _connections and holding no pool slot. ``claim()`` waits on
                # this, so it must not be set any earlier.
                if managed is not None:
                    managed.drained.set()

    async def claim(self, connection: ManagedWebSocket) -> list[ManagedWebSocket]:
        """Apply the configured limits on this connection's behalf.

        Call this only once you have authorized the connection for its label
        values: the manager cannot check a label, so ``claim`` is you vouching
        for it. Connections that never claim are invisible to the limits — they
        neither displace anyone nor count against anyone.

        Returns the connections displaced, each already closed with
        ``WS_SUPERSEDED``.
        """
        if connection.claimed or connection.superseded:
            return []
        connection.claimed = True

        # Select and mark synchronously. Nothing may interleave between
        # choosing a victim and marking it, so there is no await until this
        # loop has finished.
        victims: list[ManagedWebSocket] = []
        for label, maximum in self.config.limits.items():
            value = connection.labels.get(label)
            if value is None or maximum < 1:
                continue
            # _connections is insertion-ordered, so the group is oldest-first.
            group = [
                other
                for other in self._connections.values()
                if other is not connection
                and other.claimed
                and not other.superseded
                and other.labels.get(label) == value
            ]
            excess = len(group) + 1 - maximum
            if excess <= 0:
                continue
            for victim in group[:excess]:
                # Marking here also keeps a victim from being counted or picked
                # a second time by a later limit.
                victim.superseded = True
                victims.append(victim)

        for victim in victims:
            # The peer may already be gone.
            with contextlib.suppress(Exception):
                await victim.websocket.close(code=WS_SUPERSEDED, reason="Superseded")
            # After the close, so the frame is queued before run() can return.
            victim._evict.set()

        drain = self.config.supersede_drain_seconds
        if drain > 0:
            for victim in victims:
                # Victims run as independent tasks, so this cannot deadlock.
                # On timeout, proceed.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(victim.drained.wait(), timeout=drain)
        return victims

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
                        code=WS_CLIENT_TIMEOUT,
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
        await websocket.close(code=WS_SERVER_BUSY, reason="Server busy")

    async def reject_unauthorized(self, websocket: WebSocket, reason: str) -> None:
        await websocket.accept()
        await websocket.send_json({"detail": reason})
        await websocket.close(code=WS_AUTH_FAILED, reason=reason)

    async def fastapi_handler(
        self,
        websocket: WebSocket,
        handler: Handler,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        try:
            await self.run(websocket, handler, labels=labels)
        except ConnectionLimitError:
            await self.reject_busy(websocket)
        except ValueError as exc:
            await self.reject_unauthorized(websocket, str(exc))


class ConnectionLimitError(Exception):
    pass


class InvalidLabelError(Exception):
    """A caller supplied labels the manager will not accept.

    Deliberately not a ``ValueError``: ``fastapi_handler`` turns ``ValueError``
    into an auth rejection, so raising one here would report a library misuse to
    the client as a bad token and leak this message. This propagates instead.
    """
