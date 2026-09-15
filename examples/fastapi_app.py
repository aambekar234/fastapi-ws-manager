from fastapi import FastAPI, WebSocket

from fastapi_ws_manager import (
    SUBJECT_LABEL,
    ManagedWebSocket,
    SupabaseTokenVerifier,
    WebSocketManager,
    WebSocketManagerConfig,
)


app = FastAPI(title="fastapi-ws-manager example")

manager = WebSocketManager(
    verifier=SupabaseTokenVerifier(
        key="your-supabase-jwt-secret-or-public-key",
        algorithms=["HS256"],
        audience="authenticated",
        issuer="https://your-project.supabase.co/auth/v1",
    ),
    config=WebSocketManagerConfig(
        max_connections=250,
        # Drop clients that go silent for 30s. Clients keep the connection
        # alive by sending a periodic {"type": "ping"}.
        client_timeout_seconds=30,
        # One live connection per document, and at most three per user. The
        # manager fills in the "subject" label from the verified token.
        limits={"document-id": 1, SUBJECT_LABEL: 3},
        # Give a displaced handler a moment to finish persisting before its
        # replacement starts reading the same state.
        supersede_drain_seconds=2.0,
    ),
)


async def may_open_document(user_id: str, document_id: str) -> bool:
    """Stand-in for your own authorization check."""
    return True


async def socket_handler(connection: ManagedWebSocket) -> None:
    document_id = connection.labels.get("document-id")
    if document_id is None:
        await connection.send_json({"type": "error", "detail": "document-id required"})
        return

    # Authorize before claiming. claim() displaces whoever else holds this
    # document, so claiming a value the user may not open would let anyone
    # knock others off by guessing an id — the manager cannot check that for
    # you, which is why it waits for you to ask.
    if not await may_open_document(connection.user_id, document_id):
        await connection.send_json({"type": "forbidden", "document-id": document_id})
        return

    displaced = await manager.claim(connection)
    await connection.send_json({"type": "ready", "took_over": len(displaced)})

    try:
        while True:
            message = await connection.receive_json()
            if message["type"] == "ping":
                await connection.send_json({"type": "pong"})
            if message["type"] == "echo":
                await connection.send_json(
                    {
                        "type": "echo",
                        "user_id": connection.user_id,
                        "connection_id": connection.connection_id,
                    }
                )
            if message["type"] == "broadcast":
                await manager.broadcast_json(
                    {
                        "type": "broadcast",
                        "from": connection.user_id,
                        "message": message["message"],
                    }
                )
    finally:
        if connection.superseded:
            # A newer connection took this document over. Flush state for it to
            # pick up; don't treat this as the user going away.
            pass
        else:
            # The client went away for its own reasons.
            pass


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    # Labels are per-connection, so this route calls fastapi_handler directly
    # instead of manager.endpoint(socket_handler).
    document_id = websocket.query_params.get("document-id")
    await manager.fastapi_handler(
        websocket,
        socket_handler,
        labels={"document-id": document_id} if document_id else None,
    )
