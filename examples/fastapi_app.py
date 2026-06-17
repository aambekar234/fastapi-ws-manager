from fastapi import FastAPI

from fastapi_ws_manager import (
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
    ),
)


async def socket_handler(connection: ManagedWebSocket) -> None:
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


app.websocket("/ws")(manager.endpoint(socket_handler))
