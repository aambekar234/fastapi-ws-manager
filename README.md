# fastapi-ws-manager

Poetry-based helper library for managing FastAPI websocket connections with:

- connection pool limits
- pluggable token verification (Supabase built-in)
- client-driven liveness (idle-timeout eviction)
- label-based limits with explicit takeover
- per-connection send and receive helpers

## Features

- works with current FastAPI releases
- plugs into any FastAPI websocket route with a single wrapper
- enforces a max connection pool and rejects overflow with `1013` plus a `Server busy` payload
- pluggable auth: ships an offline Supabase JWT verifier (`pyjwt[crypto]`), or bring your own (Auth0, Cognito, opaque-token introspection, …)
- drops idle clients: if a client sends nothing within `client_timeout_seconds`, the server closes the socket and frees the slot
- label-based limits with explicit takeover: group connections by your own labels (a document, a device, a user), cap each group, and let a newer connection supersede the oldest — but only when your handler asks for it

## Install

Install a published release artifact (wheel) straight from GitHub Releases — works
with both pip and Poetry:

```bash
# pip
pip install "https://github.com/aambekar234/fastapi-ws-manager/releases/download/v0.1.0/fastapi_ws_manager-0.1.0-py3-none-any.whl"

# Poetry (pin the release URL)
poetry add "https://github.com/aambekar234/fastapi-ws-manager/releases/download/v0.1.0/fastapi_ws_manager-0.1.0-py3-none-any.whl"
```

Or install from a git tag:

```bash
poetry add "git+https://github.com/aambekar234/fastapi-ws-manager.git@v0.1.0"
```

For local development in this repository:

```bash
poetry install
```

## Usage

```python
from fastapi import FastAPI

from fastapi_ws_manager import (
	SupabaseTokenVerifier,
	WebSocketManager,
	WebSocketManagerConfig,
)

app = FastAPI()

manager = WebSocketManager(
	verifier=SupabaseTokenVerifier(
		key="your-supabase-jwt-secret-or-public-key",
		algorithms=["HS256"],
		audience="authenticated",
		issuer="https://your-project.supabase.co/auth/v1",
	),
	config=WebSocketManagerConfig(
		max_connections=100,
		client_timeout_seconds=30,
	),
)


async def handle_socket(connection):
	while True:
		message = await connection.receive_json()
		if message["type"] == "ping":
			await connection.send_json({"type": "pong", "user_id": connection.user_id})


app.websocket("/ws")(manager.endpoint(handle_socket))
```

The client is responsible for liveness: it must send a message — typically a
`{"type": "ping"}` — at least once per `client_timeout_seconds`. Any received
message (not only pings) resets the timer. If the client goes silent past the
timeout, the server closes the connection with `WS_CLIENT_TIMEOUT` (`4408`) and reclaims the
pool slot. Set `client_timeout_seconds=0` to disable idle eviction.

Clients can provide their auth token with either:

- a `token` query parameter
- an `Authorization: Bearer <jwt>` header

## Custom verifiers

`SupabaseTokenVerifier` is just one implementation. The manager depends only on a
small structural interface, `TokenVerifier`: any object with a `verify(token)`
method that returns `TokenClaims` (or an awaitable of it) works — no base class to
inherit. Raise `TokenVerificationError` (or any `ValueError`) to reject a
connection; the manager closes it with `WS_AUTH_FAILED` (`1008`).

`verify` may be **sync or async**, so verifiers that need network I/O (fetching a
JWKS, calling a token-introspection endpoint) are first-class:

```python
from fastapi_ws_manager import (
	TokenClaims,
	TokenVerificationError,
	WebSocketManager,
)


class Auth0Verifier:
	def __init__(self, jwks_client, audience, issuer):
		self._jwks = jwks_client
		self._audience = audience
		self._issuer = issuer

	async def verify(self, token: str) -> TokenClaims:
		try:
			signing_key = await self._jwks.fetch(token)  # network I/O, awaited
			payload = decode_jwt(token, signing_key, self._audience, self._issuer)
		except Exception as exc:
			raise TokenVerificationError("Invalid auth token") from exc
		return TokenClaims(subject=payload["sub"], raw_claims=payload)


manager = WebSocketManager(verifier=Auth0Verifier(...))
```

The `subject` you put on `TokenClaims` is what `connection.user_id` returns.

## Label limits and takeover

A label is a grouping key you attach to a connection — a document id, a device id, a room. Configure
a limit per label name and a newer connection can take a group over from the connections already
holding it.

Labels are per-connection, so a route that uses them calls `fastapi_handler` (or `run`) directly
rather than `manager.endpoint(...)`:

```python
from fastapi_ws_manager import SUBJECT_LABEL, WebSocketManager, WebSocketManagerConfig

manager = WebSocketManager(
	verifier=...,
	config=WebSocketManagerConfig(
		max_connections=250,
		# label name -> max concurrent connections sharing a value for that label
		limits={"document-id": 1, SUBJECT_LABEL: 3},
		supersede_drain_seconds=2.0,
	),
)


async def handle_socket(connection):
	document_id = connection.labels["document-id"]

	# Authorize first — see the warning below.
	if not await may_open_document(connection.user_id, document_id):
		await connection.send_json({"type": "forbidden"})
		return

	displaced = await manager.claim(connection)
	await connection.send_json({"type": "ready", "took_over": len(displaced)})
	...


@app.websocket("/ws")
async def websocket_endpoint(websocket):
	await manager.fastapi_handler(
		websocket,
		handle_socket,
		labels={"document-id": websocket.query_params["document-id"]},
	)
```

Nothing is enforced until a handler calls `claim()`. `claim()` applies every configured limit on that
connection's behalf, closes the connections it displaces with `WS_SUPERSEDED`, and returns them. A
connection that never claims is invisible to the limits: it cannot displace anyone, and it cannot
block anyone.

**Call `claim` only after you have authorized the connection for its label values.** The manager
cannot check a label — it has no idea what a `document-id` means or who is allowed to open one.
`claim` is you vouching for it. Claim on a value you have not checked and anyone can knock anyone
else off by guessing an id.

**Labels are grouping keys, not credentials.** They decide who shares a limit and nothing else. Never
read one back as proof of anything.

The one label you do not supply is `subject` (exported as `SUBJECT_LABEL`): the manager sets it from
the verified token, so `limits={SUBJECT_LABEL: 3}` caps connections per user and the value cannot be
spoofed by a caller. Supplying it yourself — or a non-string key or value — raises `InvalidLabelError`.
That is a programming error, not a client error, so it is deliberately **not** a `ValueError`: it is
never laundered into an auth rejection, and it propagates instead of being reported to the client.

On the displaced side, `connection.superseded` tells a handler during teardown whether it was taken
over or whether the client simply vanished — usually different cleanup. If a replacement would read
state the displaced handler is still writing, set `supersede_drain_seconds`: `claim()` then waits up
to that long for each displaced handler to finish before it returns.

### Close codes

These are the library's wire contract, not settings. Clients are written against them, so they are
fixed and there is no knob to change them.

| Constant | Code | Meaning |
| --- | --- | --- |
| `WS_AUTH_FAILED` | `1008` | token missing or rejected |
| `WS_SERVER_BUSY` | `1013` | global pool full; the connection was never established |
| `WS_CLIENT_TIMEOUT` | `4408` | client fell silent past `client_timeout_seconds` |
| `WS_SUPERSEDED` | `4429` | a newer connection claimed this connection's label group |

**Clients must not auto-reconnect on `4429`.** If both sides reconnect when superseded, two clients
take the group from each other in a loop. Treat it as "someone else has this now", surface it, and
reconnect only on a deliberate user action.

### Two limits, two behaviours

The global pool **rejects** overflow with `WS_SERVER_BUSY`; a label limit **supersedes** the oldest
member of the group. The asymmetry is deliberate. A saturated server and a user opening a second tab
are different situations: the first means the server has nothing left to give, so the honest answer
is "not now"; the second means the user has moved, so the honest answer is to follow them.

Two consequences worth planning for:

- `max_connections` needs headroom above your label limits. A connection reserves its pool slot
  before its handler runs, so a connection rejected as busy never reaches `claim()` — on a saturated
  pool, takeover is unreachable.
- Limits are per process, counted in memory. Behind a load balancer they are per replica:
  `{"document-id": 1}` across four replicas permits up to four connections for one document. Pin a
  group to a single process, or treat the limit as per-replica.

## Notes

- The built-in Supabase verification is offline. The server uses the shared JWT secret or public key you configure.
- If you use asymmetric signing in Supabase, pass the public key and matching algorithm list to `SupabaseTokenVerifier`.
- Liveness is client-driven: the server never pings. Clients keep the connection open by sending a periodic message; the server only evicts clients that fall silent. Replying to pings with a `pong` is optional and handled in your own handler.

See the sample app in `examples/fastapi_app.py`.

