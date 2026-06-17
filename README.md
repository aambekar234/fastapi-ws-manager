# fastapi-ws-manager

Poetry-based helper library for managing FastAPI websocket connections with:

- connection pool limits
- pluggable token verification (Supabase built-in)
- client-driven liveness (idle-timeout eviction)
- per-connection send and receive helpers

## Features

- works with current FastAPI releases
- plugs into any FastAPI websocket route with a single wrapper
- enforces a max connection pool and rejects overflow with `1013` plus a `Server busy` payload
- pluggable auth: ships an offline Supabase JWT verifier (`pyjwt[crypto]`), or bring your own (Auth0, Cognito, opaque-token introspection, …)
- drops idle clients: if a client sends nothing within `client_timeout_seconds`, the server closes the socket and frees the slot

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
timeout, the server closes the connection with code `4408` and reclaims the
pool slot. Set `client_timeout_seconds=0` to disable idle eviction.

Clients can provide their auth token with either:

- a `token` query parameter
- an `Authorization: Bearer <jwt>` header

## Custom verifiers

`SupabaseTokenVerifier` is just one implementation. The manager depends only on a
small structural interface, `TokenVerifier`: any object with a `verify(token)`
method that returns `TokenClaims` (or an awaitable of it) works — no base class to
inherit. Raise `TokenVerificationError` (or any `ValueError`) to reject a
connection; the manager closes it with the auth close code (`1008` by default).

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

## Releasing

Releases are fully automated by GitHub Actions. There is **no manual version bump
or tagging** — just merge to `main` with [conventional commit](https://www.conventionalcommits.org)
messages and the rest happens for you.

When you merge a pull request into `main`, the `release` workflow runs the test
suite and then [python-semantic-release](https://python-semantic-release.readthedocs.io)
inspects the commits since the last release. It computes the next version, updates
`pyproject.toml` and `CHANGELOG.md`, creates the `vX.Y.Z` tag, runs `poetry build`,
and publishes a GitHub Release with the wheel + sdist attached. If no
release-worthy commits are present, nothing is released.

The bump level comes from the commit messages (squash-merge is recommended so the
squash subject drives the bump):

| Commit prefix                       | Effect                                   |
| ----------------------------------- | ---------------------------------------- |
| `fix: ...`                          | patch release (`0.1.0` → `0.1.1`)        |
| `feat: ...`                         | minor release (`0.1.0` → `0.2.0`)        |
| `feat!: ...` / `BREAKING CHANGE:`   | minor while `0.x` (`major_on_zero` off)  |
| `chore:`, `docs:`, `test:`, ...     | no release                               |

The `ci` workflow runs the test suite on every pull request.

## Notes

- The built-in Supabase verification is offline. The server uses the shared JWT secret or public key you configure.
- If you use asymmetric signing in Supabase, pass the public key and matching algorithm list to `SupabaseTokenVerifier`.
- Liveness is client-driven: the server never pings. Clients keep the connection open by sending a periodic message; the server only evicts clients that fall silent. Replying to pings with a `pong` is optional and handled in your own handler.

See the sample app in `examples/fastapi_app.py`.

