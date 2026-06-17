from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Protocol, runtime_checkable

import jwt
from jwt import InvalidTokenError


@dataclass(slots=True)
class TokenClaims:
    subject: str
    raw_claims: dict[str, Any]


class TokenVerificationError(ValueError):
    """Raised by a verifier when a token is missing, invalid, or expired.

    Subclasses ``ValueError`` so it flows through ``WebSocketManager``'s existing
    rejection path. Custom verifiers should raise this (or any ``ValueError``) to
    reject a connection.
    """


@runtime_checkable
class TokenVerifier(Protocol):
    """Structural interface the manager depends on.

    Any object with a ``verify`` method that takes a token string and returns
    :class:`TokenClaims` (or an awaitable of it) satisfies this Protocol — no
    inheritance required. Raise :class:`TokenVerificationError` (or any
    ``ValueError``) to reject a connection.
    """

    def verify(self, token: str) -> TokenClaims | Awaitable[TokenClaims]: ...


class SupabaseTokenVerifier:
    def __init__(
        self,
        *,
        key: str,
        algorithms: list[str] | tuple[str, ...] = ("HS256",),
        audience: str | None = None,
        issuer: str | None = None,
    ) -> None:
        self._key = key
        self._algorithms = list(algorithms)
        self._audience = audience
        self._issuer = issuer

    def verify(self, token: str) -> TokenClaims:
        options = {"require": ["exp", "sub"]}
        try:
            payload = jwt.decode(
                token,
                key=self._key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
                options=options,
            )
        except InvalidTokenError as exc:
            raise TokenVerificationError("Invalid auth token") from exc

        subject = payload.get("sub")
        if not isinstance(subject, str) or not subject:
            raise TokenVerificationError("Invalid auth token")

        return TokenClaims(subject=subject, raw_claims=payload)
