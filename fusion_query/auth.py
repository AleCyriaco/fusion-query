"""
Authentication providers for Oracle Fusion Cloud.

Supported methods:
    - BasicAuth: Username + password (most common)
    - OAuth2Auth: Client credentials grant via Oracle IDCS

Implementing a new auth provider:
    Subclass AuthProvider and implement apply(session).
    This is the extension point for Java/Rust drivers to follow.
"""

from __future__ import annotations

import base64
import time
import threading
from abc import ABC, abstractmethod
from typing import Optional

import requests


class AuthProvider(ABC):
    """
    Base class for all authentication providers.

    Any frontend or driver (Python, Java, Rust) must implement this contract:
      - apply(session) -> mutates the requests.Session with auth headers/cookies.

    This is the interface to implement when porting to other languages.
    """

    @abstractmethod
    def apply(self, session: requests.Session) -> None:
        """Apply authentication to the HTTP session."""
        ...

    @abstractmethod
    def describe(self) -> dict:
        """Return a JSON-serializable description (for REST API / logging)."""
        ...


class BasicAuth(AuthProvider):
    """
    HTTP Basic Authentication (username + password).

    This is the simplest and most common method for Oracle Fusion Cloud.
    The BIP REST/SOAP APIs accept standard HTTP Basic Auth.

    Example:
        auth = BasicAuth("admin", "secret123")
        client = FusionClient(url, auth=auth)
    """

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    def apply(self, session: requests.Session) -> None:
        session.auth = (self.username, self.password)

    def describe(self) -> dict:
        return {"type": "basic", "username": self.username}

    def __repr__(self) -> str:
        return f"BasicAuth(username={self.username!r})"


class OAuth2Auth(AuthProvider):
    """
    OAuth2 Client Credentials grant via Oracle IDCS.

    Obtains and auto-refreshes a Bearer token from the IDCS token endpoint.
    Typically used for server-to-server integrations.

    Token endpoint is usually:
        https://<idcs-host>/oauth2/v1/token

    Example:
        auth = OAuth2Auth(
            token_url="https://idcs-xxx.identity.oraclecloud.com/oauth2/v1/token",
            client_id="abc123",
            client_secret="secret",
            scope="urn:opc:resource:consumer::all",
        )
        client = FusionClient(url, auth=auth)
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str = "urn:opc:resource:consumer::all",
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def _fetch_token(self) -> None:
        """Fetch a new access token from the IDCS token endpoint."""
        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        resp = requests.post(
            self.token_url,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "client_credentials",
                "scope": self.scope,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)
        # Refresh 60s before actual expiry to avoid edge cases
        self._expires_at = time.time() + expires_in - 60

    def _ensure_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        with self._lock:
            if self._access_token is None or time.time() >= self._expires_at:
                self._fetch_token()
            return self._access_token  # type: ignore[return-value]

    def apply(self, session: requests.Session) -> None:
        token = self._ensure_token()
        session.headers["Authorization"] = f"Bearer {token}"

    def describe(self) -> dict:
        return {
            "type": "oauth2",
            "token_url": self.token_url,
            "client_id": self.client_id,
        }

    def __repr__(self) -> str:
        return f"OAuth2Auth(client_id={self.client_id!r})"
