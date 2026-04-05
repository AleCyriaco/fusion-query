"""
REST API server for fusion-query.

Provides a language-agnostic HTTP interface so any language (Java, Rust, JS, Go)
can use fusion-query as a backend service.

Start:
    fusion-query serve --port 8000

Or programmatically:
    from fusion_query.server import create_app
    app = create_app()
    # Use with uvicorn: uvicorn.run(app, port=8000)

Endpoints:
    POST /connect          — Create a named connection
    DELETE /connect/{name}  — Remove a connection
    GET /connections       — List active connections
    POST /query            — Execute a SQL query (single page)
    POST /query/all        — Execute and auto-paginate
    POST /setup            — Deploy proxy report to BIP
    GET /health            — Health check

All endpoints return JSON. The response format is identical to QueryResult.to_dict()
so that any driver can parse it with the same schema.

Pagination protocol over REST (for frontend/driver implementors):
    POST /query with {"sql": "...", "page_size": 1000, "page": 0}
    Response includes "page_info" with "has_next": true/false
    To get next page: POST /query with {"sql": "...", "page_size": 1000, "page": 1}
"""

from __future__ import annotations

from typing import Optional

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
except ImportError:
    raise ImportError(
        "REST server requires fastapi and uvicorn.\n"
        "Install with: pip install fusion-query[server]"
    )

from fusion_query.client import FusionClient
from fusion_query.auth import BasicAuth, OAuth2Auth
from fusion_query.catalog import ensure_report_deployed


# ---------------------------------------------------------------------------
# Request/Response models (Pydantic)
# ---------------------------------------------------------------------------

class ConnectRequest(BaseModel):
    """
    Create a connection to an Oracle Fusion instance.

    Fields:
        name:       Connection identifier (for managing multiple connections).
        url:        Oracle Fusion Cloud URL (e.g., https://xxxx.fa.us2.oraclecloud.com).
        username:   Username for Basic Auth.
        password:   Password for Basic Auth.
        report_path: BIP report path (optional, uses default if omitted).
        timeout:    HTTP timeout in seconds (default: 120).

    For OAuth2, use oauth2_token_url, oauth2_client_id, oauth2_client_secret instead.
    """
    name: str = "default"
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    report_path: str = "/Custom/FusionQuery/Proxy/v1/csv.xdo"
    timeout: int = 120


class QueryRequest(BaseModel):
    """
    Execute a SQL query.

    Fields:
        sql:          SQL statement to execute.
        connection:   Connection name (default: "default").
        page_size:    Rows per page, 1-1000 (default: 1000).
        page:         Page number, 0-based (default: 0).
        max_rows:     Max total rows for /query/all (default: None = unlimited).

    Pagination:
        The response includes page_info.has_next. If true, increment page
        and send another request to get the next batch. See README for
        the full pagination protocol.
    """
    sql: str
    connection: str = "default"
    page_size: int = 1000
    page: int = 0
    max_rows: Optional[int] = None


class SetupRequest(BaseModel):
    """Deploy the proxy report to BIP catalog."""
    connection: str = "default"
    folder: str = "/Custom/FusionQuery"


# ---------------------------------------------------------------------------
# App state
# ---------------------------------------------------------------------------

_connections: dict[str, FusionClient] = {}
_tested_clients: dict[str, FusionClient] = {}  # Clients that passed /test, waiting for /connect


def _build_client(req: ConnectRequest) -> FusionClient:
    """Build a FusionClient from a ConnectRequest."""
    if req.oauth2_token_url and req.oauth2_client_id:
        auth = OAuth2Auth(
            token_url=req.oauth2_token_url,
            client_id=req.oauth2_client_id,
            client_secret=req.oauth2_client_secret or "",
        )
    elif req.username and req.password:
        auth = BasicAuth(req.username, req.password)
    else:
        raise HTTPException(400, "Provide username+password or oauth2 credentials.")

    return FusionClient(
        url=req.url,
        auth=auth,
        report_path=req.report_path,
        timeout=req.timeout,
    )


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="fusion-query",
        description=(
            "Oracle Fusion Cloud SQL query engine via BI Publisher. "
            "Language-agnostic REST API for Java, Rust, JS, Go, or any HTTP client."
        ),
        version="0.1.0",
    )

    @app.get("/health")
    def health():
        return {"status": "ok", "connections": list(_connections.keys())}

    @app.post("/test")
    def test_connection(req: ConnectRequest):
        """
        Test a connection and auto-deploy the proxy report if needed.

        This is the endpoint frontends should call when the user clicks
        "Test Connection". It:
          1. Validates credentials against the BIP server
          2. Checks if the proxy report exists; deploys it if missing
          3. Runs SELECT 1 FROM DUAL to verify end-to-end

        The frontend flow should be:
          1. User fills in URL + username + password
          2. User clicks "Test Connection" → POST /test
          3. If success: enable "Save Connection" button
          4. User clicks "Save" → POST /connect
        """
        client = _build_client(req)
        result = client.test_connection()

        if result["success"]:
            # Store temporarily so /connect can skip re-testing
            _tested_clients[req.name] = client

        return result

    @app.post("/connect")
    def connect(req: ConnectRequest):
        """
        Save a connection. Must call POST /test first.

        If the connection was already tested (POST /test returned success),
        it's saved immediately. Otherwise, test is run automatically.
        """
        # Use pre-tested client if available
        if req.name in _tested_clients:
            _connections[req.name] = _tested_clients.pop(req.name)
            return {
                "status": "connected",
                "name": req.name,
                "url": req.url,
                "tested": True,
            }

        # No prior test — build, test, then save
        client = _build_client(req)
        result = client.test_connection()

        if not result["success"]:
            raise HTTPException(400, {
                "status": "test_failed",
                "detail": result["error"],
                "test_result": result,
            })

        _connections[req.name] = client
        return {
            "status": "connected",
            "name": req.name,
            "url": req.url,
            "tested": True,
            "proxy_was_installed": result.get("proxy_was_installed", False),
        }

    @app.delete("/connect/{name}")
    def disconnect(name: str):
        """Remove a named connection."""
        if name not in _connections:
            raise HTTPException(404, f"Connection '{name}' not found.")
        del _connections[name]
        return {"status": "disconnected", "name": name}

    @app.get("/connections")
    def list_connections():
        """List active connections."""
        return {
            "connections": [
                {"name": name, "url": client.url}
                for name, client in _connections.items()
            ]
        }

    @app.post("/query")
    def query(req: QueryRequest):
        """
        Execute a SQL query (single page).

        Returns a QueryResult with page_info for pagination.
        Check page_info.has_next to know if more pages are available.
        To fetch the next page, send another request with page incremented.
        """
        client = _connections.get(req.connection)
        if not client:
            raise HTTPException(404, f"Connection '{req.connection}' not found. POST /connect first.")

        result = client.query(
            sql=req.sql,
            page_size=req.page_size,
            page=req.page,
        )

        if result.error:
            raise HTTPException(500, result.to_dict())

        return result.to_dict()

    @app.post("/query/all")
    def query_all(req: QueryRequest):
        """
        Execute a SQL query and auto-paginate to fetch all rows.

        Set max_rows to limit the total. Without it, fetches everything.
        """
        client = _connections.get(req.connection)
        if not client:
            raise HTTPException(404, f"Connection '{req.connection}' not found. POST /connect first.")

        result = client.query_all(
            sql=req.sql,
            page_size=req.page_size,
            max_rows=req.max_rows,
        )

        if result.error:
            raise HTTPException(500, result.to_dict())

        return result.to_dict()

    @app.post("/setup")
    def setup(req: SetupRequest):
        """Deploy the proxy report to the BIP catalog."""
        client = _connections.get(req.connection)
        if not client:
            raise HTTPException(404, f"Connection '{req.connection}' not found. POST /connect first.")

        success = ensure_report_deployed(
            base_url=client.url,
            session=client._session,
            report_path=client.report_path,
        )

        if success:
            return {"status": "deployed", "report_path": client.report_path}
        else:
            raise HTTPException(500, "Failed to deploy proxy report. Check permissions.")

    return app
