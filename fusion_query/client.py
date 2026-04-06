"""
Core Oracle Fusion Cloud query client.

Connects to Oracle BI Publisher (BIP) REST API to execute arbitrary SQL
against the Fusion database using a proxy report with a PL/SQL REF CURSOR.

Architecture (for driver implementors in Java/Rust/etc.):
    1. Encode:  SQL → gzip compress → base64 encode
    2. Request: POST /xmlpserver/services/rest/v1/reports/.../run
    3. Decode:  base64 decode response → parse CSV (pipe-delimited)
    4. Paginate: BIP has a 1000-row limit per request. This client wraps
       queries with OFFSET/FETCH to transparently paginate.

Pagination Protocol:
    See PageInfo and the README for the full pagination contract that
    all frontends (CLI, REST API, Java driver, Rust driver) must follow.
"""

from __future__ import annotations

import base64
import csv
import gzip
import io
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Union

import requests

from fusion_query.auth import AuthProvider, BasicAuth

logger = logging.getLogger("fusion_query.client")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class PageInfo:
    """
    Pagination state — the universal contract for all frontends/drivers.

    Every query response includes a PageInfo. Frontends inspect it to decide
    whether to fetch more data. This same structure must be implemented
    identically in Java/Rust/JS drivers.

    Fields:
        page:          Current page number (0-based).
        page_size:     Rows requested per page (default 1000, max 1000).
        offset:        Row offset used in this page's SQL (page * page_size).
        rows_returned: Actual rows returned in this page (may be < page_size
                       on the last page).
        has_next:      True if rows_returned == page_size, meaning there are
                       likely more rows. False means this is the last page.
        total_fetched: Cumulative rows fetched across all pages so far.
        max_rows:      Upper limit set by the caller (None = unlimited).
        exhausted:     True if max_rows was reached or has_next is False.

    Pagination algorithm for any driver:
        page = client.query(sql, page_size=1000)
        all_rows = page.rows
        while page.page_info.has_next:
            page = client.fetch_next(page)
            all_rows.extend(page.rows)
    """

    page: int = 0
    page_size: int = 1000
    offset: int = 0
    rows_returned: int = 0
    has_next: bool = False
    total_fetched: int = 0
    max_rows: Optional[int] = None
    exhausted: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class QueryResult:
    """
    Result of a single query page.

    Fields:
        columns:        Column names from the CSV header.
        rows:           List of dicts (column_name -> value).
        page_info:      Pagination state for this page.
        sql:            The original SQL (without pagination wrapper).
        execution_time: Time in seconds for this page request.
        error:          Error message if the query failed, else None.

    For driver implementors:
        This is the canonical response shape. Java/Rust drivers should
        return an equivalent structure with identical field names.
    """

    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    page_info: PageInfo = field(default_factory=PageInfo)
    sql: str = ""
    execution_time: float = 0.0
    error: Optional[str] = None

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def has_next(self) -> bool:
        return self.page_info.has_next

    def to_dict(self) -> dict:
        """JSON-serializable dict — used by the REST API server."""
        return {
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "page_info": self.page_info.to_dict(),
            "sql": self.sql,
            "execution_time": self.execution_time,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Encoding helpers (encoding pipeline)
# ---------------------------------------------------------------------------

def encode_sql(sql: str) -> str:
    """
    Encode SQL for transmission to the BIP proxy report.

    Pipeline: SQL text → UTF-8 bytes → gzip compress → base64 encode.

    The BIP Data Model reverses this:
        base64 decode → LZ uncompress → cast to VARCHAR2 → OPEN CURSOR
    """
    sql_bytes = sql.encode("utf-8")
    compressed = gzip.compress(sql_bytes)
    encoded = base64.b64encode(compressed).decode("ascii")
    return encoded


def _wrap_paginated_sql(sql: str, offset: int, fetch: int) -> str:
    """
    Wrap a SQL statement with OFFSET/FETCH for pagination.

    Oracle Fusion DB supports the SQL:2008 OFFSET/FETCH syntax:
        SELECT ... FROM ... OFFSET n ROWS FETCH NEXT m ROWS ONLY

    We wrap the user's SQL as a subquery to avoid conflicts with
    any existing ORDER BY, OFFSET, or FETCH in the original SQL.

    NOTE: Without an ORDER BY, Oracle does not guarantee row order.
    For deterministic pagination, the caller should include ORDER BY
    in their SQL. This wrapper preserves whatever ordering the
    original query specifies.
    """
    # Strip trailing semicolons and whitespace
    sql = sql.strip().rstrip(";").strip()

    return (
        f"SELECT * FROM (\n{sql}\n) fusion_query_page\n"
        f"OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY"
    )


# ---------------------------------------------------------------------------
# BIP REST API interaction
# ---------------------------------------------------------------------------

# Default report path for the FusionQueryProxy report.
# This is the path where the auto-deploy installs the report.
# Can be overridden in FusionClient constructor.
DEFAULT_REPORT_PATH = "/Custom/FusionQuery/Proxy/v1/csv.xdo"

# BIP enforces a max of ~1000 rows per report execution.
MAX_PAGE_SIZE = 1000


def _build_report_request(encoded_sql: str) -> dict:
    """
    Build the JSON body for the BIP REST API /run endpoint.

    The proxy report (FusionQueryProxy) accepts one parameter:
        P_B64_CONTENT: base64-encoded gzipped SQL string.
    """
    return {
        "byPassCache": True,
        "flattenXML": False,
        "attributeFormat": "csv",
        "parameterNameValues": {
            "listOfParamNameValues": {
                "item": [
                    {
                        "name": "P_B64_CONTENT",
                        "values": {"item": [encoded_sql]},
                    }
                ]
            }
        },
    }


def _parse_csv_response(raw_bytes: bytes) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Parse the pipe-delimited CSV returned by the BIP report.

    The FusionQueryProxy report uses '|' as delimiter and CSV format.
    Returns (columns, rows) where rows is a list of dicts.
    """
    text = raw_bytes.decode("utf-8", errors="replace")

    if not text.strip():
        return [], []

    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    columns = reader.fieldnames or []
    rows = [dict(row) for row in reader]
    return list(columns), rows


# ---------------------------------------------------------------------------
# FusionClient
# ---------------------------------------------------------------------------

class FusionClient:
    """
    Oracle Fusion Cloud SQL query client.

    Executes arbitrary SQL against the Fusion database via BI Publisher,
    with automatic pagination for large result sets.

    Args:
        url:          Oracle Fusion Cloud instance URL.
                      Example: "https://xxxx.fa.us2.oraclecloud.com"
        auth:         An AuthProvider instance, or (username, password) tuple.
        report_path:  BIP catalog path to the proxy report.
                      Default: /Custom/FusionQuery/Proxy/v1/csv.xdo
        timeout:      HTTP request timeout in seconds (default: 120).
        verify_ssl:   Verify SSL certificates (default: True).

    Example:
        client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "user", "pass")

        # Single page (up to 1000 rows):
        result = client.query("SELECT * FROM PER_USERS")

        # Manual pagination:
        page = client.query("SELECT * FROM PER_USERS ORDER BY USER_ID", page_size=500)
        while page.has_next:
            page = client.fetch_next(page)

        # Auto-paginate (fetches ALL rows across pages):
        result = client.query_all("SELECT * FROM PER_USERS ORDER BY USER_ID")

        # Auto-paginate with limit:
        result = client.query_all("SELECT * FROM PER_USERS ORDER BY USER_ID",
                                  max_rows=5000)
    """

    def __init__(
        self,
        url: str,
        auth: Union[AuthProvider, str, tuple] = None,
        password: Optional[str] = None,
        report_path: str = DEFAULT_REPORT_PATH,
        timeout: int = 120,
        verify_ssl: bool = True,
    ) -> None:
        # Normalize URL
        self.url = url.rstrip("/")

        # Flexible auth: FusionClient(url, "user", "pass") or FusionClient(url, auth=BasicAuth(...))
        if isinstance(auth, str) and password is not None:
            self._auth = BasicAuth(auth, password)
        elif isinstance(auth, tuple) and len(auth) == 2:
            self._auth = BasicAuth(auth[0], auth[1])
        elif isinstance(auth, AuthProvider):
            self._auth = auth
        else:
            raise ValueError(
                "auth must be an AuthProvider, a (username, password) tuple, "
                "or pass username as second arg and password as third arg."
            )

        self.report_path = report_path
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self._auto_deploy = True  # Auto-deploy proxy report on first use
        self._deploy_checked = False  # Only check once per session
        self._use_soap = False  # Prefer REST, auto-switch to SOAP if REST fails

        # Build session
        self._session = requests.Session()
        self._session.verify = verify_ssl
        self._auth.apply(self._session)

    @property
    def _username(self) -> Optional[str]:
        """Extract username from the auth provider (if BasicAuth)."""
        if isinstance(self._auth, BasicAuth):
            return self._auth.username
        return None

    def _ensure_proxy_deployed(self) -> None:
        """
        Auto-deploy the proxy report if it doesn't exist.

        Called automatically before the first query. Idempotent — only
        checks once per client session.

        Uses the same strategy as test_connection(): SOAP-based deploy
        to user's personal folder, works on all instances including OCS.
        """
        if self._deploy_checked or not self._auto_deploy:
            return

        self._deploy_checked = True

        # Delegate to test_connection which handles SOAP deploy
        try:
            result = self.test_connection()
            # test_connection already sets self.report_path and self._use_soap
        except Exception as exc:
            logger.warning(
                "Auto-deploy check failed (non-fatal): %s. "
                "If queries fail, run: fusion-query setup --url %s --user <user>",
                exc,
                self.url,
            )

    @property
    def _run_url(self) -> str:
        """BIP REST API endpoint for running reports."""
        # URL-encode the report path for the REST endpoint
        path = self.report_path
        if path.startswith("/"):
            path = path[1:]
        return f"{self.url}/xmlpserver/services/rest/v1/reports/{path}/run"

    def test_connection(self) -> dict:
        """
        Test connectivity and auto-deploy the proxy report if needed.

        This is the method frontends should call when the user clicks
        "Test Connection". It performs three steps:

        1. Verifies HTTP connectivity to the BIP server
        2. Checks if the proxy report exists; deploys it if missing
        3. Runs 'SELECT 1 FROM DUAL' to confirm end-to-end functionality

        Returns a dict with status details (for frontend display):
            {
                "success": True/False,      # True when connectivity + auth OK
                "connectivity": "ok" / "failed",
                "proxy_deployed": True/False,
                "proxy_was_installed": True/False,  # True if deployed NOW
                "query_test": "ok" / "failed" / "skipped",
                "query_ready": True/False,  # True only when full SQL pipeline works
                "error": None / "error message",
                "warning": None / "warning message",
            }

        ``success`` reflects whether credentials are valid and the server is
        reachable — this is what frontends should use to enable "Save Connection".
        ``query_ready`` is True only when the proxy report is deployed and a
        test query ran successfully.  Frontends can show this as an extra
        indicator but should NOT block saving on it.
        """
        status = {
            "success": False,
            "connectivity": "unknown",
            "proxy_deployed": False,
            "proxy_was_installed": False,
            "query_test": "skipped",
            "query_ready": False,
            "error": None,
            "warning": None,
        }

        # Step 1: Test connectivity via SOAP (works on all instances incl. OCS)
        from fusion_query.soap import SOAPCatalog, SOAPReportService
        from fusion_query.catalog import _user_report_path

        username = self._username
        password = self._auth.password if isinstance(self._auth, BasicAuth) else None

        if not username or not password:
            status["error"] = "SOAP API requires BasicAuth credentials."
            return status

        soap_catalog = SOAPCatalog(
            self.url, self._session, username, password, self.timeout
        )

        # Test connectivity by listing root folder via SOAP
        try:
            soap_ok = soap_catalog.object_exists("/Custom")  # lightweight check
            status["connectivity"] = "ok"
            status["success"] = True
        except requests.exceptions.ConnectionError:
            status["connectivity"] = "failed"
            status["error"] = f"Cannot connect to {self.url}. Check the URL."
            return status
        except requests.exceptions.Timeout:
            status["connectivity"] = "failed"
            status["error"] = "Connection timed out. Check the URL and network."
            return status
        except requests.exceptions.RequestException as exc:
            status["connectivity"] = "failed"
            status["error"] = f"Connection error: {exc}"
            return status

        # Step 2: Check / deploy proxy report via SOAP
        # Check user's personal folder first, then /Custom/
        user_path = _user_report_path(username)
        deployed_path = None

        if soap_catalog.object_exists(user_path):
            deployed_path = user_path
        elif soap_catalog.object_exists(self.report_path):
            deployed_path = self.report_path
        else:
            # Deploy to user's personal folder (no BI Admin needed)
            from fusion_query.catalog import _user_folder, _TEMPLATE_PATH
            import zipfile, io, re as _re, os

            target = _user_folder(username)
            template = str(_TEMPLATE_PATH)

            if os.path.exists(template):
                soap_catalog.create_folder(target)
                soap_catalog.create_folder(f"{target}/v1")

                with zipfile.ZipFile(template) as zf:
                    dm_content = report_content = None
                    for name in zf.namelist():
                        if name.endswith("dm.xdmz"):
                            dm_content = zf.read(name)
                        elif name.endswith("csv.xdoz"):
                            report_content = zf.read(name)

                if dm_content and report_content:
                    # Patch report to point to new DM path
                    new_dm = f"{target}/v1/dm.xdm"
                    zf_in = zipfile.ZipFile(io.BytesIO(report_content))
                    buf = io.BytesIO()
                    zf_out = zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED)
                    for entry in zf_in.namelist():
                        data = zf_in.read(entry)
                        if entry == "_report.xdo":
                            text = data.decode("utf-8")
                            text = _re.sub(
                                r'(<dataModel\s+url=")[^"]*(")',
                                rf"\g<1>{new_dm}\2",
                                text,
                            )
                            data = text.encode("utf-8")
                        zf_out.writestr(entry, data)
                    zf_out.close()
                    report_content = buf.getvalue()

                    dm_ok = soap_catalog.upload_object(
                        f"{target}/v1/dm.xdm", dm_content, "xdmz"
                    )
                    rpt_ok = soap_catalog.upload_object(
                        f"{target}/v1/csv.xdo", report_content, "xdoz"
                    )
                    if dm_ok and rpt_ok:
                        deployed_path = user_path
                        status["proxy_was_installed"] = True

        if deployed_path:
            status["proxy_deployed"] = True
            self.report_path = deployed_path
            self._use_soap = True
        else:
            status["warning"] = "Could not deploy proxy report."
            self._deploy_checked = True
            return status

        self._deploy_checked = True

        # Step 3: Run test query via SOAP
        try:
            soap_report = SOAPReportService(
                self.url, self._session, username, password, self.timeout
            )
            sql = "SELECT 1 AS OK FROM DUAL"
            encoded = encode_sql(sql)
            raw = soap_report.run_report(self.report_path, encoded)
            status["query_test"] = "ok"
            status["query_ready"] = True
            self._use_soap = True
        except Exception as exc:
            status["query_test"] = "failed"
            status["warning"] = f"Query test failed: {exc}"

        return status

    def query(
        self,
        sql: str,
        page_size: int = MAX_PAGE_SIZE,
        page: int = 0,
        offset: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute a SQL query and return a single page of results.

        Args:
            sql:        The SQL to execute.
            page_size:  Max rows per page (1-1000, default 1000).
            page:       Page number (0-based, default 0).
            offset:     Explicit row offset (overrides page * page_size).

        Returns:
            QueryResult with rows and page_info for pagination.

        Pagination contract:
            - Check result.page_info.has_next to know if more pages exist.
            - Use client.fetch_next(result) to get the next page.
            - Or use client.query_all(sql) to auto-fetch all pages.
        """
        # Auto-deploy on first query only if test_connection() was never called
        self._ensure_proxy_deployed()

        page_size = min(max(1, page_size), MAX_PAGE_SIZE)
        if offset is None:
            offset = page * page_size

        # Wrap SQL with pagination
        paginated_sql = _wrap_paginated_sql(sql, offset, page_size)

        # Encode and execute
        t0 = time.time()
        encoded = encode_sql(paginated_sql)

        # Use SOAP if enabled (OCS instances), REST otherwise
        if self._use_soap:
            return self._query_soap(sql, encoded, page, page_size, offset, t0)

        body = _build_report_request(encoded)

        try:
            resp = self._session.post(
                self._run_url,
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            elapsed = time.time() - t0
            error_detail = ""
            try:
                error_detail = exc.response.text[:500]
            except Exception:
                pass

            # Auto-switch to SOAP on REST failure
            if not self._use_soap and self._username:
                logger.info("REST API failed, switching to SOAP")
                self._use_soap = True
                return self._query_soap(sql, encoded, page, page_size, offset, t0)

            return QueryResult(
                sql=sql,
                execution_time=elapsed,
                error=f"HTTP {exc.response.status_code}: {error_detail}",
                page_info=PageInfo(
                    page=page, page_size=page_size, offset=offset,
                    exhausted=True,
                ),
            )
        except requests.exceptions.RequestException as exc:
            elapsed = time.time() - t0
            return QueryResult(
                sql=sql,
                execution_time=elapsed,
                error=str(exc),
                page_info=PageInfo(
                    page=page, page_size=page_size, offset=offset,
                    exhausted=True,
                ),
            )

        elapsed = time.time() - t0

        # Decode response: BIP returns base64-encoded report output
        try:
            response_data = resp.json()
            report_bytes_b64 = response_data.get("reportBytes", "")
            if not report_bytes_b64:
                return QueryResult(
                    sql=sql,
                    execution_time=elapsed,
                    error="Empty response from BIP (no reportBytes).",
                    page_info=PageInfo(
                        page=page, page_size=page_size, offset=offset,
                        exhausted=True,
                    ),
                )
            raw_csv = base64.b64decode(report_bytes_b64)
        except Exception as exc:
            return QueryResult(
                sql=sql,
                execution_time=elapsed,
                error=f"Failed to decode BIP response: {exc}",
                page_info=PageInfo(
                    page=page, page_size=page_size, offset=offset,
                    exhausted=True,
                ),
            )

        # Parse CSV
        columns, rows = _parse_csv_response(raw_csv)
        rows_returned = len(rows)
        has_next = rows_returned >= page_size

        page_info = PageInfo(
            page=page,
            page_size=page_size,
            offset=offset,
            rows_returned=rows_returned,
            has_next=has_next,
            total_fetched=offset + rows_returned,
            max_rows=None,
            exhausted=not has_next,
        )

        return QueryResult(
            columns=columns,
            rows=rows,
            page_info=page_info,
            sql=sql,
            execution_time=elapsed,
        )

    def _query_soap(
        self, sql: str, encoded: str,
        page: int, page_size: int, offset: int, t0: float,
    ) -> QueryResult:
        """Execute a query via SOAP ReportService (for OCS instances)."""
        from fusion_query.soap import SOAPReportService

        username = self._username
        password = self._auth.password if isinstance(self._auth, BasicAuth) else None

        try:
            soap = SOAPReportService(
                self.url, self._session, username, password, self.timeout
            )
            raw_csv = soap.run_report(self.report_path, encoded)
        except Exception as exc:
            elapsed = time.time() - t0
            return QueryResult(
                sql=sql,
                execution_time=elapsed,
                error=str(exc),
                page_info=PageInfo(
                    page=page, page_size=page_size, offset=offset,
                    exhausted=True,
                ),
            )

        elapsed = time.time() - t0
        columns, rows = _parse_csv_response(raw_csv)
        rows_returned = len(rows)
        has_next = rows_returned >= page_size

        page_info = PageInfo(
            page=page,
            page_size=page_size,
            offset=offset,
            rows_returned=rows_returned,
            has_next=has_next,
            total_fetched=offset + rows_returned,
            max_rows=None,
            exhausted=not has_next,
        )

        return QueryResult(
            columns=columns,
            rows=rows,
            page_info=page_info,
            sql=sql,
            execution_time=elapsed,
        )

    def fetch_next(self, previous: QueryResult) -> QueryResult:
        """
        Fetch the next page of results from a previous QueryResult.

        Args:
            previous: A QueryResult with page_info.has_next == True.

        Returns:
            The next QueryResult page.

        Raises:
            StopIteration: If there are no more pages.
        """
        if not previous.page_info.has_next:
            raise StopIteration("No more pages available.")

        next_page = previous.page_info.page + 1
        next_offset = previous.page_info.offset + previous.page_info.page_size

        result = self.query(
            sql=previous.sql,
            page_size=previous.page_info.page_size,
            page=next_page,
            offset=next_offset,
        )

        # Carry forward the cumulative total
        result.page_info.total_fetched = (
            previous.page_info.total_fetched + result.page_info.rows_returned
        )

        # Check max_rows limit if it was set
        if previous.page_info.max_rows is not None:
            result.page_info.max_rows = previous.page_info.max_rows
            if result.page_info.total_fetched >= previous.page_info.max_rows:
                result.page_info.has_next = False
                result.page_info.exhausted = True

        return result

    def query_all(
        self,
        sql: str,
        page_size: int = MAX_PAGE_SIZE,
        max_rows: Optional[int] = None,
        on_page: Optional[callable] = None,
    ) -> QueryResult:
        """
        Execute a SQL query and auto-paginate to fetch all rows.

        Args:
            sql:        The SQL to execute.
            page_size:  Rows per page (1-1000, default 1000).
            max_rows:   Maximum total rows to fetch (None = unlimited).
            on_page:    Optional callback(QueryResult) called after each page.
                        Useful for progress reporting in frontends.

        Returns:
            A single QueryResult with all rows merged.

        WARNING: Without max_rows, this will fetch the ENTIRE result set.
        For tables with millions of rows, always set max_rows or use
        manual pagination with query() + fetch_next().
        """
        all_rows: list[dict[str, Any]] = []
        columns: list[str] = []
        total_time = 0.0

        page = self.query(sql, page_size=page_size)
        page.page_info.max_rows = max_rows
        columns = page.columns
        all_rows.extend(page.rows)
        total_time += page.execution_time

        if on_page:
            on_page(page)

        while page.page_info.has_next:
            if max_rows is not None and len(all_rows) >= max_rows:
                break

            page = self.fetch_next(page)
            all_rows.extend(page.rows)
            total_time += page.execution_time

            if on_page:
                on_page(page)

        # Trim to max_rows if exceeded
        if max_rows is not None and len(all_rows) > max_rows:
            all_rows = all_rows[:max_rows]

        return QueryResult(
            columns=columns,
            rows=all_rows,
            page_info=PageInfo(
                page=page.page_info.page,
                page_size=page_size,
                offset=0,
                rows_returned=len(all_rows),
                has_next=False,
                total_fetched=len(all_rows),
                max_rows=max_rows,
                exhausted=True,
            ),
            sql=sql,
            execution_time=total_time,
        )

    def __repr__(self) -> str:
        return f"FusionClient(url={self.url!r}, auth={self._auth!r})"
