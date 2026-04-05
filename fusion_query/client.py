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
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Union

import requests

from fusion_query.auth import AuthProvider, BasicAuth


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

        # Build session
        self._session = requests.Session()
        self._session.verify = verify_ssl
        self._auth.apply(self._session)

    @property
    def _run_url(self) -> str:
        """BIP REST API endpoint for running reports."""
        # URL-encode the report path for the REST endpoint
        path = self.report_path
        if path.startswith("/"):
            path = path[1:]
        return f"{self.url}/xmlpserver/services/rest/v1/reports/{path}/run"

    def test_connection(self) -> bool:
        """
        Test connectivity to Oracle Fusion BIP.

        Runs 'SELECT 1 FROM DUAL' and returns True if successful.
        Raises on failure.
        """
        result = self.query("SELECT 1 AS OK FROM DUAL")
        if result.error:
            raise ConnectionError(f"Connection test failed: {result.error}")
        return True

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
        page_size = min(max(1, page_size), MAX_PAGE_SIZE)
        if offset is None:
            offset = page * page_size

        # Wrap SQL with pagination
        paginated_sql = _wrap_paginated_sql(sql, offset, page_size)

        # Encode and execute
        t0 = time.time()
        encoded = encode_sql(paginated_sql)
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
