<p align="center">
  <img src="docs/logo.svg" width="120" alt="fusion-query logo">
</p>

<h1 align="center">fusion-query</h1>

<p align="center">
  <strong>Universal Oracle Fusion Cloud SQL query engine via BI Publisher</strong>
</p>

<p align="center">
  <a href="README_pt-BR.md">Portugues</a> |
  <a href="README_es.md">Espanol</a>
</p>

<p align="center">
  <img src="https://img.shields.io/pypi/v/fusion-query?color=blue" alt="PyPI">
  <img src="https://img.shields.io/pypi/pyversions/fusion-query" alt="Python">
  <img src="https://img.shields.io/github/license/AleCyriaco/fusion-query" alt="License">
</p>

---

Run arbitrary SQL queries against Oracle Fusion Cloud (ERP, HCM, SCM) databases using **only a URL, username, and password**. No JDBC, no VPN, no direct database access required.

## How it works

```
SQL → gzip → base64 → HTTP POST to BI Publisher → PL/SQL REF CURSOR → CSV → parsed rows
```

fusion-query uses Oracle BI Publisher with a lightweight proxy report. The proxy report contains a PL/SQL block that receives compressed SQL, executes it via REF CURSOR against the Fusion database, and returns pipe-delimited CSV results. Works with both REST and SOAP APIs, automatically selecting the best transport for your instance.

```sql
-- PL/SQL inside the proxy report Data Model:
DECLARE
  TYPE CurType IS REF CURSOR;
  xdo_cursor CurType;
BEGIN
  OPEN :xdo_cursor FOR
    utl_raw.cast_to_varchar2(
      UTL_COMPRESS.lz_uncompress(
        TO_BLOB(utl_encode.base64_decode(
          UTL_RAW.CAST_TO_RAW(:P_B64_CONTENT)
        ))
      )
    );
END;
```

---

## Installation

```bash
pip install fusion-query
```

With extras:
```bash
pip install fusion-query[server]   # REST API server (FastAPI + Uvicorn)
pip install fusion-query[cli]      # Rich table formatting
pip install fusion-query[all]      # Everything
```

---

## Quick start

### Just connect and query — no setup required

```python
from fusion_query import FusionClient

client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "user", "pass")

result = client.query("SELECT USERNAME, EMAIL_ADDRESS FROM PER_USERS")
for row in result.rows:
    print(row["USERNAME"], row["EMAIL_ADDRESS"])
```

The proxy report is **auto-deployed** to your personal BIP folder (`/~username/FusionQuery/`) on first use. No BI Administrator role required — any authenticated user can start querying immediately.

---

## Three ways to use

### Python library

```python
from fusion_query import FusionClient

client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "user", "pass")

# Single page (up to 1000 rows)
result = client.query("SELECT * FROM PER_USERS")

# Auto-paginate all rows
result = client.query_all("SELECT * FROM PER_USERS ORDER BY USER_ID", max_rows=5000)

# Manual page-by-page
page = client.query("SELECT * FROM GL_JE_LINES ORDER BY JE_LINE_NUM")
while page.has_next:
    page = client.fetch_next(page)
```

### CLI

```bash
# Run a query
fusion-query query --url https://... --user admin "SELECT SYSDATE FROM DUAL"

# JSON output
fusion-query query --url ... --user admin -f json "SELECT * FROM PER_USERS"

# Fetch all rows (auto-paginate)
fusion-query query --url ... --user admin --all --max-rows 5000 \
  "SELECT * FROM PER_USERS ORDER BY USER_ID"

# Test connection
fusion-query test --url https://... --user admin
```

### REST API (language-agnostic: Java, Rust, JS, Go, etc.)

```bash
fusion-query serve --port 8000
# API docs at http://localhost:8000/docs
```

```bash
# Connect
curl -X POST http://localhost:8000/connect \
  -H "Content-Type: application/json" \
  -d '{"name":"prod", "url":"https://xxxx.oraclecloud.com",
       "username":"admin", "password":"secret"}'

# Query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT USER_NAME FROM PER_USERS", "connection":"prod"}'

# Auto-paginate
curl -X POST http://localhost:8000/query/all \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM PER_USERS ORDER BY USER_ID",
       "connection":"prod", "max_rows":5000}'
```

---

## Pagination

### The problem

Oracle BI Publisher enforces a **limit of ~1000 rows** per report execution. Queries returning more are silently truncated.

### The solution

fusion-query wraps your SQL with `OFFSET ... FETCH NEXT ... ROWS ONLY` and makes multiple HTTP requests to transparently fetch all data in pages.

### PageInfo — universal pagination contract

Every response includes a `page_info` object. **All drivers (Python, Java, Rust, JS) must implement this same structure:**

```json
{
  "page_info": {
    "page": 0,
    "page_size": 1000,
    "offset": 0,
    "rows_returned": 1000,
    "has_next": true,
    "total_fetched": 1000,
    "max_rows": null,
    "exhausted": false
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `page` | int | Current page number (0-based) |
| `page_size` | int | Rows requested per page (max 1000) |
| `offset` | int | SQL OFFSET used for this page |
| `rows_returned` | int | Actual rows in this page |
| `has_next` | bool | `true` if `rows_returned == page_size` (more data likely exists) |
| `total_fetched` | int | Cumulative rows across all pages |
| `max_rows` | int/null | Upper limit set by caller (null = unlimited) |
| `exhausted` | bool | `true` if no more data or max_rows reached |

### Pagination algorithm (for driver implementors)

```
function query_all(sql, page_size=1000, max_rows=None):
    all_rows = []
    page = 0

    loop:
        paginated_sql = wrap_with_offset(sql, page * page_size, page_size)
        encoded = base64(gzip(paginated_sql))
        response = HTTP_POST(bip_url, {P_B64_CONTENT: encoded})
        rows = parse_csv(base64_decode(response.reportBytes))

        all_rows.append(rows)

        if len(rows) < page_size → break  // last page
        if max_rows and total >= max_rows → break

        page += 1
    return all_rows
```

### Python examples

```python
# Manual pagination
page = client.query("SELECT * FROM GL_JE_LINES ORDER BY JE_LINE_NUM", page_size=500)
all_rows = list(page.rows)
while page.has_next:
    page = client.fetch_next(page)
    all_rows.extend(page.rows)
    print(f"Fetched {page.page_info.total_fetched} rows so far...")

# Auto-pagination with progress
def on_page(result):
    pi = result.page_info
    print(f"Page {pi.page}: {pi.rows_returned} rows ({pi.total_fetched} total)")

result = client.query_all(
    "SELECT * FROM AP_INVOICES_ALL ORDER BY INVOICE_ID",
    max_rows=10000,
    on_page=on_page,
)
```

### REST API pagination

```bash
# Page 0
curl -X POST http://localhost:8000/query \
  -d '{"sql":"SELECT * FROM PER_USERS ORDER BY USER_ID", "page":0}'
# → {"page_info": {"has_next": true, "page": 0, ...}}

# Page 1
curl -X POST http://localhost:8000/query \
  -d '{"sql":"SELECT * FROM PER_USERS ORDER BY USER_ID", "page":1}'
# → {"page_info": {"has_next": false, "page": 1, ...}}
```

> **Important:** Always include `ORDER BY` for deterministic pagination. Without it, Oracle does not guarantee row order and rows may shift between pages.

---

## Authentication

### Basic Auth

```python
from fusion_query import FusionClient, BasicAuth

# Shorthand
client = FusionClient("https://...", "user", "pass")

# Explicit
client = FusionClient("https://...", auth=BasicAuth("user", "pass"))
```

### OAuth2 (IDCS client credentials)

```python
from fusion_query import FusionClient, OAuth2Auth

auth = OAuth2Auth(
    token_url="https://idcs-xxx.identity.oraclecloud.com/oauth2/v1/token",
    client_id="abc123",
    client_secret="secret",
)
client = FusionClient("https://...", auth=auth)
```

### Custom auth (for driver implementors)

```python
from fusion_query.auth import AuthProvider

class MyAuth(AuthProvider):
    def apply(self, session):
        session.headers["Authorization"] = "Bearer my-token"
    def describe(self):
        return {"type": "custom"}
```

---

## Proxy report deployment

The proxy report is **auto-deployed on first use** — no manual setup required.

### How auto-deploy works

1. On first `query()` or `test_connection()`, fusion-query checks if the proxy report exists
2. If not found, it deploys to your **personal BIP folder** (`/~username/FusionQuery/v1/`)
3. Any authenticated user can write to their own `~/` folder — no BI Administrator role needed
4. Uses SOAP API for deployment (works on all instances including OCS)

### Shared deployment (optional)

To deploy to a shared folder accessible by all users:

```bash
fusion-query setup --url https://xxxx.fa.us2.oraclecloud.com --user bi_admin
```

This deploys to `/Custom/FusionQuery/Proxy/v1/` which requires **BI Administrator** role but is shared across all users.

### Manual deployment (if needed)

1. Log into Oracle Fusion as BI Administrator
2. Navigate to **Reports and Analytics > Catalog**
3. Create folder: `/Shared Folders/Custom/FusionQuery/Proxy/v1/`
4. Create a Data Model with parameter `P_B64_CONTENT` (String), data source `ApplicationDB_FSCM`, and the PL/SQL shown above
5. Create a Report referencing the Data Model, output CSV, delimiter `|`

---

## Architecture for driver implementors

This Python package is the **reference implementation**. The protocol is simple enough to port to any language.

### Core algorithm

```
1. ENCODE   sql_bytes = UTF8(sql)
            compressed = GZIP(sql_bytes)
            encoded = BASE64(compressed)

2. REQUEST  — REST v1 (preferred):
            POST {url}/xmlpserver/services/rest/v1/reports/{report_path}/run
            Content-Type: application/json
            Authorization: Basic base64(user:pass)
            Body: { "byPassCache": true, "attributeFormat": "csv",
                    "parameterNameValues": { ... P_B64_CONTENT: encoded } }

            — SOAP v2 (fallback for OCS instances):
            POST {url}/xmlpserver/services/v2/ReportService
            Content-Type: text/xml
            Body: SOAP envelope with <v2:runReport> + inline credentials

3. DECODE   csv_bytes = BASE64_DECODE(response.reportBytes)
            rows = PARSE_CSV(csv_bytes, delimiter='|')

4. PAGINATE Wrap SQL: SELECT * FROM ({sql}) t OFFSET n ROWS FETCH NEXT 1000 ROWS ONLY
            Repeat 1-3 until rows_returned < page_size
```

> **Dual transport:** fusion-query tries the REST v1 API first. If it fails (common on Oracle Cloud Services instances), it automatically switches to the SOAP v2 API for the remainder of the session.

### Response schema

```json
{
  "columns": ["USER_ID", "USER_NAME", "EMAIL_ADDRESS"],
  "rows": [
    {"USER_ID": "123", "USER_NAME": "ADMIN", "EMAIL_ADDRESS": "admin@example.com"}
  ],
  "row_count": 1,
  "page_info": {
    "page": 0, "page_size": 1000, "offset": 0,
    "rows_returned": 1, "has_next": false,
    "total_fetched": 1, "max_rows": null, "exhausted": true
  },
  "sql": "SELECT USER_ID, USER_NAME, EMAIL_ADDRESS FROM PER_USERS",
  "execution_time": 1.23,
  "error": null
}
```

### Project structure

```
fusion_query/
  __init__.py       — Public API: FusionClient, QueryResult, PageInfo, BasicAuth, OAuth2Auth
  client.py         — Core engine: encode, request, decode, paginate
  auth.py           — AuthProvider interface + BasicAuth + OAuth2Auth
  catalog.py        — Auto-deploy proxy report via BIP Catalog REST API
  soap.py           — SOAP v2 API client (catalog + report execution for OCS instances)
  cli.py            — CLI: query, setup, test, serve
  server.py         — REST API server (FastAPI)
  setup/
    FusionQueryProxy.xdrz — Bundled BIP report template
```

---

## Environment variables

| Variable | Description |
|----------|-------------|
| `FUSION_PASSWORD` | Password for CLI (avoids `--password` flag) |

## Limitations

- **1000 rows per request** — handled transparently via pagination
- **Read-only** — only SELECT statements (no INSERT/UPDATE/DELETE/DDL)
- **Data source** — bundled template uses `ApplicationDB_FSCM`; modify for HCM/SCM
- **Timeouts** — large queries may timeout; adjust with `timeout=` parameter

## Contributing

Contributions welcome! Areas of interest:

- **Java driver** — JDBC-compatible wrapper for DBeaver, SQL Developer, etc.
- **Rust driver** — High-performance native client
- **JavaScript/TypeScript** — Node.js and browser support
- **Go driver** — For cloud-native tooling

See the [Architecture section](#architecture-for-driver-implementors) for the protocol spec.

## License

MIT
