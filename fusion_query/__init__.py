"""
fusion-query: Universal Oracle Fusion Cloud SQL query engine via BI Publisher.

Usage:
    from fusion_query import FusionClient

    client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "user", "pass")
    result = client.query("SELECT * FROM PER_USERS")
    for row in result.rows:
        print(row)

Pagination (auto):
    result = client.query("SELECT * FROM PER_USERS", max_rows=5000)
    # Automatically fetches pages of 1000 rows until max_rows is reached.

Pagination (manual):
    page = client.query("SELECT * FROM PER_USERS", page_size=1000)
    while page.has_next:
        page = client.fetch_next(page)
"""

from fusion_query.client import FusionClient, QueryResult, PageInfo
from fusion_query.auth import BasicAuth, OAuth2Auth

__version__ = "0.1.0"
__all__ = ["FusionClient", "QueryResult", "PageInfo", "BasicAuth", "OAuth2Auth"]
