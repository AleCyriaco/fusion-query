"""
Command-line interface for fusion-query.

Commands:
    fusion-query query   "SELECT * FROM DUAL"         # Run a query
    fusion-query setup   --url ... --user ...          # Deploy proxy report
    fusion-query test    --url ... --user ...          # Test connection
    fusion-query serve   --port 8000                   # Start REST API server

All commands accept --url, --user, --password for connection.
Password can also be set via FUSION_PASSWORD env var.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import time


def _get_client(args):
    """Build a FusionClient from CLI args."""
    from fusion_query import FusionClient

    password = args.password or os.environ.get("FUSION_PASSWORD")
    if not password:
        password = getpass.getpass("Password: ")

    return FusionClient(
        url=args.url,
        auth=args.user,
        password=password,
        report_path=args.report_path,
        timeout=args.timeout,
        verify_ssl=not args.no_verify_ssl,
    )


def cmd_query(args):
    """Execute a SQL query and print results."""
    client = _get_client(args)

    def on_page(result):
        if args.format == "json":
            return
        pi = result.page_info
        print(
            f"  Page {pi.page}: {pi.rows_returned} rows "
            f"({pi.total_fetched} total, {result.execution_time:.2f}s)",
            file=sys.stderr,
        )

    if args.all:
        result = client.query_all(
            args.sql,
            page_size=args.page_size,
            max_rows=args.max_rows,
            on_page=on_page,
        )
    else:
        result = client.query(
            args.sql,
            page_size=args.page_size,
            page=args.page,
        )

    if result.error:
        print(f"ERROR: {result.error}", file=sys.stderr)
        sys.exit(1)

    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2, default=str))
    elif args.format == "csv":
        if result.columns:
            print(",".join(result.columns))
        for row in result.rows:
            print(",".join(str(row.get(c, "")) for c in result.columns))
    elif args.format == "table":
        _print_table(result)

    # Print pagination info to stderr
    pi = result.page_info
    if not args.all:
        print(
            f"\n--- Page {pi.page} | {pi.rows_returned} rows | "
            f"has_next: {pi.has_next} | total: {pi.total_fetched} | "
            f"{result.execution_time:.2f}s ---",
            file=sys.stderr,
        )


def _print_table(result):
    """Print results as a formatted table."""
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()
        table = Table(show_header=True, header_style="bold")
        for col in result.columns:
            table.add_column(col)
        for row in result.rows:
            table.add_row(*[str(row.get(c, "")) for c in result.columns])
        console.print(table)
    except ImportError:
        # Fallback without rich
        if result.columns:
            print(" | ".join(result.columns))
            print("-" * (sum(len(c) + 3 for c in result.columns)))
        for row in result.rows:
            print(" | ".join(str(row.get(c, "")) for c in result.columns))


def cmd_setup(args):
    """Deploy the proxy report to BIP catalog."""
    from fusion_query.catalog import CatalogService
    from fusion_query.auth import BasicAuth

    password = args.password or os.environ.get("FUSION_PASSWORD")
    if not password:
        password = getpass.getpass("Password: ")

    import requests as req

    session = req.Session()
    session.verify = not args.no_verify_ssl
    BasicAuth(args.user, password).apply(session)

    catalog = CatalogService(args.url, session, timeout=args.timeout)

    if catalog.report_is_deployed(args.report_path):
        print("Proxy report is already deployed.")
        return

    print("Deploying proxy report to BIP catalog...")
    success = catalog.deploy_report(target_folder=args.folder)
    if success:
        print("Proxy report deployed successfully!")
        print(f"  Report path: {args.report_path}")
    else:
        print("ERROR: Deployment failed. Check credentials and permissions.", file=sys.stderr)
        sys.exit(1)


def cmd_test(args):
    """Test connection to Oracle Fusion BIP."""
    client = _get_client(args)

    print(f"Testing connection to {args.url}...")
    try:
        result = client.test_connection()
    except Exception as exc:
        print(f"Connection FAILED: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"  connectivity    : {result.get('connectivity', 'unknown')}")
    print(f"  proxy_deployed  : {result.get('proxy_deployed', False)}")
    print(f"  proxy_installed : {result.get('proxy_was_installed', False)}")
    print(f"  query_test      : {result.get('query_test', 'skipped')}")
    print(f"  query_ready     : {result.get('query_ready', False)}")
    if result.get("error"):
        print(f"  error           : {result['error']}", file=sys.stderr)
    if result.get("warning"):
        print(f"  warning         : {result['warning']}", file=sys.stderr)

    if result.get("success"):
        if result.get("query_ready"):
            print("Connection successful! SQL queries are ready.")
        else:
            print("Connection successful (credentials OK). Proxy report not deployed — SQL queries unavailable.")
    else:
        print("Connection FAILED.", file=sys.stderr)
        sys.exit(1)


def cmd_serve(args):
    """Start the REST API server."""
    try:
        from fusion_query.server import create_app
        import uvicorn
    except ImportError:
        print(
            "REST server requires extra dependencies.\n"
            "Install with: pip install fusion-query[server]",
            file=sys.stderr,
        )
        sys.exit(1)

    app = create_app()
    print(f"Starting fusion-query REST API on http://0.0.0.0:{args.port}")
    print(f"API docs: http://0.0.0.0:{args.port}/docs")
    uvicorn.run(app, host="0.0.0.0", port=args.port)


def main():
    parser = argparse.ArgumentParser(
        prog="fusion-query",
        description="Oracle Fusion Cloud SQL query engine via BI Publisher",
    )
    parser.add_argument("--version", action="version", version="fusion-query 0.1.0")

    # Common connection args
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", required=True, help="Oracle Fusion Cloud URL")
    common.add_argument("--user", "-u", required=True, help="Username")
    common.add_argument("--password", "-p", default=None, help="Password (or set FUSION_PASSWORD env var)")
    common.add_argument("--report-path", default="/Custom/FusionQuery/Proxy/v1/csv.xdo",
                        help="BIP report path")
    common.add_argument("--timeout", type=int, default=120, help="HTTP timeout in seconds")
    common.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL verification")

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # query
    p_query = subparsers.add_parser("query", parents=[common], help="Execute a SQL query")
    p_query.add_argument("sql", help="SQL statement to execute")
    p_query.add_argument("--format", "-f", choices=["table", "csv", "json"], default="table",
                         help="Output format (default: table)")
    p_query.add_argument("--page-size", type=int, default=1000, help="Rows per page (max 1000)")
    p_query.add_argument("--page", type=int, default=0, help="Page number (0-based)")
    p_query.add_argument("--all", action="store_true", help="Fetch all pages")
    p_query.add_argument("--max-rows", type=int, default=None, help="Max rows to fetch (with --all)")
    p_query.set_defaults(func=cmd_query)

    # setup
    p_setup = subparsers.add_parser("setup", parents=[common], help="Deploy proxy report to BIP")
    p_setup.add_argument("--folder", default="/Custom/FusionQuery",
                         help="Target folder in BIP catalog")
    p_setup.set_defaults(func=cmd_setup)

    # test
    p_test = subparsers.add_parser("test", parents=[common], help="Test connection")
    p_test.set_defaults(func=cmd_test)

    # serve
    p_serve = subparsers.add_parser("serve", help="Start REST API server")
    p_serve.add_argument("--port", type=int, default=8000, help="Port (default: 8000)")
    p_serve.set_defaults(func=cmd_serve)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
