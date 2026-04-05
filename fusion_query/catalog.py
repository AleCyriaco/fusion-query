"""
BIP Catalog Service — auto-deploy the proxy report.

On first use, fusion-query needs a "proxy report" deployed to the BIP catalog.
This module handles automatic deployment using the BIP Catalog REST API.

The proxy report consists of:
    - A Data Model (dm.xdm) with PL/SQL REF CURSOR that decodes gzipped SQL
    - A Report (csv.xdo) that outputs pipe-delimited CSV

The template is bundled as FusionQueryProxy.xdrz in the package.

For driver implementors (Java/Rust):
    Implement the same catalog check + upload flow:
    1. GET /xmlpserver/services/rest/v1/catalogservice?objectAbsolutePath=...
    2. If 404 → POST to create folder + upload report
    3. Cache the result so you only check once per session
"""

from __future__ import annotations

import os
import base64
import zipfile
import io
import json
import logging
from pathlib import Path
from typing import Optional

import requests

from fusion_query.auth import AuthProvider

logger = logging.getLogger("fusion_query.catalog")

# Path to the bundled report template
_TEMPLATE_PATH = Path(__file__).parent / "setup" / "FusionQueryProxy.xdrz"

# Default catalog paths
DEFAULT_FOLDER = "/Custom/FusionQuery"
DEFAULT_DM_PATH = "/Custom/FusionQuery/Proxy/v1/dm.xdm"
DEFAULT_REPORT_PATH = "/Custom/FusionQuery/Proxy/v1/csv.xdo"


class CatalogService:
    """
    Manages the BIP catalog for the proxy report.

    Handles checking if the report exists and deploying it if missing.

    Args:
        base_url:   Oracle Fusion Cloud instance URL.
        session:    An authenticated requests.Session.
        timeout:    HTTP timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        session: requests.Session,
        timeout: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.timeout = timeout
        self._catalog_url = f"{self.base_url}/xmlpserver/services/rest/v1/catalogservice"

    def object_exists(self, path: str) -> bool:
        """Check if a catalog object exists at the given path."""
        try:
            resp = self.session.get(
                self._catalog_url,
                params={"objectAbsolutePath": path},
                timeout=self.timeout,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def report_is_deployed(self, report_path: str = DEFAULT_REPORT_PATH) -> bool:
        """Check if the proxy report is already deployed."""
        return self.object_exists(report_path)

    def create_folder(self, folder_path: str) -> bool:
        """Create a folder in the BIP catalog."""
        # Split into parent and folder name
        parts = folder_path.rstrip("/").rsplit("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid folder path: {folder_path}")
        parent, name = parts

        payload = {
            "folderAbsolutePathURL": parent,
            "folderName": name,
        }

        try:
            resp = self.session.post(
                f"{self._catalog_url}/folder",
                json=payload,
                timeout=self.timeout,
            )
            if resp.status_code in (200, 201, 409):  # 409 = already exists
                logger.info("Folder created/exists: %s", folder_path)
                return True
            logger.warning("Failed to create folder %s: %s", folder_path, resp.text[:200])
            return False
        except requests.RequestException as exc:
            logger.error("Error creating folder %s: %s", folder_path, exc)
            return False

    def upload_object(
        self,
        catalog_path: str,
        content: bytes,
        object_type: str = "xdoz",
    ) -> bool:
        """
        Upload a catalog object (report or data model) to BIP.

        Args:
            catalog_path:  Target path in the BIP catalog.
            content:       Raw file bytes to upload.
            object_type:   File type (xdoz, xdmz, etc.).
        """
        encoded = base64.b64encode(content).decode("ascii")

        payload = {
            "objectAbsolutePathURL": catalog_path,
            "objectType": object_type,
            "objectData": encoded,
        }

        try:
            resp = self.session.post(
                f"{self._catalog_url}",
                json=payload,
                timeout=self.timeout,
            )
            if resp.status_code in (200, 201):
                logger.info("Uploaded: %s", catalog_path)
                return True
            logger.warning("Failed to upload %s: %s", catalog_path, resp.text[:200])
            return False
        except requests.RequestException as exc:
            logger.error("Error uploading %s: %s", catalog_path, exc)
            return False

    def deploy_report(
        self,
        template_path: Optional[str] = None,
        target_folder: str = DEFAULT_FOLDER,
    ) -> bool:
        """
        Deploy the proxy report to the BIP catalog.

        Extracts the FusionQueryProxy.xdrz template and uploads:
            1. Creates /Custom/FusionQuery/ folder
            2. Creates /Custom/FusionQuery/Proxy/ folder
            3. Creates /Custom/FusionQuery/Proxy/v1/ folder
            4. Uploads the Data Model (dm.xdm)
            5. Uploads the Report (csv.xdo)

        Args:
            template_path:  Path to .xdrz file. Default: bundled template.
            target_folder:  Target folder in BIP catalog.

        Returns:
            True if deployment succeeded.
        """
        if template_path is None:
            template_path = str(_TEMPLATE_PATH)

        if not os.path.exists(template_path):
            raise FileNotFoundError(
                f"Report template not found: {template_path}. "
                "Ensure fusion-query is properly installed."
            )

        logger.info("Deploying proxy report from %s to %s", template_path, target_folder)

        # Create folder hierarchy
        folders = [
            target_folder,
            f"{target_folder}/Proxy",
            f"{target_folder}/Proxy/v1",
        ]
        for folder in folders:
            self.create_folder(folder)

        # Extract template
        with zipfile.ZipFile(template_path, "r") as zf:
            # Read the data model and report
            dm_content = None
            report_content = None

            for name in zf.namelist():
                if name.endswith("dm.xdmz"):
                    dm_content = zf.read(name)
                elif name.endswith("csv.xdoz"):
                    report_content = zf.read(name)

        if dm_content is None or report_content is None:
            raise ValueError(
                "Invalid template: missing dm.xdmz or csv.xdoz in the .xdrz file."
            )

        # Upload Data Model
        dm_ok = self.upload_object(
            f"{target_folder}/Proxy/v1/dm.xdm",
            dm_content,
            object_type="xdmz",
        )

        # Upload Report
        report_ok = self.upload_object(
            f"{target_folder}/Proxy/v1/csv.xdo",
            report_content,
            object_type="xdoz",
        )

        if dm_ok and report_ok:
            logger.info("Proxy report deployed successfully.")
            return True

        logger.error("Proxy report deployment failed.")
        return False


def ensure_report_deployed(
    base_url: str,
    session: requests.Session,
    report_path: str = DEFAULT_REPORT_PATH,
    timeout: int = 60,
) -> bool:
    """
    Convenience function: check if the proxy report exists, deploy if missing.

    Call this on first connection. It's idempotent — safe to call multiple times.

    Returns True if the report is ready (already existed or was deployed).
    """
    catalog = CatalogService(base_url, session, timeout)

    if catalog.report_is_deployed(report_path):
        logger.info("Proxy report already deployed at %s", report_path)
        return True

    logger.info("Proxy report not found. Deploying...")
    return catalog.deploy_report()
