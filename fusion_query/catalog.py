"""
BIP Catalog Service — auto-deploy the proxy report.

On first use, fusion-query needs a "proxy report" deployed to the BIP catalog.
This module handles automatic deployment using the BIP Catalog REST API.

The proxy report consists of:
    - A Data Model (dm.xdm) with PL/SQL REF CURSOR that decodes gzipped SQL
    - A Report (csv.xdo) that outputs pipe-delimited CSV

The template is bundled as FusionQueryProxy.xdrz in the package.

Deployment strategy:
    1. Try the user's personal folder first: /~username/FusionQuery/v1/
       Any authenticated user can write to their own ~ folder.
    2. Fall back to /Custom/FusionQuery/Proxy/v1/ (requires BI Administrator).
"""

from __future__ import annotations

import os
import re
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

# Default catalog paths (shared folder — requires BI Administrator)
DEFAULT_FOLDER = "/Custom/FusionQuery"
DEFAULT_DM_PATH = "/Custom/FusionQuery/Proxy/v1/dm.xdm"
DEFAULT_REPORT_PATH = "/Custom/FusionQuery/Proxy/v1/csv.xdo"

# Original dataModel reference baked into the report template
_ORIGINAL_DM_URL = "/~REDACTED/DataViewerTool/v1/dm.xdm"


def _user_folder(username: str) -> str:
    """Return the BIP personal folder path for a given username."""
    return f"/~{username}/FusionQuery"


def _user_report_path(username: str) -> str:
    """Return the report path in the user's personal folder."""
    return f"/~{username}/FusionQuery/v1/csv.xdo"


def _user_dm_path(username: str) -> str:
    """Return the data model path in the user's personal folder."""
    return f"/~{username}/FusionQuery/v1/dm.xdm"


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
            logger.debug("Failed to create folder %s (HTTP %s)", folder_path, resp.status_code)
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
            logger.debug("Failed to upload %s (HTTP %s)", catalog_path, resp.status_code)
            return False
        except requests.RequestException as exc:
            logger.error("Error uploading %s: %s", catalog_path, exc)
            return False

    def _patch_report_xdoz(self, xdoz_bytes: bytes, new_dm_path: str) -> bytes:
        """
        Rewrite the dataModel URL inside csv.xdoz to point to the new dm.xdm path.

        The bundled template references the original author's path. When deploying
        to a different folder we must patch this reference so the report finds
        its data model.
        """
        zf_in = zipfile.ZipFile(io.BytesIO(xdoz_bytes), "r")
        buf = io.BytesIO()
        zf_out = zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED)

        for entry in zf_in.namelist():
            data = zf_in.read(entry)
            if entry == "_report.xdo":
                text = data.decode("utf-8")
                # Replace any dataModel url with the new path
                text = re.sub(
                    r'(<dataModel\s+url=")[^"]*(")',
                    rf"\g<1>{new_dm_path}\2",
                    text,
                )
                data = text.encode("utf-8")
            zf_out.writestr(entry, data)

        zf_out.close()
        return buf.getvalue()

    def deploy_report(
        self,
        template_path: Optional[str] = None,
        target_folder: str = DEFAULT_FOLDER,
    ) -> bool:
        """
        Deploy the proxy report to the BIP catalog.

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
            f"{target_folder}/v1",
        ]
        for folder in folders:
            self.create_folder(folder)

        # Extract template
        with zipfile.ZipFile(template_path, "r") as zf:
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

        # Patch the report to point to the correct data model path
        new_dm_path = f"{target_folder}/v1/dm.xdm"
        report_content = self._patch_report_xdoz(report_content, new_dm_path)

        # Upload Data Model
        dm_ok = self.upload_object(
            f"{target_folder}/v1/dm.xdm",
            dm_content,
            object_type="xdmz",
        )

        # Upload Report
        report_ok = self.upload_object(
            f"{target_folder}/v1/csv.xdo",
            report_content,
            object_type="xdoz",
        )

        if dm_ok and report_ok:
            logger.info("Proxy report deployed successfully.")
            return True

        logger.debug("Proxy report deployment failed.")
        return False

    def deploy_to_user_folder(self, username: str) -> bool:
        """
        Deploy the proxy report to the user's personal BIP folder (~username/).

        Any authenticated user can write to their own personal folder
        without BI Administrator role.
        """
        folder = _user_folder(username)
        return self.deploy_report(target_folder=folder)


def ensure_report_deployed(
    base_url: str,
    session: requests.Session,
    report_path: str = DEFAULT_REPORT_PATH,
    timeout: int = 60,
    username: Optional[str] = None,
) -> tuple[bool, str]:
    """
    Convenience function: check if the proxy report exists, deploy if missing.

    Strategy:
        1. Check the given report_path (default: /Custom/...)
        2. If username is provided, also check /~username/FusionQuery/v1/csv.xdo
        3. If not found anywhere, try deploying to user's personal folder first
        4. Fall back to /Custom/ (requires BI Administrator)

    Returns (deployed: bool, actual_report_path: str).
    """
    catalog = CatalogService(base_url, session, timeout)

    # Check default /Custom/ path
    if catalog.report_is_deployed(report_path):
        logger.info("Proxy report already deployed at %s", report_path)
        return True, report_path

    # Check user's personal folder
    if username:
        user_path = _user_report_path(username)
        if catalog.report_is_deployed(user_path):
            logger.info("Proxy report found in user folder at %s", user_path)
            return True, user_path

    # Not found — try deploying
    logger.info("Proxy report not found. Deploying...")

    # Strategy 1: deploy to user's personal folder (no special permissions needed)
    if username:
        if catalog.deploy_to_user_folder(username):
            user_path = _user_report_path(username)
            logger.info("Deployed to user folder: %s", user_path)
            return True, user_path

    # Strategy 2: fall back to /Custom/ (requires BI Administrator)
    if catalog.deploy_report():
        return True, report_path

    return False, report_path
