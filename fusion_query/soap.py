"""
BIP SOAP v2 API client — works on all Oracle Fusion instances including OCS.

The REST v1 API is broken on many Oracle Cloud Services (OCS) instances,
returning HTTP 500 for every operation. The SOAP v2 API works reliably
everywhere because it embeds credentials directly in the SOAP body.

This module provides SOAP equivalents for:
    - CatalogService: createFolder, uploadObject, getFolderContents
    - ReportService:  runReport (execute SQL via the proxy report)

The SOAP API is the reliable transport for OCS instances.
"""

from __future__ import annotations

import base64
import logging
import re
from typing import Optional
from xml.sax.saxutils import escape

import requests

logger = logging.getLogger("fusion_query.soap")

# ---------------------------------------------------------------------------
# SOAP envelope helpers
# ---------------------------------------------------------------------------

_SOAP_NS = 'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"'
_BIP_NS = 'xmlns:v2="http://xmlns.oracle.com/oxp/service/v2"'


def _envelope(body: str) -> str:
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<soapenv:Envelope {_SOAP_NS} {_BIP_NS}>\n'
        f'  <soapenv:Body>\n{body}\n  </soapenv:Body>\n'
        f'</soapenv:Envelope>'
    )


def _credentials(user: str, password: str) -> str:
    return (
        f"      <v2:userID>{escape(user)}</v2:userID>\n"
        f"      <v2:password>{escape(password)}</v2:password>"
    )


# ---------------------------------------------------------------------------
# SOAP Catalog operations
# ---------------------------------------------------------------------------

class SOAPCatalog:
    """BIP CatalogService via SOAP v2."""

    def __init__(
        self,
        base_url: str,
        session: requests.Session,
        username: str,
        password: str,
        timeout: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.username = username
        self.password = password
        self.timeout = timeout
        self._url = f"{self.base_url}/xmlpserver/services/v2/CatalogService"

    def _post(self, body: str) -> requests.Response:
        return self.session.post(
            self._url,
            data=_envelope(body).encode("utf-8"),
            headers={"Content-Type": "text/xml; charset=utf-8"},
            timeout=self.timeout,
        )

    def create_folder(self, folder_path: str) -> bool:
        """Create a folder at the given absolute path."""
        body = (
            f"    <v2:createFolder>\n"
            f"      <v2:folderAbsolutePath>{escape(folder_path)}</v2:folderAbsolutePath>\n"
            f"{_credentials(self.username, self.password)}\n"
            f"    </v2:createFolder>"
        )
        try:
            resp = self._post(body)
            ok = resp.status_code == 200 and "Fault" not in resp.text
            if ok:
                logger.info("SOAP folder created: %s", folder_path)
            else:
                logger.debug("SOAP create folder %s failed: %s", folder_path, resp.text[:200])
            return ok
        except requests.RequestException as exc:
            logger.error("SOAP create folder error: %s", exc)
            return False

    def upload_object(
        self,
        catalog_path: str,
        content: bytes,
        object_type: str = "xdoz",
    ) -> bool:
        """Upload a catalog object (report or data model)."""
        b64 = base64.b64encode(content).decode("ascii")
        body = (
            f"    <v2:uploadObject>\n"
            f"      <v2:reportObjectAbsolutePathURL>{escape(catalog_path)}</v2:reportObjectAbsolutePathURL>\n"
            f"      <v2:objectType>{escape(object_type)}</v2:objectType>\n"
            f"      <v2:objectZippedData>{b64}</v2:objectZippedData>\n"
            f"{_credentials(self.username, self.password)}\n"
            f"    </v2:uploadObject>"
        )
        try:
            resp = self._post(body)
            ok = resp.status_code == 200 and "Fault" not in resp.text
            if ok:
                logger.info("SOAP uploaded: %s", catalog_path)
            else:
                logger.debug("SOAP upload %s failed: %s", catalog_path, resp.text[:200])
            return ok
        except requests.RequestException as exc:
            logger.error("SOAP upload error: %s", exc)
            return False

    def object_exists(self, path: str) -> bool:
        """Check if a catalog object exists."""
        # Use getFolderContents on the parent to check
        parent = path.rsplit("/", 1)[0] if "/" in path else "/"
        name = path.rsplit("/", 1)[-1] if "/" in path else path

        body = (
            f"    <v2:getFolderContents>\n"
            f"      <v2:folderAbsolutePath>{escape(parent)}</v2:folderAbsolutePath>\n"
            f"{_credentials(self.username, self.password)}\n"
            f"    </v2:getFolderContents>"
        )
        try:
            resp = self._post(body)
            if resp.status_code != 200 or "Fault" in resp.text:
                return False
            return f"<fileName>{name}</fileName>" in resp.text
        except requests.RequestException:
            return False


# ---------------------------------------------------------------------------
# SOAP Report execution
# ---------------------------------------------------------------------------

class SOAPReportService:
    """BIP ReportService via SOAP v2 — runs queries through the proxy report."""

    def __init__(
        self,
        base_url: str,
        session: requests.Session,
        username: str,
        password: str,
        timeout: int = 120,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.username = username
        self.password = password
        self.timeout = timeout
        self._url = f"{self.base_url}/xmlpserver/services/v2/ReportService"

    def run_report(
        self,
        report_path: str,
        encoded_sql: str,
    ) -> bytes:
        """
        Execute a report with the given encoded SQL parameter.

        Returns the raw CSV bytes from the report output.
        Raises RuntimeError on SOAP fault.
        """
        body = (
            f"    <v2:runReport>\n"
            f"      <v2:reportRequest>\n"
            f"        <v2:attributeFormat>csv</v2:attributeFormat>\n"
            f"        <v2:byPassCache>true</v2:byPassCache>\n"
            f"        <v2:flattenXML>false</v2:flattenXML>\n"
            f"        <v2:parameterNameValues>\n"
            f"          <v2:listOfParamNameValues>\n"
            f"            <v2:item>\n"
            f"              <v2:multiValuesAllowed>false</v2:multiValuesAllowed>\n"
            f"              <v2:name>P_B64_CONTENT</v2:name>\n"
            f"              <v2:refreshParamOnChange>false</v2:refreshParamOnChange>\n"
            f"              <v2:selectAll>false</v2:selectAll>\n"
            f"              <v2:templateParam>false</v2:templateParam>\n"
            f"              <v2:useNullForAll>false</v2:useNullForAll>\n"
            f"              <v2:values>\n"
            f"                <v2:item>{encoded_sql}</v2:item>\n"
            f"              </v2:values>\n"
            f"            </v2:item>\n"
            f"          </v2:listOfParamNameValues>\n"
            f"        </v2:parameterNameValues>\n"
            f"        <v2:reportAbsolutePath>{escape(report_path)}</v2:reportAbsolutePath>\n"
            f"        <v2:sizeOfDataChunkDownload>-1</v2:sizeOfDataChunkDownload>\n"
            f"      </v2:reportRequest>\n"
            f"{_credentials(self.username, self.password)}\n"
            f"    </v2:runReport>"
        )

        resp = self.session.post(
            self._url,
            data=_envelope(body).encode("utf-8"),
            headers={"Content-Type": "text/xml; charset=utf-8"},
            timeout=self.timeout,
        )

        if resp.status_code != 200 or "Fault" in resp.text:
            # Extract fault string
            m = re.search(r"<faultstring>([^<]+)</faultstring>", resp.text)
            msg = m.group(1) if m else resp.text[:300]
            raise RuntimeError(f"SOAP runReport failed: {msg}")

        # Extract reportBytes
        m = re.search(r"<reportBytes>([^<]+)</reportBytes>", resp.text)
        if not m:
            raise RuntimeError("SOAP response missing reportBytes")

        return base64.b64decode(m.group(1))
