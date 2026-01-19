import os
import asyncio
from typing import Any, Dict, Optional

import httpx

from .config_logging import logger
from .constants import DAEMON_API_KEY


class ApplicationError(Exception):
    """Custom exception for HTTP request errors."""

    pass


class DbConnector:
    """Class to allow database connectivity through HTTP via HTTPX."""

    def __init__(
        self,
        db_base_url: str,
        daemon_api_key: Optional[str] = None,
    ) -> None:
        self.db_base_url = db_base_url.rstrip("/")
        self.db_daemon_url = f"{self.db_base_url}/daemon"
        self.headers = {"Authorization": f"Api-Key {daemon_api_key or DAEMON_API_KEY}"}

    async def http_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: float = 120.0,
        max_retries: int = 5,
        base_delay: float = 1.0,
    ) -> httpx.Response:
        """
        Perform an asynchronous HTTP request using HTTPX AsyncClient with retries.

        Raises:
            ApplicationError: On non-2xx responses.
        """
        async with httpx.AsyncClient(headers=self.headers, timeout=timeout) as client:
            for attempt in range(max_retries + 1):
                try:
                    response = await client.request(
                        method, url, params=params, json=json_body
                    )
                    response.raise_for_status()
                    return response
                except httpx.HTTPStatusError as e:
                    # Retry only on server errors (5xx)
                    if 500 <= e.response.status_code < 600:
                        if attempt == max_retries:
                            logger.error(
                                f"HTTP {method} {url} failed: {e.response.status_code} {e.response.text}"
                            )
                            raise ApplicationError(
                                f"HTTP {method} {url} error: {e.response.status_code}"
                            ) from e
                    else:
                        # Client error (4xx), do not retry
                        logger.error(
                            f"HTTP {method} {url} failed: {e.response.status_code} {e.response.text}"
                        )
                        raise ApplicationError(
                            f"HTTP {method} {url} error: {e.response.status_code}"
                        ) from e
                except httpx.HTTPError as e:
                    # Network error, retry
                    if attempt == max_retries:
                        logger.error(f"Network error during {method} {url}: {e}")
                        raise ApplicationError(f"HTTP {method} {url} network error") from e

                # Wait before retrying if we haven't exhausted retries and exception wasn't raised
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Request {method} {url} failed, retrying in {delay}s... (Attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
        
        # Should be unreachable
        raise ApplicationError(f"HTTP {method} {url} failed after retries")

    async def get_job(self, pid: str) -> Dict[str, Any]:
        """
        Get job information by PID.
        """
        url = f"{self.db_daemon_url}/jobs/{pid}/"
        response = await self.http_request("GET", url)
        return response.json()

    async def update_job(self, pid: str, payload: Dict[str, Any]) -> httpx.Response:
        """
        Update a job by PID with provided payload.
        """
        url = f"{self.db_daemon_url}/jobs/{pid}/"
        return await self.http_request("PATCH", url, json_body=payload)

    async def update_job_info(
        self, pid: str, payload: Dict[str, Any]
    ) -> httpx.Response:
        """
        Update a job by PID with provided payload.
        """
        url = f"{self.db_daemon_url}/job-info/{pid}/"
        return await self.http_request("PATCH", url, json_body=payload)

    async def transfer_results(self, archive_path: str, job_pid: str) -> Dict[str, Any]:
        """
        Upload a results tarball to the specified endpoint using JWT auth.

        Args:
            archive_path: Full path to the .tar.gz file to upload.
            upload_url: Optional override for the upload endpoint URL.
                        If not provided, reads DB_UPLOAD_URL env var.

        Returns:
            The JSON response from the server.

        Raises:
            ApplicationError: On missing configuration or HTTP errors.
        """
        url = self.db_daemon_url + "/upload/tarball/"
        if not url:
            raise ApplicationError(
                "Missing DB_UPLOAD_URL env var or upload_url parameter"
            )

        logger.info(f"Uploading results archive {archive_path} to {url}")
        async with httpx.AsyncClient(headers=self.headers, timeout=120.0) as client:
            try:
                with open(archive_path, "rb") as f:
                    files = {
                        "file": (os.path.basename(archive_path), f, "application/gzip")
                    }
                    response = await client.post(
                        url, files=files, data={"job_pid": job_pid}
                    )
                    response.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"Transfer failed ({e.response.status_code}): {e.response.text}"
                )
                raise ApplicationError(
                    f"Transfer error: {e.response.status_code}"
                ) from e
            except httpx.HTTPError as e:
                logger.error(f"Network error during transfer: {e}")
                raise ApplicationError("Transfer network error") from e

        data = response.json()
        logger.info(f"Transfer succeeded, server returned: {data}")
        return data

    async def log_queue_position(self, job_pid: str, pending: int):
        url = self.db_daemon_url + f"/jobs/{job_pid}/queue-poll/"
        if not url:
            raise ApplicationError(
                "Missing DB_UPLOAD_URL env var or upload_url parameter"
            )
        return await self.http_request(
            "POST", url, json_body={"queue_position": pending}
        )
