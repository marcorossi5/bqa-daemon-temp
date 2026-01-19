import json
import logging
import os
import tarfile
from pathlib import Path
from typing import Any, Optional

import httpx
from temporalio import activity

from .. import constants, db_connector, slurm_postprocess
from . import job_handler

logger = logging.getLogger(__name__)


class SLURMError(Exception):
    """Custom exception for SLURM-related errors."""

    pass


@activity.defn
async def update_job_status(
    job_pid: str, status: str, metadata: Optional[dict] = None
) -> bool:
    """Update the status of a job in the database.

    Args:
        job_pid: The ID of the job to update
        status: The new status (e.g., 'pending', 'running', 'completed', 'error')
        metadata: Additional metadata to store with the status update

    Returns:
        bool: True if the update was successful
    """
    try:
        base = os.getenv("WEBAPP_BASE_ENDPOINT")
        await db_connector.DbConnector(base).update_job(job_pid, {"status": status})
        logger.info(f"Updating job {job_pid} status to {status}")
        if metadata:
            logger.info(f"Metadata: {metadata}")
        return True
    except Exception as e:
        logger.error(f"Failed to update job status: {e}")
        raise ValueError(f"Failed to update job status: {e}")


@activity.defn
async def prepare_job_run_artifacts(job: dict[str, Any]):
    """Gather all necessary inputs for the SLURM job.

    Args:
        job: The job to gather inputs for
    """
    try:
        job_folder = constants.BASE_JOB_FOLDER / job["pid"]

        try:
            job_folder.mkdir()
        except FileExistsError:
            logger.error(
                "Job launch failed: job folder %s already exists", job_folder.as_posix()
            )
            # @TODO: understand if relaunching the job is a valid option
            raise ValueError("Job launch failed: job folder already exists")

        circuit_path = job_folder / "circuit.json"
        with open(circuit_path, "w") as f:
            json.dump(job["circuit"], f)

        settings_path = job_folder / "settings.json"
        settings = {
            "device": job["projectquota"]["partition"]["device"],
            "nshots": job["nshots"],
            "verbatim": job["verbatim"],
        }
        settings_path.write_text(json.dumps(settings, indent=2))

    except Exception as e:
        logger.error(f"Failed to gather job inputs: {e}")
        raise ValueError(f"Failed to gather job inputs: {e}")


@activity.defn
async def launch_job(job: dict[str, Any], namespace: str, workflow_id: str) -> int:
    """Execute the SLURM command directly via local subprocess."""
    handler = job_handler.get_job_handler()
    process_pid = handler.launch(job, namespace, workflow_id)

    if process_pid is None:
        raise ValueError("Job launch failed")
    logger.info("Job %s launched successfully", job["pid"])
    return process_pid


@activity.defn
async def register_process_pid_to_db(job_pid: str, process_pid: int):
    """Register the process pid to the database."""
    try:
        base = os.getenv("WEBAPP_BASE_ENDPOINT")
        await db_connector.DbConnector(base).update_job(
            job_pid, {"process_pid": process_pid}
        )
        logger.info("Process pid %s registered for job %s", process_pid, job_pid)
    except Exception as e:
        logger.error(f"Failed to register process pid: {e}")
        raise ValueError(f"Failed to register process pid: {e}")


@activity.defn
async def check_job_exit_status(job_pid: str) -> bool:
    try:
        job_folder = constants.BASE_JOB_FOLDER / job_pid
        exit_status_path = job_folder / "exit_status.log"
        exit_code = int(exit_status_path.read_text().strip())
        return exit_code != 0
    except Exception as e:
        logger.error(f"Failed to check job exit status: {e}")
        raise ValueError(f"Failed to check job exit status: {e}")


@activity.defn
async def update_quotas(job: dict[str, Any]):
    """Update the quotas for the given job."""
    try:
        job_folder = constants.BASE_JOB_FOLDER / job["pid"]
        stdout_path = job_folder / "stdout.log"
        stderr_path = job_folder / "stderr.log"
        seconds_spent_path = job_folder / "seconds_spent.log"
        seconds_spent = float(seconds_spent_path.read_text())

        await slurm_postprocess.postprocess(
            pid=job["pid"],
            seconds_spent=seconds_spent,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to update job quotas: {e}")
        raise ValueError(f"Failed to update job quotas: {e}")


@activity.defn
async def create_results_archive(
    job: dict[str, Any],
    archive_name: str = "results.tar.gz",
) -> dict[str, Any]:
    """
    Create a tar.gz archive of all files in slurm_job_folder.

    Args:
        job: The job to gather inputs for
        archive_name: Name of the output archive file (default: results.tar.gz).

    Returns:
        A dict with:
          - archive_path: Full path to the created archive.
          - size_bytes: Archive size in bytes.
          - size_kb: Archive size in kilobytes (3-decimal precision).
    """
    slurm_job_folder = constants.BASE_JOB_FOLDER / job["pid"]
    logger.info(f"Creating results archive in {slurm_job_folder}")

    # Full path for the archive
    archive_path = os.path.join(slurm_job_folder, archive_name)

    try:
        # Pack everything under slurm_job_folder, but flatten so that
        # its contents appear at the root of the tarball.
        with tarfile.open(archive_path, "w:gz") as tar:
            for entry in Path(slurm_job_folder).iterdir():
                tar.add(entry, arcname=entry.name)

        # Stat the file to get its size
        stat = os.stat(archive_path)
        size_bytes = stat.st_size
        size_kb = round(size_bytes / 1024, 3)

        logger.info(
            f"Archive created: {archive_path} " f"({size_bytes} bytes / {size_kb} KB)"
        )

        return {
            "archive_path": archive_path,
            "size_bytes": size_bytes,
            "size_kb": size_kb,
        }

    except Exception as e:
        logger.error(f"Error creating archive in {slurm_job_folder}: {e}")
        raise ValueError(f"Error creating archive in {slurm_job_folder}: {e}")


@activity.defn
async def transfer_results_to_webserver(
    job_pid: str,
    archive_path: str,
) -> dict[str, Any]:
    """
    Activity to upload results archive via DbConnector.transfer_results.

    Args:
        archive_path: Path to the .tar.gz file.
        db_base_url: Optional base URL for JWT endpoint (falls back to env).
        upload_url: Optional override for the upload endpoint (falls back to env).

    Returns:
        The JSON response from the server.

    Raises:
        ActivityError: On missing config or HTTP errors.
    """
    # Resolve base URL
    base = os.getenv("WEBAPP_BASE_ENDPOINT")
    if not base:
        logger.error("Missing WEBAPP_BASE_ENDPOINT env var")
        raise ValueError("Missing WEBAPP_BASE_ENDPOINT env var")

    connector = db_connector.DbConnector(base)
    return await connector.transfer_results(archive_path, job_pid)


async def upload_tarball(
    file_path: str, api_url: str, user_email: str, job_pid: str, token: str
) -> dict[str, Any]:
    """
    Upload a tarball file to the daemon API.

    Args:
        file_path: Path to the .tar.gz or .tgz file to upload
        api_url: Base URL of your API (e.g., "http://localhost:8000/daemon/upload/tarball/")
        user_email: Email of the user
        job_pid: Job process ID
        token: JWT authentication token

    Returns:
        Dictionary containing the API response
    """
    # Verify file exists and is a tarball
    file_path = Path(file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    if not (file_path.endswith(".tar.gz") or file_path.endswith(".tgz")):
        raise ValueError("File must be a .tar.gz or .tgz archive")

    # Prepare headers
    headers = {
        "Authorization": f"Bearer {token}",
    }

    # Prepare form data
    files = {"file": (file_path.name, open(file_path, "rb"), "application/gzip")}

    data = {"job_pid": job_pid}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                api_url, headers=headers, data=data, files=files, timeout=30.0
            )
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP error occurred: {e.response.text}"
        logger.error(msg)
        raise ValueError(msg)
    except Exception as e:
        msg = f"An error occurred: {str(e)}"
        logger.error(msg)
        raise ValueError(msg)
