import os
from datetime import timedelta
from typing import List, Tuple

import paramiko
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from .. import constants, db_connector
from ..workflows.utils import CountRequest, CountResult

# Retry policy for SLURM-related activities
ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=3,
)


def _run_ssh_command(cmd: str) -> str:
    """
    Helper to run a command on the SLURM server via SSH and return stdout.
    Raises Exception on connection or command failure.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            constants.SLURM_SERVER_HOST,
            port=int(constants.SLURM_SERVER_PORT),
            username=constants.SLURM_SERVER_USER,
            key_filename=constants.SLURM_SERVER_SSH_KEY_PATH,
        )
        stdin, stdout, stderr = client.exec_command(cmd)
        stdout.channel.set_combine_stderr(True)
        output = stdout.read().decode("utf-8", errors="ignore").strip()
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            raise Exception(f"SSH command failed (exit {exit_status}): {output}")
        return output
    except Exception as e:
        raise Exception(f"SSH execution error: {e}")
    finally:
        client.close()


async def get_job_details(process_pid: int):
    """
    Activity to fetch Priority and SubmitTime for a SLURM job via SSH.

    Returns:
        priority (int), submit_time (ISO string)
        OR (None, None) if job not found (finished/invalid).
    Raises:
        ActivityError on SSH or parse failure.
    """
    cmd = f"scontrol show job {process_pid}"
    try:
        output = _run_ssh_command(cmd)
    except Exception as e:
        if "slurm_load_jobs error: Invalid job id specified" in str(e):
            return None, None
        raise e

    priority = None
    submit_time = None
    for token in output.split():  # whitespace tokens
        if token.startswith("Priority="):
            try:
                priority = int(token.split("=", 1)[1])
            except ValueError:
                raise ActivityError(f"Invalid Priority value: {token}")
        elif token.startswith("SubmitTime="):
            submit_time = token.split("=", 1)[1]

    if priority is None or submit_time is None:
        raise ActivityError(
            f"Could not parse Priority/SubmitTime for job {process_pid}"
        )
    return priority, submit_time


async def list_pending_jobs(partition: str):
    """
    Activity to list all pending SLURM jobs via SSH.

    Returns:
        List of (priority, eligible_time) tuples.
    Raises:
        ActivityError on SSH or parse failure.
    """
    cmd = f'squeue -t PD -p {partition} -h -o "%Q %V %i"'

    output = _run_ssh_command(cmd)

    jobs: List[Tuple[int, str, int]] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        p_str, time_str, job_id = parts[0], parts[1], parts[2]
        try:
            priority = int(p_str)
            job_id = int(job_id)
            jobs.append((priority, time_str, job_id))
        except ValueError:
            continue
    return jobs, output


async def list_running_jobs(partition: str):
    cmd = f'squeue -t R -p {partition} -h -o "%Q %V %i"'

    output = _run_ssh_command(cmd)

    jobs: List[Tuple[int, str, int]] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        p_str, time_str, job_id = parts[0], parts[1], parts[2]
        try:
            priority = int(p_str)
            job_id = int(job_id)
            jobs.append((priority, time_str, job_id))
        except ValueError:
            continue
    return jobs, output


@activity.defn
async def count_jobs_ahead(req: CountRequest) -> CountResult:
    logger = activity.logger

    if req.process_pid is None:
        # No SLURM job yet, treat as no queue position
        return CountResult(priority=0, submit_time="", pending=0, pending_str="")

    if not req.job or not req.job_partition:
        raise ApplicationError("Missing job_pid or job_partition")

    try:
        priority, submit_time = await get_job_details(req.process_pid)
        if priority is None:
            return CountResult(priority=0, submit_time="", pending=0, pending_str="")
        
        pending, pending_str = await list_pending_jobs(req.job_partition)

        pending_count = 0
        for p, t, pid in pending:
            if (
                p > priority
                or (p == priority and t < submit_time)
                or int(pid) == int(req.process_pid)
            ):
                pending_count += 1

        return CountResult(
            priority=priority,
            submit_time=submit_time,
            pending=pending_count,
            pending_str=pending_str,
        )
    except Exception as e:
        logger.error(f"count_jobs_ahead failed: {e}")
        raise ApplicationError(str(e))


@activity.defn
async def log_queue_position(job_pid: str, pending: int) -> None:
    logger = activity.logger
    logger.info(f"Queue position for {job_pid}: {pending}")
    try:
        base = os.getenv("WEBAPP_BASE_ENDPOINT")
        await db_connector.DbConnector(base).log_queue_position(job_pid, pending)
    except Exception as e:
        logger.error(f"Failed to log queue position: {e}")
        raise ValueError(f"Failed to log queue position: {e}")
