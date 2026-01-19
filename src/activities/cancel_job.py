import logging

from temporalio import activity

from . import job_handler

logger = logging.getLogger(__name__)


@activity.defn
async def cancel_slurm_job(job_pid: str, process_pid: int) -> bool:
    handler = job_handler.get_job_handler()
    cancel_ok = handler.cancel_job(job_pid, process_pid)

    if not cancel_ok:
        raise ValueError("Job cancellation failed")
    logger.info("Job %s cancellation successful", job_pid)
