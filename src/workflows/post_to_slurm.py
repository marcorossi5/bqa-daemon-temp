from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

# Import activities & utils for workflows
with workflow.unsafe.imports_passed_through():
    from .. import activities
    from . import utils

# Retry policy for short, idempotent activities
DEFAULT_RETRY = RetryPolicy(
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=5,
)

# Retry policy for the polling activity (we want resilience, not noise)
POLL_RETRY = RetryPolicy(
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=0,  # unlimited attempts while workflow is alive
)


TRACKER_POLLING_INTERVAL = 3

@dataclass
class JobResult:
    job_pid: str
    status: str
    output: str
    metadata: Dict[str, Any]
    error: Optional[str] = None


# ------------------------------
# Child workflow: queue tracking
# ------------------------------


@workflow.defn(name="track-queue-position")
class TrackQueuePositionWorkflow:
    def __init__(self) -> None:
        self._process_pid: Optional[int] = None
        self._stop: bool = False

    @workflow.run
    async def run(self, job_pid: str, device: str) -> None:
        """Poll queue position until told to stop.

        Flow:
        - Wait for process_pid via signal (set_process_pid)
        - Poll count_jobs_ahead -> log_queue_position
        - Exit on stop signal (notify_stop)
        """
        logger = workflow.logger
        logger.info(f"TrackQueuePosition started (job={job_pid}, device={device})")

        # 1) Wait for process_pid to arrive
        await workflow.wait_condition(
            lambda: self._process_pid is not None or self._stop
        )
        if self._stop:
            logger.info("Tracker stopped before process_pid was provided")
            return

        # 2) Poll loop (Temporal timers keep determinism; no busy loop)
        while not self._stop:
            try:
                req = utils.CountRequest(
                    job=job_pid,
                    job_partition=device,
                    process_pid=self._process_pid,  # now known
                )
                res = await utils.exec(
                    activities.count_jobs_ahead,
                    args=[req],
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=POLL_RETRY,
                )
                logger.debug(
                    f"[tracker] queue position: {res.pending} (prio={res.priority}, submit={res.submit_time})"
                )
                await utils.exec(
                    activities.log_queue_position,
                    args=[job_pid, res.pending],
                    retry_policy=POLL_RETRY,
                )
            except Exception as e:
                # One failure should not kill tracking; just log and continue
                logger.info(f"[tracker] polling hiccup: {e}")

            # Cadence (adjust as you like; Temporal timer)
            await workflow.sleep(TRACKER_POLLING_INTERVAL)

        logger.info("TrackQueuePosition exiting cleanly")

    # Signals
    @workflow.signal
    def set_process_pid(self, pid: int) -> None:
        self._process_pid = pid

    @workflow.signal
    def notify_stop(self) -> None:
        self._stop = True


# ------------------------------
# Parent workflow: job posting
# ------------------------------


@workflow.defn(name="post-job-on-slurm")
class PostJobOnSlurmWorkflow:
    def __init__(self):
        self.job_done = False
        self.job_started = False

    # External signals (e.g., from SLURM integration / daemon hooks)
    @workflow.signal
    def notify_job_start(self) -> None:
        self.job_started = True

    @workflow.signal
    def notify_job_done(self) -> None:
        self.job_done = True

    @workflow.run
    async def run(self, job_data: Dict[str, Any]) -> JobResult:
        info = workflow.info()
        logger = workflow.logger
        job_pid = job_data.get("pid", "unknown")
        device = job_data["projectquota"]["partition"]["device"]

        logger.info(f"PostJobOnSlurmWorkflow started for job: {job_pid}")

        # 0) Immediately mark pending (requirement 1)
        try:
            await utils.exec(
                activities.update_job_status,
                args=[job_pid, "pending", {"update_time": workflow.now().isoformat()}],
                retry_policy=DEFAULT_RETRY,
            )
        except Exception as e:
            logger.warning(f"Initial pending status update failed: {e}")

        # 1) Start the queue tracker child in parallel (requirement 3)
        tracker = await workflow.start_child_workflow(
            TrackQueuePositionWorkflow.run,
            args=[job_pid, device],
            id=f"track-queue-{job_pid}",
            retry_policy=RetryPolicy(maximum_attempts=0),
        )

        # 2) Prepare artifacts and launch job
        await utils.exec(
            activities.prepare_job_run_artifacts,
            args=[job_data],
            retry_policy=DEFAULT_RETRY,
        )

        process_pid = await utils.exec(
            activities.launch_job,
            args=[job_data, info.namespace, info.workflow_id],
            retry_policy=DEFAULT_RETRY,
        )

        # Send process pid to db
        await utils.exec(
            activities.register_process_pid_to_db,
            args=[job_pid, process_pid],
            retry_policy=DEFAULT_RETRY,
        )

        # Feed process_pid to tracker (it may have been waiting)
        try:
            await tracker.signal(
                TrackQueuePositionWorkflow.set_process_pid, process_pid
            )
        except Exception as e:
            logger.warning(f"Failed to signal tracker with process_pid: {e}")

        # 3) Let SLURM set 'running' itself (requirement 2).
        #    We only stop the tracker once the job actually starts.
        #    This assumes something external (daemon/adapter) will call notify_job_start().
        await workflow.wait_condition(lambda: self.job_started)
        try:
            await tracker.signal(TrackQueuePositionWorkflow.notify_stop)
        except Exception as e:
            logger.info(f"Tracker stop signal failed (already stopped?): {e}")

        # 4) Wait for SLURM to finish
        await workflow.wait_condition(lambda: self.job_done)

        # 5) Check exit status and set success flag
        success = True
        error_msg: Optional[str] = None
        try:
            failed = await utils.exec(
                activities.check_job_exit_status,
                args=[job_pid],
                retry_policy=DEFAULT_RETRY,
            )
            if failed:
                raise ApplicationError("Slurm job exited non-zero")
        except (ActivityError, ApplicationError) as ae:
            success = False
            error_msg = str(ae)
            logger.error(f"Job execution failed: {error_msg}")
        except Exception as e:
            success = False
            error_msg = f"Unexpected error: {e}"
            logger.error(error_msg)

        # 6) If job is still considered progressing, mark postprocessing
        try:
            await utils.exec(
                activities.update_job_status,
                args=[
                    job_pid,
                    "postprocessing",
                    {"update_time": workflow.now().isoformat()},
                ],
                retry_policy=DEFAULT_RETRY,
            )
        except Exception as e:
            logger.warning(f"Postprocessing status update failed: {e}")

        # 7) Archive + transfer (best-effort but affects success if previously OK)
        result: Dict[str, Any] = {}
        try:
            result = await utils.exec(
                activities.create_results_archive,
                args=[job_data],
                retry_policy=DEFAULT_RETRY,
            )
            await utils.exec(
                activities.transfer_results_to_webserver,
                args=[job_pid, result.get("archive_path")],
                retry_policy=DEFAULT_RETRY,
            )
        except Exception as e:
            if success:
                success = False
                error_msg = f"Archiving/transfer failed: {e}"
            logger.error(error_msg or f"Archive/transfer step failed: {e}")

        # 8) Update quotas regardless of success state
        try:
            await utils.exec(
                activities.update_quotas,
                args=[job_data],
                retry_policy=DEFAULT_RETRY,
            )
        except Exception as e:
            # If quota update fails after a successful job, mark as error
            success = False
            error_msg = f"Quota update failed: {e}"
            logger.error(error_msg)

        # 9) Final status update
        if success:
            await utils.exec(
                activities.update_job_status,
                args=[job_pid, "success", {"result": result}],
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=DEFAULT_RETRY,
            )
            logger.info(f"Workflow completed successfully: {result}")
            return JobResult(
                job_pid=job_pid,
                status="success",
                output=result.get("output", ""),
                metadata=result.get("metadata", {}),
            )

        # error path
        payload = {"error": error_msg}
        await utils.exec(
            activities.update_job_status,
            args=[job_pid, "error", payload],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )
        logger.info(f"Workflow ended with error: {error_msg}")
        raise ApplicationError(error_msg or "Unknown failure")
