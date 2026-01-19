from datetime import timedelta
from typing import Any, List

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .. import activities


# Retry policy for activities
ACTIVITY_RETRY_POLICY = RetryPolicy(
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=1,
)


@workflow.defn(name="check-pending-queue")
class CheckPendingQueueWorkflow:
    @workflow.run
    async def run(self, job_data: dict[str, Any]) -> List[int]:
        """
        Returns the list of pending Slurm jobs whose IDs are strictly less than `job_pid`.
        """
        job_pid = job_data.get("job")
        process_pid = job_data.get("process_pid")

        # 1) Call the Activity to fetch all pending job IDs:
        pending: List[int] = await workflow.execute_activity(
            activities.list_pending_jobs,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        # 2) Filter
        earlier = [job_id for job_id in pending if job_id < job_pid]

        # 3) Optionally, if you expect NONE, you could raise or return empty:
        if not earlier:
            workflow.logger.info("No pending jobs before %d", job_pid)
        else:
            workflow.logger.info(
                "Found %d pending jobs before %d: %s", len(earlier), job_pid, earlier
            )

        return earlier
