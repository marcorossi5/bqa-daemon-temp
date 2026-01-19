from typing import Any

from temporalio import workflow
from temporalio.workflow import get_external_workflow_handle

with workflow.unsafe.imports_passed_through():
    from .. import activities
    from . import utils


@workflow.defn(name="cancel-job")
class CancelJobWorkflow:
    async def cancel_workflow_if_running(self, handle: str) -> bool:
        try:
            await handle.cancel()
            workflow.logger.info("Sent cancel to workflow %s", handle.id)
            return True
        except Exception as err:
            workflow.logger.info(str(err))
            return False

    @workflow.run
    async def run(self, job_data: dict[str, Any]) -> None:
        job_pid = job_data.get("job")
        process_pid = job_data.get("process_pid")

        handle = get_external_workflow_handle(f"track-queue-{job_pid}")
        canceled_checker = await self.cancel_workflow_if_running(handle)

        handle = get_external_workflow_handle(f"post-job-on-slurm-{job_pid}")
        canceled_post = await self.cancel_workflow_if_running(handle)

        if canceled_post and canceled_checker and process_pid is not None:
            await utils.exec(
                activities.cancel_slurm_job,
                args=[job_pid, process_pid],
            )
        else:
            workflow.logger.info(
                "Skipping Slurm cancellation for job %s because no cancel signal was sent or process_pid is None",
                job_pid,
            )
