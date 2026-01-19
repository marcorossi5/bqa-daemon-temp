from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from temporalio import workflow
from temporalio.common import RetryPolicy

DEFAULT_START_TO_CLOSE_TIMEOUT = timedelta(minutes=1)
DEFAULT_RETRY_POLICY = RetryPolicy(
    maximum_attempts=1,
    maximum_interval=timedelta(seconds=10),
)


async def exec(func: Callable[..., Any], args: list, **kwargs) -> Any:
    """Helper to invoke an activity with standard timeout & retry policy."""
    if (
        kwargs.get("schedule_to_close_timeout") is None
        and kwargs.get("start_to_close_timeout") is None
    ):
        kwargs.setdefault("start_to_close_timeout", DEFAULT_START_TO_CLOSE_TIMEOUT)
    kwargs.setdefault("retry_policy", DEFAULT_RETRY_POLICY)
    return await workflow.execute_activity(func, args=args, **kwargs)


@dataclass
class CountResult:
    """
    Result container for number of pending SLURM jobs ahead of a given job.
    """

    priority: int
    submit_time: str
    pending: int
    pending_str: str


@dataclass
class CountRequest:
    job: str
    job_partition: str
    process_pid: int
