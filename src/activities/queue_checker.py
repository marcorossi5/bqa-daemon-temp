import subprocess
from typing import List

from temporalio import activity

@activity.defn
async def list_pending_jobs() -> List[int]:
    """
    SSH to Slurm host and return all PENDING job IDs as a list of ints.
    """
    # If you already have an SSH wrapper, use it here. 
    # For simplicity, we shell out locally; adjust to your SSH pattern.
    pass