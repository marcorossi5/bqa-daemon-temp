import argparse
import json
import os
from pathlib import Path
from typing import Dict, Optional

from . import db_connector


def get_versions_dict(job_folder: Path) -> Dict:
    versions_path = job_folder / "versions.json"
    if not versions_path.is_file():
        return {}

    with open(versions_path) as f:
        return json.load(f)


def get_new_shots_quota(job_info: Dict, job_folder: Path) -> int:
    settings_path = job_folder / "settings.json"
    with open(settings_path) as f:
        settings = json.load(f)
    nshots = settings.get("nshots")
    shots_quota = job_info["projectquota"]["shots_left"]
    if nshots is None:
        return shots_quota
    return shots_quota - nshots


def get_new_jobs_quota(job_info: Dict) -> int:
    """Computes the new project jobs quota."""
    return job_info["projectquota"]["jobs_left"] - 1


def get_new_time_quota(job_info: Dict, seconds_spent: float) -> float:
    """Computes the new project time quota.

    Add the preallocated seconds to the actual time quota.
    Remove the effective pre-allocated seconds.
    """
    return job_info["projectquota"]["seconds_left"] - seconds_spent


def get_transpiled_circuit(job_folder: Path) -> Optional[Dict]:
    transpiled_circuit_path = job_folder / "transpiled_circuit.json"
    if transpiled_circuit_path.is_file():
        with open(transpiled_circuit_path) as f:
            return json.load(f)
    return None


def get_frequencies(job_folder: Path) -> Optional[Dict]:
    freq_path = job_folder / "frequencies.json"
    if freq_path.exists():
        with open(freq_path) as f:
            return json.load(f)
    return None


def remove_null_values_from_dict(d: Dict) -> Dict:
    return {k: v for k, v in d.items() if v is not None}


async def postprocess(
    pid: str,
    seconds_spent: float,
    stdout_path: Path,
    stderr_path: Path,
):
    """Executes jobs termination operations and marks job as error.

    The following procedures are taken:

    - saving job result path to db
    - subtracting quota time spent by user

    :param pid: the pid run by the user
    :type pid: str
    :param stdout_path: the path to the computation stdout
    :type stdout_path: Path
    :param stderr_path: the path to the computation stderr
    :type stderr_path: Path
    :param seconds_spent: time spent by the user to run the given job
    :type seconds_spent: float
    """

    base = os.getenv("WEBAPP_BASE_ENDPOINT")
    db_connection = db_connector.DbConnector(base)

    job_info = await db_connection.get_job(pid)
    job_folder = stderr_path.parent

    versions_dict = get_versions_dict(job_folder)

    new_shots_quota = get_new_shots_quota(job_info, job_folder)

    new_job_quota = get_new_jobs_quota(job_info)

    new_time_quota = get_new_time_quota(job_info, seconds_spent)

    transpiled_circuit = get_transpiled_circuit(job_folder)

    frequencies = get_frequencies(job_folder)

    payload = {
        "result_path": f"{pid}-results.tar.gz",
        "qibo_version": versions_dict.get("qibo_version"),
        "qibolab_version": versions_dict.get("qibolab_version"),
        "stdout": stdout_path.read_text() if stdout_path.is_file() else None,
        "stderr": stderr_path.read_text() if stderr_path.is_file() else None,
        "transpiled_circuit": transpiled_circuit,
        "frequencies": frequencies,
        "runtime": seconds_spent,
        "projectquota": {
            "jobs_left": new_job_quota,
            "shots_left": new_shots_quota,
            "seconds_left": new_time_quota,
        },
    }

    payload = remove_null_values_from_dict(payload)
    await db_connection.update_job(pid, payload)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("pid", help="Job's process ID")
    parser.add_argument("--time", type=float, help="Job's runtime")
    parser.add_argument(
        "--out-log", type=Path, default=None, help="Computation stdout file path"
    )
    parser.add_argument(
        "--err-log", type=Path, default=None, help="Computation stderr file path"
    )
    args = parser.parse_args()
    postprocess(
        pid=args.pid,
        seconds_spent=args.time,
        stdout_path=args.out_log,
        stderr_path=args.err_log,
    )
