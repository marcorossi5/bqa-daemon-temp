import asyncio
import argparse

from . import db_connector


async def put_jobs_to_status(
    pid: str,
    status: str,
    endpoint: str,
    daemon_api_key: str,
):
    """Marks a job with as given status.

    :param pid: the pid run by the user
    :type pid: str
    :param status: the final job's status
    :type status: str
    :param endpoint: the webapp endpoint url
    :type endpoint: str
    :param daemon_api_key: the daemon's api key
    :type daemon_api_key: str
    """

    db_connection = db_connector.DbConnector(endpoint, daemon_api_key)

    if status not in [
        "running",
        "postprocessing",
        "success",
        "error",
    ]:
        raise ValueError(f"Unexpected `status` parameter value, got {status}")

    payload = {"status": status}
    await db_connection.update_job(pid, payload)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("pid", help="Job's process ID")
    parser.add_argument(
        "--status",
        help="Job's final status",
        choices=["running", "postprocessing", "success", "error"],
    )
    parser.add_argument("--endpoint", help="Webapp endpoint")
    parser.add_argument("--daemon_api_key", help="Daemon's api key")
    args = parser.parse_args()
    asyncio.run(
        put_jobs_to_status(
            pid=args.pid,
            status=args.status,
            endpoint=args.endpoint,
            daemon_api_key=args.daemon_api_key,
        )
    )
