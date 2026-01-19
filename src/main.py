import asyncio
import logging
import os
import signal
from contextlib import suppress

from google.protobuf.duration_pb2 import Duration
from temporalio.api.workflowservice.v1 import (
    ListNamespacesRequest,
    RegisterNamespaceRequest,
)
from temporalio.client import Client, TLSConfig
from temporalio.service import ConnectConfig, ServiceClient
from temporalio.worker import Worker

TEMPORAL_SERVER_URL = os.getenv("TEMPORAL_SERVER_URL", "localhost:7234")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE_NAME = os.getenv("TEMPORAL_TASK_QUEUE_NAME", "jobs-queue")
RETRY_BACKOFF_SECS = float(os.getenv("WORKER_RETRY_BACKOFF_SECS", "3.0"))
NAMESPACE_RETENTION_DAYS = int(os.getenv("NAMESPACE_RETENTION_DAYS", "1"))
TEMPORAL_MTLS_CERT_PATH = os.getenv("TEMPORAL_MTLS_CERT_PATH")
TEMPORAL_MTLS_KEY_PATH = os.getenv("TEMPORAL_MTLS_KEY_PATH")
TEMPORAL_MTLS_DOMAIN = os.getenv("TEMPORAL_MTLS_DOMAIN")

stop_event = asyncio.Event()


def _handle_signal(*_):
    stop_event.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


async def ensure_namespace(
    client: ServiceClient, namespace: str, retention_days: int = 1
):
    # idempotent: retry until service becomes available
    while not stop_event.is_set():
        try:
            resp = await client.workflow_service.list_namespaces(
                ListNamespacesRequest()
            )
            if any(ns.namespace_info.name == namespace for ns in resp.namespaces):
                logging.warning(
                    "⚠️ Namespace %r already exists; skipping registration", namespace
                )
                return
            await client.workflow_service.register_namespace(
                RegisterNamespaceRequest(
                    namespace=namespace,
                    workflow_execution_retention_period=Duration(
                        seconds=retention_days * 86400
                    ),
                    owner_email="support.cloud@qibo.science",
                )
            )
            logging.warning("✅ Successfully registered namespace %r", namespace)
            return
        except Exception as e:
            logging.error(
                "Namespace ensure failed: %s; retrying in %.1fs", e, RETRY_BACKOFF_SECS
            )
            await asyncio.wait([stop_event.wait()], timeout=RETRY_BACKOFF_SECS)


async def build_temporal_client() -> Client:
    # Prepare TLS Config if configured
    tls_config = None
    if TEMPORAL_MTLS_CERT_PATH and TEMPORAL_MTLS_KEY_PATH:
        with open(TEMPORAL_MTLS_CERT_PATH, "rb") as f:
            client_cert = f.read()
        with open(TEMPORAL_MTLS_KEY_PATH, "rb") as f:
            client_key = f.read()

        tls_config = TLSConfig(
            client_cert=client_cert,
            client_private_key=client_key,
            domain=TEMPORAL_MTLS_DOMAIN,
        )

    # Retry connect until the tunnel/server is available
    while not stop_event.is_set():
        try:
            # ServiceClient first, to ensure control plane reachability
            svc = await ServiceClient.connect(
                ConnectConfig(target_host=TEMPORAL_SERVER_URL, tls=tls_config)
            )
            await ensure_namespace(
                svc, TEMPORAL_NAMESPACE, retention_days=NAMESPACE_RETENTION_DAYS
            )
            # App client
            client = await Client.connect(
                TEMPORAL_SERVER_URL,
                namespace=TEMPORAL_NAMESPACE,
                tls=tls_config,
            )
            logging.warning(
                "✅ Connected to Temporal at %s (ns=%s)",
                TEMPORAL_SERVER_URL,
                TEMPORAL_NAMESPACE,
            )
            return client
        except Exception as e:
            logging.error(
                "Connect failed: %s; retrying in %.1fs", e, RETRY_BACKOFF_SECS
            )
            await asyncio.wait([stop_event.wait()], timeout=RETRY_BACKOFF_SECS)

    raise asyncio.CancelledError()


async def run_worker_forever():
    while not stop_event.is_set():
        client = None
        worker = None
        try:
            client = await build_temporal_client()
            from . import (  # import here so restart picks up new code if container hot-reloads
                activities,
                workflows,
            )

            worker = Worker(
                client,
                task_queue=TEMPORAL_TASK_QUEUE_NAME,
                workflows=[
                    workflows.PostJobOnSlurmWorkflow,
                    workflows.CancelJobWorkflow,
                    workflows.CheckPendingQueueWorkflow,
                    workflows.TrackQueuePositionWorkflow,
                ],
                activities=[
                    activities.prepare_job_run_artifacts,
                    activities.launch_job,
                    activities.check_job_exit_status,
                    activities.update_quotas,
                    activities.create_results_archive,
                    activities.transfer_results_to_webserver,
                    activities.update_job_status,
                    activities.cancel_slurm_job,
                    activities.register_process_pid_to_db,
                    activities.count_jobs_ahead,
                    activities.log_queue_position,
                ],
            )
            logging.warning("👀 Worker polling queue: %s", TEMPORAL_TASK_QUEUE_NAME)
            await worker.run()  # blocks; returns on cancellation or raises on channel failure
            logging.warning("Worker exited normally; will recreate.")
        except asyncio.CancelledError:
            logging.warning("Shutdown requested; exiting worker loop.")
            break
        except Exception as e:
            logging.error(
                "Worker crashed: %s; restarting in %.1fs", e, RETRY_BACKOFF_SECS
            )
            await asyncio.wait([stop_event.wait()], timeout=RETRY_BACKOFF_SECS)
        finally:
            # Ensure resources closed before recreating
            with suppress(Exception):
                if worker:
                    await worker.shutdown()
            with suppress(Exception):
                if client:
                    await client.close()


async def main():
    await run_worker_forever()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    asyncio.run(main())
