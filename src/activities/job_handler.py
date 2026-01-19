import os
import signal
import subprocess as sp
import threading
from abc import ABC, abstractmethod
from typing import Optional

import paramiko

from .. import constants
from ..config_logging import logger


class JobHandler(ABC):
    def launch(self, job: dict, namespace: str, workflow_id: str) -> bool:
        """Launches a job.

        Args:
            job: the job dictionary.
            namespace: the namespace of the workflow.
            workflow_id: the workflow id of the workflow.

        Returns:
            Whether the job has been successfully launched or not
        """
        try:
            return self.launch_fn(job, namespace, workflow_id)
        except (sp.CalledProcessError, paramiko.SSHException, OSError) as err:
            logger.error("New job launch failed: %s", job["pid"])
            logger.error(
                "Error: The process exited with a non-zero status code\n%s",
                err,
            )
            return False

    def cancel_job(self, job_pid: str, process_pid: int) -> bool:
        """Cancels a job.

        Args:
            job_pid: the job pid.

        Returns:
            Whether the job has been successfully cancelled or not
        """
        try:
            return self.cancel_job_fn(job_pid, process_pid)
        except (sp.CalledProcessError, paramiko.SSHException, OSError) as err:
            logger.error("Job cancellation failed: %s", job_pid)
            logger.error(
                "Error: The process exited with a non-zero status code\n%s",
                err,
            )
            return False

    @abstractmethod
    def launch_fn(self, job: dict, namespace: str, workflow_id: str):
        """Abstract method to launch the given job"""

    @abstractmethod
    def cancel_job_fn(self, job_pid: str, process_pid: int):
        """Abstract method to cancel the given job"""

    @abstractmethod
    def _get_launch_command(self, job: dict, namespace: str, workflow_id: str) -> str:
        """Returns the command used to launch the job.

        Args:
            job: the job dictionary.

        Returns:
            the command used to launch the command from CLI.
        """


def get_computation_job_cpu_resources(num_qubits: int) -> int:
    if num_qubits <= 15:
        return 1
    if num_qubits > 15 and num_qubits <= 25:
        return 2
    return 16


def get_computation_job_mem_resources(num_qubits: int) -> int:
    memory = 2 ** (num_qubits + 1) * 64 + 2**30
    mib = memory / 2**20
    return int(mib)


class SlurmJobHandler(JobHandler):
    def launch_fn(self, job: dict, namespace: str, workflow_id: str) -> Optional[int]:
        """Returns the job's slurm pid or None if the job has not been successfully launched"""
        cmd = self._get_launch_command(job, namespace, workflow_id)

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
            # merge stderr into stdout so we capture all text
            stdout.channel.set_combine_stderr(True)

            # read everything (you can also stream line-by-line)
            output = stdout.read().decode("utf-8", errors="ignore").strip()
            exit_status = stdout.channel.recv_exit_status()

            logger.info(
                "Launched job %s on slurm queue (exit status=%d). Slurm pid: %s",
                job["pid"],
                exit_status,
                output.strip().replace("\n", " "),
            )

            return int(output)

        except Exception as e:
            # logs traceback and returns False on any failure
            logger.exception("Failed to launch command")
            return None

        finally:
            client.close()

    def _get_launch_command(self, job: dict, namespace: str, workflow_id: str):
        partition = job["projectquota"]["partition"]
        job_folder = constants.SLURM_SERVER_BASE_JOB_FOLDER / job["pid"]
        slurm_server_output_path = (
            constants.SLURM_SERVER_BASE_LOGS_FOLDER / f"{job['pid']}.txt"
        )
        script_path = constants.SLURM_SERVER_SCRIPTS_FOLDER / "launch_script.sh"
        ssh_config_path = constants.SLURM_SSH_CONFIG_PATH
        local_results_path = job_folder / "results.tar.gz"
        
        # @TODO: remove the remote results path as the output is a json and can be stored in the db
        remote_results_path = (
            constants.AWS_BASE_JOB_FOLDER
            / f"{job['user']['email']}/{job['pid']}-results.tar.gz"
        )

        computation_job_cpus = (
            get_computation_job_cpu_resources(job["num_qubits"])
            if partition["hardware_type"] == "simulator"
            else 1
        )

        computation_job_mem = (
            get_computation_job_mem_resources(job["num_qubits"])
            if partition["hardware_type"] == "simulator"
            else 0
        )

        wall_time = min(
            job["projectquota"]["seconds_left"],
            job["projectquota"]["max_walltime_seconds"],
        )

        return f"""sbatch --parsable \
-p {partition['device']} \
-o {slurm_server_output_path} \
--ntasks 1 --cpus-per-task {computation_job_cpus} \
--mem {computation_job_mem} \
{script_path} \
{constants.SLURM_SERVER_SOURCE_FOLDER} \
--wall-time {wall_time} \
--job-pid {job['pid']} \
--local-results-path {local_results_path} \
--remote-results-path {remote_results_path} \
--slurm-job-folder {job_folder} \
--ssh-config-path {ssh_config_path} \
--endpoint {constants.WEBAPP_BASE_ENDPOINT} \
--namespace {namespace} \
--workflow-id {workflow_id} \
--daemon-api-key {constants.DAEMON_API_KEY} \
--modulefiles-folder {constants.MODULEFILES_FOLDER}"""

    def cancel_job_fn(self, job_pid: str, process_id: int):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                constants.SLURM_SERVER_HOST,
                port=int(constants.SLURM_SERVER_PORT),
                username=constants.SLURM_SERVER_USER,
                key_filename=constants.SLURM_SERVER_SSH_KEY_PATH,
            )

            # Check if it's running or pending
            stdin2, stdout2, stderr2 = client.exec_command(f"squeue -h -j {process_id}")
            squeue_out = stdout2.read().decode("utf-8", errors="ignore").strip()
            if not squeue_out:
                logger.info(
                    "Slurm job %s not found in queue for internal PID %s; skipping scancel",
                    process_id,
                    job_pid,
                )
                return False

            # Cancel it
            stdin3, stdout3, stderr3 = client.exec_command(f"scancel {process_id}")
            cancel_status = stdout3.channel.recv_exit_status()
            if cancel_status != 0:
                err = stderr3.read().decode("utf-8", errors="ignore")
                logger.error("scancel failed for %s: %s", process_id, err)
                return False

            logger.info(
                "Cancelled Slurm job %s for internal PID %s", process_id, job_pid
            )
            return True

        except Exception:
            logger.exception("Failed to cancel Slurm job for internal PID %s", job_pid)
            return False

        finally:
            client.close()


class LocalJobHandler(JobHandler):
    def launch_fn(self, job: dict, namespace: str, workflow_id: str):
        cmd = self._get_launch_command(job, namespace, workflow_id)

        # Create job folder if it doesn't exist
        job_folder = constants.SLURM_SERVER_BASE_JOB_FOLDER / job["pid"]

        log_path = job_folder / "job_output.log"

        def run_cmd_in_subprocess():
            with open(log_path, "w") as f:
                process = sp.Popen(
                    cmd.split(" "),
                    text=True,
                    stdout=sp.PIPE,
                    stderr=sp.STDOUT,
                    bufsize=1,
                    universal_newlines=True,
                )

                # Write output to file in real-time
                for line in process.stdout:
                    f.write(line)
                    f.flush()  # Ensure immediate write to file

                return process.wait()

        thread = threading.Thread(target=run_cmd_in_subprocess, daemon=True)
        thread.start()
        logger.info(f"Job output will be written to: {log_path}")
        return True

    def _get_launch_command(self, job: dict, namespace: str, workflow_id: str) -> str:
        job_folder = constants.SLURM_SERVER_BASE_JOB_FOLDER / job["pid"]
        script_path = constants.SLURM_SERVER_SCRIPTS_FOLDER / "launch_script_locally.sh"
        local_results_path = job_folder / "results.tar.gz"
        remote_results_path = (
            constants.AWS_BASE_JOB_FOLDER / f"{job['pid']}-results.tar.gz"
        )

        return f"""{script_path} \
--job-pid {job['pid']} \
--local-results-path {local_results_path} \
--remote-results-path {remote_results_path} \
--slurm-job-folder {job_folder} \
--endpoint {constants.WEBAPP_BASE_ENDPOINT} \
--namespace {namespace} \
--workflow-id {workflow_id} \
--daemon-api-key {constants.DAEMON_API_KEY}"""

    def cancel_job_fn(self, job_pid: str, process_pid: int) -> bool:
        """
        Checks whether `process_pid` is alive; if so, kills its process group.
        Returns True if a kill signal was sent, False otherwise.
        """
        # 1) check existence
        try:
            os.kill(process_pid, 0)
        except ProcessLookupError:
            logger.info(
                "Process %d for job %s is not running; nothing to cancel",
                process_pid,
                job_pid,
            )
            return False
        except PermissionError as e:
            logger.error(
                "Insufficient permissions to check process %d for job %s: %s",
                process_pid,
                job_pid,
                e,
            )
            return False

        # 2) attempt to terminate the group
        try:
            os.killpg(process_pid, signal.SIGTERM)
            logger.info(
                "Sent SIGTERM to process group (PGID=%d) for job %s",
                process_pid,
                job_pid,
            )
            return True
        except ProcessLookupError:
            # race: it exited between our check and kill
            logger.info(
                "Process group %d for job %s no longer exists",
                process_pid,
                job_pid,
            )
            return False
        except Exception as e:
            logger.error(
                "Failed to kill process group %d for job %s: %s",
                process_pid,
                job_pid,
                e,
            )
            return False


def get_job_handler() -> JobHandler:
    if constants.LAUNCH_IN_LOCAL_MODE:
        return LocalJobHandler()
    return SlurmJobHandler()
