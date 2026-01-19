import os
from pathlib import Path

BASE_JOB_FOLDER = Path(os.getenv("BASE_JOB_FOLDER", "./jobs"))
BASE_LOGS_FOLDER = Path(os.getenv("BASE_LOGS_FOLDER", "./jobs/logs"))

SLURM_SERVER_BASE_JOB_FOLDER = Path(os.getenv("SLURM_SERVER_BASE_JOB_FOLDER", "./jobs"))
SLURM_SERVER_BASE_LOGS_FOLDER = Path(
    os.getenv("SLURM_SERVER_BASE_LOGS_FOLDER", "./jobs/logs")
)
SLURM_SERVER_SOURCE_FOLDER = Path(os.getenv("SLURM_SERVER_SOURCE_FOLDER", "./"))
SLURM_SERVER_SCRIPTS_FOLDER = Path(
    os.getenv("SLURM_SERVER_SCRIPTS_FOLDER", "./src/daemon/scripts")
)

# webapp server
AWS_BASE_JOB_FOLDER = Path(os.getenv("AWS_BASE_JOB_FOLDER", "./jobs"))
WEBAPP_BASE_ENDPOINT = os.getenv("WEBAPP_BASE_ENDPOINT")

# ssh to slurm host
SLURM_SERVER_HOST = os.getenv("SLURM_SERVER_HOST")
SLURM_SERVER_PORT = os.getenv("SLURM_SERVER_PORT")
SLURM_SERVER_USER = os.getenv("SLURM_SERVER_USER")
SLURM_SERVER_SSH_KEY_PATH = os.getenv("SLURM_SERVER_SSH_KEY_PATH")
SLURM_SSH_CONFIG_PATH = os.getenv("SLURM_SSH_CONFIG_PATH")

# daemon settings
DAEMON_API_KEY = os.getenv("DAEMON_API_KEY")
DAEMON_LAB_LOCATION = os.getenv("DAEMON_LAB_LOCATION")

LAUNCH_IN_LOCAL_MODE = bool(os.getenv("LAUNCH_IN_LOCAL_MODE", False))
MODULEFILES_FOLDER = os.getenv("MODULEFILES_FOLDER", "")
