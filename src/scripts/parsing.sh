#!/bin/bash

# ------------------------------------------------------------------
# parsing.sh
#
# CLI argument parsing for Slurm job wrapper.
# Supports both "--flag=value" and "--flag value" forms.
# ------------------------------------------------------------------

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  -h, --help                   Display this help message
  --wall-time <time>           Dry computation runtime quota (e.g. 10m)
  --job-pid <pid>              Job PID
  --slurm-job-folder <path>    Job folder on the Slurm machine
  --aws-server-host <host>     AWS server IP address or hostname
  --local-results-path <path>  Local results path
  --remote-results-path <path> Remote results path
  --ssh-config-path <path>     Path to the SSH config file
  --archive-local-path <path>  Path to local archive output
  --archive-remote-path <path> Path to AWS archive output
  --endpoint <url>             Webapp public endpoint
  --namespace <ns>             Workflow namespace
  --workflow-id <id>           Workflow ID
  --daemon-api-key <key>       Daemon API key
  --modulefiles-folder <path>  Modulefiles folder
EOF
}

parse_cli_args() {
  local key
  while [[ $# -gt 0 ]]; do
    key="$1"
    case "$key" in
      -h|--help)
        usage
        exit 0
        ;;

      --wall-time=*)
        wall_time="${key#*=}"; shift
        ;;
      --wall-time)
        shift; wall_time="$1"; shift
        ;;

      --job-pid=*)
        job_pid="${key#*=}"; shift
        ;;
      --job-pid)
        shift; job_pid="$1"; shift
        ;;

      --slurm-job-folder=*)
        slurm_job_folder="${key#*=}"; shift
        ;;
      --slurm-job-folder)
        shift; slurm_job_folder="$1"; shift
        ;;

      --local-results-path=*)
        local_results_path="${key#*=}"; shift
        ;;
      --local-results-path)
        shift; local_results_path="$1"; shift
        ;;

      --remote-results-path=*)
        remote_results_path="${key#*=}"; shift
        ;;
      --remote-results-path)
        shift; remote_results_path="$1"; shift
        ;;

      --ssh-config-path=*)
        ssh_config_path="${key#*=}"; shift
        ;;
      --ssh-config-path)
        shift; ssh_config_path="$1"; shift
        ;;

      --archive-local-path=*)
        archive_local_path="${key#*=}"; shift
        ;;
      --archive-local-path)
        shift; archive_local_path="$1"; shift
        ;;

      --archive-remote-path=*)
        archive_remote_path="${key#*=}"; shift
        ;;
      --archive-remote-path)
        shift; archive_remote_path="$1"; shift
        ;;

      --endpoint=*)
        endpoint="${key#*=}"; shift
        ;;
      --endpoint)
        shift; endpoint="$1"; shift
        ;;

      --namespace=*)
        namespace="${key#*=}"; shift
        ;;
      --namespace)
        shift; namespace="$1"; shift
        ;;

      --workflow-id=*)
        workflow_id="${key#*=}"; shift
        ;;
      --workflow-id)
        shift; workflow_id="$1"; shift
        ;;

      --daemon-api-key=*)
        daemon_api_key="${key#*=}"; shift
        ;;
      --daemon-api-key)
        shift; daemon_api_key="$1"; shift
        ;;

      --modulefiles-folder=*)
        modulefiles_folder="${key#*=}"; shift
        ;;
      --modulefiles-folder)
        shift; modulefiles_folder="$1"; shift
        ;;

      *)
        echo "Error: Invalid option '$key'" >&2
        usage
        exit 1
        ;;
    esac
  done

  # Validate required parameters
  : "${wall_time:?ERROR: --wall-time is required}"
  : "${slurm_job_folder:?ERROR: --slurm-job-folder is required}"
  : "${endpoint:?ERROR: --endpoint is required}"
  : "${namespace:?ERROR: --namespace is required}"
  : "${workflow_id:?ERROR: --workflow-id is required}"
  : "${daemon_api_key:?ERROR: --daemon-api-key is required}"
  : "${modulefiles_folder:?ERROR: --modulefiles-folder is required}"
}
