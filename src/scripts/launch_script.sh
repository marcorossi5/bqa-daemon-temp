#!/bin/bash

# ------------------------------------------------------------------
# sbatch Slurm job wrapper
# Ensures robust logging and postprocessing on success, error, or signals
# ------------------------------------------------------------------

set -Eeuo pipefail

# Record script start time for runtime calculation
start_time=$(date +%s.%N)

# Save original args (if needed)
original_args=("$@")

# The first positional argument must be the source folder
source_folder="$1"
# Determine path to helper scripts
scripts_folder="${source_folder}/src/scripts"
# Remove the source_folder arg
shift

# Change into the project source directory
cd "${source_folder}"

# Source CLI parsing and utility functions
source "${scripts_folder}/parsing.sh"
source "${scripts_folder}/utils.sh"

# Parse remaining CLI args to populate variables:
#   wall_time, slurm_job_folder, endpoint, namespace, workflow_id,
#   daemon_api_key, modulefiles_folder, etc.
parse_cli_args "$@"

# Define log filenames and computation module (so finish() can reference err_log)
out_log="stdout.log"
err_log="stderr.log"
computation_module="src.slurm_bqa_computation"

# Function to run on exit, error, or signal to finalize logging and notify service
finish() {
  local exit_code=$1

  # Prevent double execution
  if [[ "${_finish_called:-}" == "true" ]]; then
    return
  fi
  _finish_called="true"

  # Calculate elapsed seconds with millisecond precision
  local end_time=$(date +%s.%N)
  local elapsed
  elapsed=$(echo "scale=3; $end_time - $start_time" | bc -l)

  # Ensure the job folder exists before writing logs
  mkdir -p "${slurm_job_folder}"

  # If killed by SIGKILL (exit_code 137), append a clear message
  if [ "$exit_code" -eq 137 ]; then
    echo "ERROR: job killed because it exceeded max runtime (exit code 137)" \
      >> "${slurm_job_folder}/${err_log}"
  fi

  # Write exit status and timing
  printf "%d\n" "$exit_code" > "${slurm_job_folder}/exit_status.log"
  printf "%s\n" "$elapsed"           > "${slurm_job_folder}/seconds_spent.log"

  # Notify the daemon; warn but don't fail if this call errors
  # if declare -F postprocessing_db_update &>/dev/null; then
  #   postprocessing_db_update || {
  #     echo "WARNING: postprocessing_db_update failed" >&2
  #   }
  # fi
}

# Trap EXIT, ERR, TERM, and INT to ensure finish() always runs once
trap 'finish $?' EXIT ERR TERM INT

# Load environment modules
source /etc/profile.d/modules.sh
export MODULEPATH="${modulefiles_folder}:$MODULEPATH"
module load bqa

# Mark job as running in the system
# change_job_status "running"

# Notify the daemon that the job is starting
# job_starting

# Run the computation under timeout, capturing stdout/stderr to logs
timeout -s SIGKILL "${wall_time}" \
  python -u -m "${computation_module}" "${slurm_job_folder}" \
    > "${slurm_job_folder}/${out_log}" \
    2> "${slurm_job_folder}/${err_log}"

# End of wrapper; finish() will be invoked automatically
