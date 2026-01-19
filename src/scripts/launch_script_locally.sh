#!/bin/bash
out_log="stdout.log"
err_log="stderr.log"

scripts_folder="/app/src/scripts"
source_folder=$(dirname $(dirname $scripts_folder))

# go to source folder
cd ${source_folder}

source ${scripts_folder}/parsing.sh
source ${scripts_folder}/utils.sh

echo "Parse args"
parse_cli_args "$@"

change_job_status "running"

computation_module="src.slurm_bqa_computation"

# compute computation runtime
start_time=$(date +%s.%N)

# launch circuit computation
python -u -m $computation_module $slurm_job_folder \
  > "${slurm_job_folder}/${out_log}" \
  2> "${slurm_job_folder}/${err_log}"

exit_status=$?

end_time=$(date +%s.%N)

seconds_spent=$(echo "$end_time - $start_time" | bc)

if [ $exit_status -eq 0 ]; then
    final_status="success"
else
    final_status="error"
fi

echo $final_status > ${slurm_job_folder}/"exit_status.log"
echo $seconds_spent > ${slurm_job_folder}/"seconds_spent.log"

postprocessing_db_update
