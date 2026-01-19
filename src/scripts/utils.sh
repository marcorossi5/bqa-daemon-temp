#!/bin/bash

create_results_archive() {
  echo "Create results archive"
  cd $slurm_job_folder > /dev/null
  tar -cvzf results.tar.gz *
  archive_size_bytes=$(stat -c%s "./results.tar.gz")
  kbs_spent=$(echo "scale=3; ${archive_size_bytes} / 1024" | bc -l)
  cd ${source_folder}
}

transfer_archive_to_local_webapp() {
    echo "Transfer file"
    /usr/bin/cp ${local_results_path} ${remote_results_path}
}

change_job_status() {
  # Usage: `change_job_status <new status>`
  echo "Changing job status to $1"
  slurm_change_job_status_module="src.slurm_change_job_status"
  python -u -m $slurm_change_job_status_module $job_pid \
    --status $1 \
    --endpoint $endpoint \
    --daemon_api_key $daemon_api_key

}

postprocessing_db_update() {
  echo "Job finished"
  curl -s --retry 5 --retry-delay 2 --retry-connrefused -X POST "${endpoint}/daemon/slurm-job-end/" \
    -H "Authorization: Api-Key ${daemon_api_key}" \
    -H "Content-Type: application/json" \
    -d @- <<EOF
{
  "namespace":  "${namespace}",
  "workflow_id": "${workflow_id}"
}
EOF
}


job_starting() {
  echo "Job starting"
  curl -s --retry 5 --retry-delay 2 --retry-connrefused -X POST "${endpoint}/daemon/slurm-job-start/" \
    -H "Authorization: Api-Key ${daemon_api_key}" \
    -H "Content-Type: application/json" \
    -d @- <<EOF
{
  "namespace":  "${namespace}",
  "workflow_id": "${workflow_id}"
}
EOF
}