#!/bin/bash

# Submits slurm job to start spark cluster and jupyter server. Watches
# slurm log file until jupyter URL is returned. Prints URL.

SLURM_TIMEOUT=60 # Give up if job not started after this duration.
SLURM_JOB_ID=$(sbatch --parsable ./jupyter-job.sh)
SLURM_LOG=slurm-${SLURM_JOB_ID}.out

echo -n "Cluster starting."

x=1
while [ $x -lt $SLURM_TIMEOUT ]
do
  echo -n "."
  x=$(( $x + 1 ))
  if [ -f "$SLURM_LOG" ]; then
    if grep -q "http://gl" ${SLURM_LOG}; then
      sleep 2
      JUPYTER_WEBUI=$(grep -m 1 "http://gl" ${SLURM_LOG} | sed 's/^.*http:/http:/g')
      x=$SLURM_TIMEOUT
    fi
  fi
  sleep 1
done

echo -e "\nJupyter server is running at ${JUPYTER_WEBUI} "
echo "Start a Great Lakes Remote Desktop and open a web browser to this address."
echo "Stop the Jupyter server and Spark cluster with 'scancel ${SLURM_JOB_ID}'."
