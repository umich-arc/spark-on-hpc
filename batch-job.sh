#!/bin/bash
#SBATCH --job-name=spark-cluster
#SBATCH --account=support        # change to your account
#SBATCH --partition=standard
#SBATCH --nodes=2                # node count, change as needed
#SBATCH --ntasks-per-node=1      # do not change, leave as 1 task per node
#SBATCH --cpus-per-task=36       # cpu-cores per task, change as needed
#SBATCH --mem=180g               # memory per node, change as needed
#SBATCH --time=00:60:00
#SBATCH --mail-type=NONE

# A spark and python module are required. You can select the module version
# you need depending on which cluster you are on.
module load spark/3.5 python/3.12

# Start the Spark instance.
spark-start

# Source spark-env.sh to get useful env variables.
source ${HOME}/.spark-local/${SLURM_JOB_ID}/spark/conf/spark-env.sh

# Customize the executor resources below to match resources requested above
# with an allowance for spark driver overhead. Also change the path to your spark job.
spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  /sw/examples/spark/spark-on-hpc/examples/pi.py
