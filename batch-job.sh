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

module load spark python3.8-anaconda pyarrow

./spark-start
source spark-env.sh

echo "***** Spark cluster is running. Access the Web UI at ${SPARK_MASTER_WEBUI}. *****"

# Change executor resources below to match resources requested above
# with an allowance for spark driver overhead.
# Change the path to your spark job.
spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ${SPARK_HOME}/examples/src/main/python/pi.py 10000
