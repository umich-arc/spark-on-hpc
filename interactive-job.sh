#!/bin/bash
#SBATCH --job-name=spark-cluster
#SBATCH --account=support        # change to your account
#SBATCH --partition=standard
#SBATCH --nodes=2                # node count, change as needed
#SBATCH --ntasks-per-node=1      # do not change, leave as 1 task per node
#SBATCH --cpus-per-task=36       # cpu-cores per task per node, change as needed
#SBATCH --mem=180g               # memory per node, change as needed
#SBATCH --time=00:60:00
#SBATCH --mail-type=NONE

module load spark python3.8-anaconda pyarrow

./spark-start
source spark-env.sh

echo "***** Spark cluster is running. Access the Web UI at ${SPARK_MASTER_WEBUI}. *****"
sleep infinity
