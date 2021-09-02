#!/bin/bash
#SBATCH --job-name=spark-cluster-with-jupyter
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

JUPYTER_PATH=$(which jupyter)
JUPYTER_PORT=8888
export PYSPARK_DRIVER_PYTHON=${JUPYTER_PATH}
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --NotebookApp.ip='0.0.0.0' --port ${JUPYTER_PORT}"

echo "JUPYTER_WEBUI=http://${SPARK_MASTER_HOST}:${JUPYTER_PORT}" >> spark-env.sh

pyspark --master ${SPARK_MASTER_URL} \
  --driver-memory 10G \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70

echo "***** Spark cluster is running. Access the Web UI at ${SPARK_MASTER_WEBUI}. *****"
sleep infinity
