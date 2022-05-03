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

# These modules are required.
module load spark python3.8-anaconda pyarrow

# Start the Spark instance.
./spark-start

# Source spark-env.sh to get useful env variables.
source ${HOME}/.spark-local/${SLURM_JOB_ID}/spark/conf/spark-env.sh

# Set an executor configuration.
export SPARK_EXECUTOR_CORES=1
export SPARK_EXECUTOR_MEMORY=5

# Calculate the maximum number of executors the cluster will support.
SPARK_MAX_NUM_EXECUTOR_BY_CORES=$(( SPARK_CLUSTER_CORES / SPARK_EXECUTOR_CORES ))
SPARK_MAX_NUM_EXECUTOR_BY_MEMORY=$(( SPARK_CLUSTER_MEMORY / SPARK_EXECUTOR_MEMORY ))
if [ ${SPARK_MAX_NUM_EXECUTOR_BY_CORES} -ne ${SPARK_MAX_NUM_EXECUTOR_BY_MEMORY} ]; then
    echo "Warning: There is a resource mismatch."
    echo "Executor configuration:"
    echo "  - ${SPARK_EXECUTOR_CORES} cores"
    echo "  - ${SPARK_EXECUTOR_MEMORY}G memory"
    echo "Spark cluster total capacity for executors:"
    echo "  - ${SPARK_CLUSTER_CORES} cores"
    echo "  - ${SPARK_CLUSTER_MEMORY}G memory"
    echo "Spark cluster has capacity to run the lesser of ${SPARK_MAX_NUM_EXECUTOR_BY_CORES} or ${SPARK_MAX_NUM_EXECUTOR_BY_MEMORY} executors."
    echo "Consider adjusting the spark cluster or executor configuration to avoid wasting resources."
    if [ ${SPARK_MAX_NUM_EXECUTOR_BY_MEMORY} -lt ${SPARK_MAX_NUM_EXECUTOR_BY_CORES} ]; then
        SPARK_MAX_NUM_EXECUTOR=${SPARK_MAX_NUM_EXECUTOR_BY_MEMORY}
    else
        SPARK_MAX_NUM_EXECUTOR=${SPARK_MAX_NUM_EXECUTOR_BY_CORES}
    fi
else
    SPARK_MAX_NUM_EXECUTOR=${SPARK_MAX_NUM_EXECUTOR_BY_CORES}
fi
SPARK_TOTAL_EXECUTOR_CORES=$(( SPARK_MAX_NUM_EXECUTOR * SPARK_EXECUTOR_CORES ))

# Customize the executor resources below to match resources requested above
# with an allowance for spark driver overhead. Also change the path to your spark job.

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores ${SPARK_EXECUTOR_CORES} \
  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
  ./examples/logistic_regression_with_lbfgs_example.py

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores ${SPARK_EXECUTOR_CORES} \
  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
  ./examples/nltk-test.py

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores ${SPARK_EXECUTOR_CORES} \
  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
  ./examples/numeric-integration.py

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores ${SPARK_EXECUTOR_CORES} \
  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
  ./examples/numpy-test.py

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores ${SPARK_EXECUTOR_CORES} \
  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
  ./examples/pi.py

# This example only works if you have read permission to the twitter decahose on Great Lakes.
#spark-submit --master ${SPARK_MASTER_URL} \
#  --executor-cores ${SPARK_EXECUTOR_CORES} \
#  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
#  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
#  ./examples/twitter-decahose-test.py /nfs/turbo/twitter-decahose/decahose/raw/decahose.2020-05-25.p1.bz2

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores ${SPARK_EXECUTOR_CORES} \
  --executor-memory ${SPARK_EXECUTOR_MEMORY}G \
  --total-executor-cores ${SPARK_TOTAL_EXECUTOR_CORES} \
  ./examples/word-count.py /nfs/turbo/arcts-data-hadoop-stage/data/Gutenberg.txt
