# Spark on Great Lakes

These instructions show how to run Spark on Great Lakes. First, you need to clone this repo on Great Lakes to get a copy of the example slurm jobs scripts and the `spark-start` script. Next, you will customize the example slurm job scripts with your account name and the resources you require for your spark job. Lastly, you will run the slurm job script which will call the `spark-start` script which launches a standalone Spark cluster for your use.

Spark jobs can be run either interactively, as with Jupyter Notebook, `pyspark` and other tools, or run as a batch job with `spark-submit`. The different approaches are shown below.

## Running a Jupyter Notebook

```bash
# Start on the Great Lakes login node. Clone this repo and cd to the repo.

# Customize the slurm job script to use your Great Lakes account and
# modify the compute resources to match your needs.
vi jupyter-job.sh

# Run the helper script which will start the spark cluster and jupyter notebook.
./start-jupyter.sh
```

- After the spark cluster starts, copy the Jupyter URL.
- Open a Great Lakes Remote Desktop session
- Open the Firefox web browser
- Paste the Jupyter URL in the browser

## Running an Interactive Job

```bash
# Start on the Great Lakes login node. Clone this repo and cd to the repo.

# Customize the slurm job script to use your Great Lakes account and
# modify the slurm compute resources to match your needs.
vi interactive-job.sh

# Run the slurm job script which will start the spark cluster.
sbatch ./interactive-job.sh

# Wait a few seconds for the spark cluster to start, then SSH to the spark master node.
ssh $(cat ./master.txt)

# From the spark master node, load these modules.
module load spark python3.8-anaconda pyarrow

# Source this script to set some useful env vars related to the spark cluster.
cd spark-on-great-lakes
source spark-env.sh

# Run pyspark to connect to the cluster and perform your work.
# If needed, modify the pyspark resources below to match the slurm resources.
pyspark --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70
```

Note that you cannot connect to the spark cluster from a login node because of network controls. As shown above, you MUST ssh to a compute node before running `pyspark`.

Also, as in the example above, it is best to use spark command line options `--executor-cores`, `--executor-memory`, and `--total-executor-cores` to explicitly specify the resources you desire for your spark job. Otherwise, your job may have significantly fewer resources than you intended because these parameters have low default values. Typically, you will set these parameter values to be slightly less than the resources requested in your slurm job script to allow for spark overhead. As a rule of thumb, the spark cluster consumes roughly 2 cpu cores and 10g overhead. Additional details on resource allocation are later in this document.

## Running a Batch Job

```bash
# Start on the Great Lakes login node. Clone this repo and cd to the repo.

# Customize the slurm job script to use your Great Lakes account and
# modify the compute resources to match your needs. Also modify the last line
# to reference the location your spark job, and customize the resources
# you need.
vi batch-job.sh

# Run the slurm job script which will start the cluster and then it will
# run your spark job.
sbatch ./batch-job.sh

# Optionally, view slurm output as the job runs.
tail -f slurm*.out
```

Note that the slurm job scripts `interactive-job.sh` and `batch-job.sh` only differ by 1 line. In the interactive job example, the script calls `sleep infinity` after launching the standalone spark cluster. This causes the cluster to continue running in the background so that you can connect to it via `pyspark` or other tools. In the batch job example, the `sleep` command is replaced with a `spark-submit` command which runs a batch spark job and then terminates the cluster.

## Customizing Spark Driver

By default, a spark job will start a `driver` process using 5g of memory. If you need to increase the spark `driver` memory allocation, you can use the command line option `--driver-memory`. The example below launches a spark job with `driver` memory of 10g.

```bash
pyspark --master ${SPARK_MASTER_URL} \
  --driver-memory 10g \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70
```

## Cluster Architecture and Resource Allocation

This section is provided if you would like a deeper understanding of the underlying architecture and resource allocation of the spark cluster. The `interactive-job.sh` slurm job script is used as an example.

The first lines of the script request 2 compute nodes with 36 cores and 180 GB of memory for each node. When run, this script allocates 72 cores and 360 GB memory in total. (If you need more resources, you can increase the number of nodes. If you need less resources, you can reduce the number of nodes to 1.)

After slurm has allocated the resources requested, the `interactive-job.sh` script then calls the `start-spark` script which launches a Spark cluster in standalone mode on the 2 slurm compute nodes. Although slurm will have allocated 72 cores and 360 GB memory in total, the amount of compute resources available to spark jobs will be slightly less due to overhead processes of the spark cluster. To determine the total resources available to spark jobs, take the total slurm resource allocation and reduce it by 2 cpu cores and 10g of memory. In this example, the largest spark job that could be started would use 70 cpu cores and 350g memory. The following paragraphs detail the spark processes that make up this overhead for those interested.

An idle spark cluster consists of a single spark `master` process running on ONE of the slurm compute nodes and a single `worker` process runnning on EACH of the slurm compute nodes. These processes function as a control plane for the cluster and work together to receive and execute spark job requests. Each of these processes consume 1g of memory. Additionally, when a spark job is started, the spark cluster starts a `driver` process on the same slurm compute node running the `master` process. The spark `driver` process defaults to 5g memory, but this value can be customized if more driver memory is needed.

To account for these overhead processes, the `spark-start` script configures the spark cluster capacity with slightly less resources than the amount allocated by slurm. The total compute resources available to spark jobs will be 2 cpu cores and 10g memory less than the total allocated by slurm.

In the example above, this would leave 70 cpu cores and 350g memory available for the executors of a spark job. The command `pyspark --master ${SPARK_MASTER_URL} --executor-cores 1 --executor-memory 5G --total-executor-cores 70` would start a spark job that consumes all available resources. The command launches 70 containers where each container uses 1 cpu core and 5g memory.

## Security

The `spark-start` script will enable RPC authentication and generate a random secret that is distributed to the worker nodes. In order to submit jobs to the cluster, a user must have this secret. The will be added to your environment when you `source spark-env.sh`.
