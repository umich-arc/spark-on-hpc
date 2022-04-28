# Spark on Great Lakes and Armis2

These instructions show how to run a stand-alone Spark instance on the Great Lakes and Armis2 HPC clusters. This code is intended for researchers who need to submit a batch job to Spark and are comfortable with the technical details of sizing a stand-alone Spark instance. If instead, you are looking to interactively explore data through a Jupyter Notebook with Spark integration without a large learning curve, please use the Open OnDemand Jupyter + Spark app available through a web browser at https://greatlakes.arc-ts.umich.edu.

To launch a Spark instance on an HPC cluster, first, you need to clone this repo on Great Lakes to get a copy of the example slurm jobs scripts and the `spark-start` script. Next, you will customize the example slurm job scripts with your account name and the resources you require for your spark job. Lastly, you will run the slurm job script which will call the `spark-start` script which launches a standalone Spark cluster for your use and then submit a batch job with `spark-submit`. The `examples` directory contains some example code.

## Clone This Repo

```bash
ssh greatlakes.arc-ts.umich.edu
git clone git@github.com:arc-ts/spark-on-great-lakes.git
cd ./spark-on-great-lakes
```


## Running a Batch Job

Next, you will need to write a slurm job script that calls `spart-start`. It is recommended that you copy the example script `batch-job.sh` and customize it for your use. You need to update the slurm information to use your Great Lakes or Armis2 account as well as specify the resources you wish to allocate to your Spark cluster.

The `batch-job.sh` file ends with a `spark-submit` command that will submit a batch job to your Spark cluster. You must modify this command to reference your spark job code. Additionally, you must modify the command line options `--executor-cores`, `--executor-memory`, and `--total-executor-cores` to explicitly specify the resources you desire for your spark job. Otherwise, your job may have significantly fewer resources than you intended because these parameters have low default values. Typically, you will set these parameter values to be slightly less than the resources requested in your slurm job script to allow for spark overhead. As a rule of thumb, the spark cluster consumes roughly 2 cpu cores and 10g overhead. Additional details on resource allocation are later in this document.

```bash
# Copy and customize the slurm job script to match your needs.
vi batch-job.sh

# Run the slurm job script which will start the cluster then run your spark job.
sbatch ./batch-job.sh

# Optionally, view slurm output as the job runs.
tail -f slurm*.out
```

## Customizing Spark Driver Memory

By default, a spark job will start a `driver` process using 5g of memory. If you need to increase the spark `driver` memory allocation, you can use the command line option `--driver-memory`. The example below launches a spark job with `driver` memory of 10g.

```bash
spark-submit --master ${SPARK_MASTER_URL} \
  --driver-memory 10g \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./my-spark-job.py
```

## Cluster Architecture and Resource Allocation

This section is provided if you would like a deeper understanding of the underlying architecture and resource allocation of the spark cluster. The `batch-job.sh` slurm job script is used as an example.

The first section of the script request 2 compute nodes with 36 cores and 180 GB of memory for each node. When run, this script allocates 72 cores and 360 GB memory in total. (If you need more resources, you can increase the number of nodes. If you need less resources, you can reduce the number of nodes to 1.)

After slurm has allocated the resources requested, the `batch-job.sh` script then calls the `spark-start` script which launches a Spark cluster in standalone mode on the 2 slurm compute nodes. Although slurm will have allocated 72 cores and 360 GB memory in total, the amount of compute resources available to spark jobs will be slightly less due to overhead processes of the spark cluster. To determine the total resources available to spark jobs, take the total slurm resource allocation and reduce it by 2 cpu cores and 10g of memory. In this example, the largest spark job that could be started would use 70 cpu cores and 350g memory. The following paragraphs detail the spark processes that make up this overhead for those interested.

An idle spark cluster consists of a single spark `master` process running on ONE of the slurm compute nodes and a single `worker` process runnning on EACH of the slurm compute nodes. These processes function as a control plane for the cluster and work together to receive and execute spark job requests. Each of these processes consume 1g of memory. Additionally, when a spark job is started, the spark cluster starts a `driver` process on the same slurm compute node running the `master` process. The spark `driver` process defaults to 5g memory, but this value can be customized if more driver memory is needed.

To account for these overhead processes, the `spark-start` script configures the spark cluster capacity with slightly less resources than the amount allocated by slurm. The total compute resources available to spark jobs will be 2 cpu cores and 10g memory less than the total allocated by slurm.

In the example above, this would leave 70 cpu cores and 350g memory available for the executors of a spark job. The command `spark-submit --master ${SPARK_MASTER_URL} --executor-cores 1 --executor-memory 5G --total-executor-cores 70 ...` would start a spark job that consumes all available resources. The command launches 70 containers where each container uses 1 cpu core and 5g memory.

## Security

The `spark-start` script will enables RPC authentication and generate a random secret that is distributed to the worker nodes. In order to submit jobs to the cluster, a user must have this secret. These secrets are added to the environment when `batch-jobs.sh` sources `spark-env.sh`.
