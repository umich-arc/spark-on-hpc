# Spark on Great Lakes and Armis2

These instructions show how to run a stand-alone Spark instance on the Great Lakes and Armis2 HPC clusters. This code is intended for researchers who need to submit a batch job to Spark and are comfortable with the technical details of sizing a stand-alone Spark instance. If instead, you are looking to interactively explore data through a Jupyter Notebook with Spark integration without a large learning curve, please use the Open OnDemand Jupyter + Spark app available through a web browser at https://greatlakes.arc-ts.umich.edu or https://armis2.arc-ts.umich.edu.

To launch a Spark instance on an HPC cluster, you need to copy the example slurm job script `batch-job.sh` and customize it with your slurm account name, the resources you require for your Spark job, and the location of your Spark code. Then, you will run the slurm job script with `sbatch batch-job.sh`. As the slurm job runs, it will call the `spark-start` script which launches a standalone Spark cluster and submits a Spark batch job with `spark-submit`. When the Spark job finishes, the slurm job will terminate. Spark driver logs are written to the slurm job's output log. The `examples` directory in this repo contains some example Spark code.

## An Example

Copy and customize the `batch-job.sh` slurm job script to use your Great Lakes or Armis2 account as well as specify the resources you wish to allocate to your Spark cluster. Notice that the `batch-job.sh` file ends with a `spark-submit` command that will submit a batch job to your Spark cluster. You must modify this command to reference your Spark job code.

Additionally, you must modify the `spark-submit` command line options `--executor-cores`, `--executor-memory`, and `--total-executor-cores` to explicitly specify the resources you desire for your Spark job. Otherwise, your job may have significantly fewer resources than you intended because these parameters have low default values. Typically, you will set these parameter values to be slightly less than the resources requested in your slurm job script to allow for Spark overhead. As a rule of thumb, the Spark cluster consumes roughly 2 cpu cores and 10g overhead. Additional details on resource allocation are later in this document.

```bash
# Copy and customize the slurm job script to match your needs.
cp /sw/examples/spark/spark-on-hpc/batch-job.sh ~/batch-job.sh
vi ~/batch-job.sh

# Run the slurm job script which will start the cluster then run your Spark job.
sbatch ~/batch-job.sh

# Optionally, view slurm output as the job runs.
tail -f slurm*.out
```

## Customizing Spark Driver Memory

By default, a Spark job will start a `driver` process using 5g of memory. If you need to increase the Spark `driver` memory allocation, you can use the command line option `--driver-memory`. The example below launches a Spark job with `driver` memory of 10g.

```bash
spark-submit --master ${SPARK_MASTER_URL} \
  --driver-memory 10g \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./my-spark-job.py
```

## Cluster Architecture and Resource Allocation

This section is provided if you would like a deeper understanding of the underlying architecture and resource allocation of the Spark cluster. The `batch-job.sh` slurm job script is used as an example.

The first section of the script request 2 compute nodes with 36 cores and 180 GB of memory for each node. When run, this script allocates 72 cores and 360 GB memory in total. (If you need more resources, you can increase the number of nodes. If you need less resources, you can reduce the number of nodes to 1.)

After slurm has allocated the resources requested, the `batch-job.sh` script then calls the `spark-start` script which launches a Spark cluster in standalone mode on the 2 slurm compute nodes. Although slurm will have allocated 72 cores and 360 GB memory in total, the amount of compute resources available to spark jobs will be slightly less due to overhead processes of the spark cluster. To determine the total resources available to spark jobs, take the total slurm resource allocation and reduce it by 2 cpu cores and 10g of memory. In this example, the largest spark job that could be started would use 70 cpu cores and 350g memory. The following paragraphs detail the spark processes that make up this overhead for those interested.

An idle spark cluster consists of a single spark `master` process running on ONE of the slurm compute nodes and a single `worker` process runnning on EACH of the slurm compute nodes. These processes function as a control plane for the cluster and work together to receive and execute spark job requests. Each of these processes consume 1g of memory. Additionally, when a spark job is started, the spark cluster starts a `driver` process on the same slurm compute node running the `master` process. The spark `driver` process defaults to 5g memory, but this value can be customized if more driver memory is needed.

To account for these overhead processes, the `spark-start` script configures the spark cluster capacity with slightly less resources than the amount allocated by slurm. The total compute resources available to spark jobs will be 2 cpu cores and 10g memory less than the total allocated by slurm.

In the example above, this would leave 70 cpu cores and 350g memory available for the executors of a spark job. The command `spark-submit --master ${SPARK_MASTER_URL} --executor-cores 1 --executor-memory 5G --total-executor-cores 70 ...` would start a spark job that consumes all available resources. The command launches 70 containers where each container uses 1 cpu core and 5g memory.

## Using Spark with R Language

Note that SparkR is deprecated in Spark 4 (released May 2025) and will be removed starting with Spark 5. The Spark project owners recommend using `sparklyr` as a replacement. If you have an interest in trying `sparklyr`, please let us at ARC know (4HELP@umich.edu), and we will explore making the library available with the Spark installation on the HPC clusters.

Until that time, you can use R with Spark on the HPC clusters by using the [SparkR package](https://spark.apache.org/docs/latest/sparkr.html). For interactive, exploratory data analysis in R, you can start an Open OnDemand Basic Desktop on one of the [HPC clusters](https://greatlakes.arc-ts.umich.edu/). Then open a terminal, and execute these commands.

```
module load spark R
sparkR
```

The commands will load the version of Spark and R you specify, then start an R console connected to a local Spark instance. Note that this approach will not scale beyond a single HPC node, but it is a good way to develop and test code. Later, if additional resources are needed, you can submit the R code as a batch job.

If you prefer RStudio for exploratory data analysis, you can start an Open OnDemand RStudio app on one of the [HPC clusters](https://greatlakes.arc-ts.umich.edu/). Configure the app to `load spark/3.5` to load the spark module. Once the RStudio app has started, load the SparkR package and start a local Spark instance with these commands.

```
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]")
```

If you have developed your code, but you need more resources than are provided by a single compute node, you can scale out the application by submiting it as a batch slurm job. Save your R code to a `.R` file. Make a copy of `batch-job-with-R.sh` then customize it with the location of your R code, your slurm account, and the compute resources you need. Then submit the slurm job with these commands.

```bash
# Copy and customize the slurm job script to match your needs.
cp /sw/examples/spark/spark-on-hpc/batch-job-with-R.sh ~/batch-job-with-R.sh
vi ~/batch-job-with-R.sh

# Run the slurm job script which will start the cluster then run your Spark job.
sbatch ~/batch-job-with-R.sh

# Optionally, view slurm output as the job runs.
tail -f slurm*.out
```

## Security

The `spark-start` script enables RPC authentication to protect the spark service.
