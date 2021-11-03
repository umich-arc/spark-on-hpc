# Data Science Examples

This repo contains example code that demonstrates the data science software available on Great Lakes and Cavium ThunderX. The code utilizes Spark, Python and other libraries commonly used for data science.

## Great Lakes Usage

The examples are provided as a single Jupyter Notebook `examples-notebook.ipynb`. To use on Great Lakes, launch the [Jupyter + Spark Open OnDemand application](https://greatlakes.arc-ts.umich.edu/). Then run the `examples-notebook.ipynb` notebook.

In addition to the Jupyter Notebook, the examples can also be run individually from the command line as below.

```bash
# Start an interactive spark cluster on Great Lakes using the README
# instructions from https://github.com/arc-ts/spark-on-great-lakes.

# Submit jobs with spark-submit.
spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./advanced-examples/pi.py 1500

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./advanced-examples/word-count.py /nfs/turbo/arcts-data-hadoop-stage/data/Gutenberg.txt

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./advanced-examples/numpy-test.py

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./advanced-examples/numeric-integration.py

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 10G \
  --total-executor-cores 35 \
  ./advanced-examples/logistic_regression_with_lbfgs_example.py /nfs/turbo/arcts-data-hadoop-stage/data/sample_svm_data.txt

spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./advanced-examples/nltk-test.py /nfs/turbo/arcts-data-hadoop-stage/data/complete-works-of-shakespeare.txt

# This example only works if you have read permission to the twitter decahose on Great Lakes.
spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 1 \
  --executor-memory 5G \
  --total-executor-cores 70 \
  ./advanced-examples/twitter-decahose-test.py /nfs/turbo/twitter-decahose/decahose/raw/decahose.2020-05-25.p1.bz2
```

## Cavium ThunderX Usage

The examples can also be run on Cavium ThunderX.

The example below shows how to launch Jupyter on Cavium ThunderX. After launching Jupyter, run the `examples-notebook.ipynb` notebook.

```bash
ssh -l <UNIQNAME> -L localhost:8889:localhost:8889 cavium-thunderx.arc-ts.umich.edu
export PYSPARK_DRIVER_PYTHON=$(which jupyter)
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8889'

pyspark --master yarn --num-executors 30
```

In addition to the Jupyter Notebook, the examples can also be run individually from the command line as below.

```bash
ssh <UNIQNAME>@cavium-thunderx.arc-ts.umich.edu

git clone <THIS_REPO>
cd thunderx-smoke

spark-submit --master yarn \
  --num-executors 30 \
  ./pi.py 1500

spark-submit --master yarn \
  --num-executors 9 \
  ./word-count.py /data/Gutenberg.txt

spark-submit --master yarn \
  --num-executors 20 \
  ./numpy-test.py

spark-submit --master yarn \
  --num-executors 5 \
  ./numeric-integration.py

spark-submit --master yarn \
  --num-executors 40 \
  ./logistic_regression_with_lbfgs_example.py /data/sample_svm_data.txt

spark-submit --master yarn \
  --num-executors 24 \
  nltk-test.py /data/complete-works-of-shakespeare.txt

# This example only works if you have read permission to the twitter decahose on ThunderX.
spark-submit --master yarn \
    --num-executors 24 \
    twitter-decahose-test.py /data/twitter/decahose/2020/json/decahose.2020-05-25.p1.bz2.json
```

## References

- [Spark Examples](https://spark.apache.org/examples.html)
- [NumPy Tutorial](https://cs231n.github.io/python-numpy-tutorial/)
- [Basic Numerical Integration: the Trapezoid Rule](https://nbviewer.jupyter.org/github/ipython/ipython/blob/master/examples/IPython%20Kernel/Trapezoid%20Rule.ipynb)
- [Yarn Resource Manager](https://docs.anaconda.com/anaconda-cluster/howto/spark-yarn/)
