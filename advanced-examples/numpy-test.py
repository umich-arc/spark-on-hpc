# An applicaton to test that NumPy is available and working on the Yarn cluster.
# The app creates a parallelized RDD of a series of integers. It then runs NumPy 
# mod() on each integer.
#
# Code from https://docs.anaconda.com/anaconda-cluster/howto/spark-yarn/

from pyspark import SparkConf
from pyspark import SparkContext
import time

conf = SparkConf()
conf.setAppName('NumPyTest')
sc = SparkContext(conf=conf)


def mod(x):
    import numpy as np
    return (x, np.mod(x, 2))

# Create an RDD of series of integers with 20 partitions
rdd = sc.parallelize(range(10000000), 20)
print("Created RDD of {0} partitions.".format(rdd.getNumPartitions()))

# Apply the mod function to each integer and get max
start = time.time()
rdd.map(mod).max()
end = time.time()
print("############################################################################")
print("Python Version {0}".format(sc.pythonVer))
import numpy as np
print("NumPy Version {0}".format(np.__version__))
print("Applied NumPy mod. It took {0} seconds.".format(end - start))
print("RDD has {0} partitions.".format(rdd.getNumPartitions()))
print("NumPy works!")
print("############################################################################")
