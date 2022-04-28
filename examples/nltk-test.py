# Example Python Program with NLTK
# https://docs.anaconda.com/anaconda-scale/howto/spark-nltk/

import os
import sys
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
          .appName("nltk-test") \
          .getOrCreate()

# Take clustername and return input data file path.
def setInputFile(cluster):
    location = {
        "greatlakes": "/nfs/turbo/arcts-data-hadoop-stage/data/complete-works-of-shakespeare.txt",
        "thunderx": "/data/complete-works-of-shakespeare.txt"
    }
    return location.get(cluster, "/data/complete-works-of-shakespeare.txt")

#partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 24
partitions = 12
nltk_data_location = "/nfs/turbo/arcts-data-hadoop-stage/data/nltk_data/"

start = time.time()

if len(sys.argv) > 1:
    data_location = str(sys.argv[1])
else:
    if "CLUSTER_NAME" in os.environ:
        data_location = setInputFile(os.environ['CLUSTER_NAME'])
    else:
        print("Cannot identify HPC cluster name because CLUSTER_NAME env var \
            is missing. Try passing the path to the input files as an argument. \
            Will assume we are running on Cavium ThunderX.")
        data_location = setInputFile("thunderx")

data = spark.sparkContext.textFile(data_location, partitions)

def word_tokenize(x):
    import nltk
    nltk.data.path.append(nltk_data_location)
    return nltk.word_tokenize(x)

def pos_tag(x):
    import nltk
    nltk.data.path.append(nltk_data_location)
    return nltk.pos_tag([x])

# Tokenize
words = data.flatMap(word_tokenize)

# Label parts of speech
pos_word = words.map(pos_tag)
print(pos_word.take(20))
end = time.time()

print("############################################################################")
print("NLTK works. Completed dataset tokenization and part of speech tagging of first 20 elements in {0} seconds."
      .format(round(end - start, 2)))
print("############################################################################")
