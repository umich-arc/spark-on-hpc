# Example Python Program with NLTK
# https://docs.anaconda.com/anaconda-scale/howto/spark-nltk/

import os
import sys
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
          .appName("nltk-test") \
          .getOrCreate()

#partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 24
partitions = 24
nltk_data_location = "/nfs/turbo/arcts-data-hadoop-stage/data/nltk_data/"

start = time.time()
data_location = str(sys.argv[1]) if len(sys.argv) > 1 else '/data/complete-works-of-shakespeare.txt'
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

print("Completed dataset tokenization and part of speech tagging of first 20 elements in {0} seconds."
      .format(round(end - start, 2)))
