# Example Python Program with NLTK
# https://docs.anaconda.com/anaconda-scale/howto/spark-nltk/

import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
          .appName("nltk-test") \
          .getOrCreate()

partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

#data = spark.sparkContext.textFile('/data/Gutenberg.txt',partitions)
data = spark.sparkContext.textFile('/data/complete-works-of-shakespeare.txt',partitions)

def word_tokenize(x):
    import nltk
    return nltk.word_tokenize(x)

def pos_tag(x):
    import nltk
    return nltk.pos_tag([x])

# Tokenize
words = data.flatMap(word_tokenize)
words.saveAsTextFile('nltk_test_tokens')

# Label parts of speech
pos_word = words.map(pos_tag)
pos_word.saveAsTextFile('nltk_test_token_pos')
