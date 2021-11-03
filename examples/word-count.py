# Do a word count on Complete Works of Shakespeare

import sys
import time

from pyspark import SparkContext

sc = SparkContext(appName = "WordCountExample")

# Create an RDD in PySpark from large text file in HDFS
start = time.time()
input_text_file = str(sys.argv[1]) if len(sys.argv) > 1 else "/data/Gutenberg.txt"
rdd = sc.textFile(input_text_file)

# Create function to make it all lower-case and split the lines into words,
# creating a new RDD with each element being a word.
def Func(lines):
    lines = lines.lower()
    lines = lines.split()
    return lines

rdd_flat  = rdd.flatMap(Func)

# Do a word count using a map-reduce like function.
# Map each word with a count of 1 like a key-value pair where the value is 1.
rdd_mapped = rdd_flat.map(lambda x: (x,1))
# Then group each count by key.
rdd_grouped = rdd_mapped.groupByKey()
# Take the sum of each word, then swap the key value pair order,
# then sort by value instead of key.
rdd_frequency = rdd_grouped.mapValues(sum).map(lambda x: (x[1],x[0])).sortByKey(False)
# Get the 10 most frequent words.
top_ten = rdd_frequency.take(10)
end = time.time()

print("############################################################################")
print("Top 10 most frequent words were found with Spark in {} seconds.".format(round(end - start, 2)))
print("Most frequent words: {}".format(top_ten))
print("############################################################################")
