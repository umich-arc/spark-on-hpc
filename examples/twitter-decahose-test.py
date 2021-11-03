# An applicaton to test that a subset of the twitter decahose data is available. 
# Note that this test is not exhaustive and only checks for one of the many decahose 
# files that should be in HDFS. The app reads a 130GB json file into a DataFrame 
# and does a few simple operations.

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
          .appName("twitter-decahose-test") \
          .getOrCreate()

decahosePartition = str(sys.argv[1]) if len(sys.argv) > 1 else "/data/twitter/decahose/2020/json/decahose.2020-05-25.p1.bz2.json"

# The 130GB JSON file takes about 6 minutes to read.
df1 = spark.read.json(decahosePartition)

# Display the columns.
df1.columns

# Display the first tweet.
df1.first()

# Group tweets by possibly sensitive and count the tweets in each group. Takes about 2 minutes.
df1.groupBy("possibly_sensitive").count().show()
print("############################################################################")
print("Twitter decahose dataset is available!")
print("############################################################################")
