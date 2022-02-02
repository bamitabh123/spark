# Create a Spark Object
from pyspark.sql import SparkSession
import os

os.environ['SPARK_HOME'] = '/opt/spark3'
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName("Python Word Count") \
    .getOrCreate()

text_file = spark.sparkContext.textFile('practice/test')  # hdfs path
wordCounts = text_file.flatMap(lambda line: line.split(" ")) \
    .filter(lambda x: x.isdigit() == False) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
top3Words = wordCounts.takeOrdered(10, lambda k: -float(k[1]))
print(wordCounts.filter(lambda x: x == "Harry"))

print("Word Count for word Harry : " + result["Harry"])

# for i in rdd.toDebugString().split(“\n”) : print(i) #Displays Logical Execution Plan.

spark.stop()
