# Find Number of blocks in a file using fsck command
# hadoop fsck <file path>

from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
import os
os.environ['SPARK_HOME'] = '/opt/spark3'
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# Practice 1 --Create RDD using textFile API
rdd = spark.sparkContext.textFile('practice/test')
rdd.take(5)
for i in rdd.take(5):
    print(i)
# Get the Number of Partitions in the RDD
rdd.getNumPartitions()
# Get the Number of elements in each partition
rdd.glom().map(len).collect()

# Practice 2 --Create RDD using textFile API and a defined number of partitions
rdd = spark.sparkContext.textFile('practice/test', 10)
# Get the Number of Partitions in the RDD
rdd.getNumPartitions()
# Get the Number of elements in each partition
rdd.glom().map(len).collect()

# Practice -3 --Create a RDD from a Python List
lst = [1, 2, 3, 4, 5, 6, 7]
rdd = spark.sparkContext.parallelize(lst)
for i in rdd.take(5):
    print(i)

# Practice -4 --Create a RDD from local file
lst = open('/home/hadoop/git/spark/Complete_PySpark_Developer/data/Harry-potter.txt').read().splitlines()
lst[0:10]
rdd = spark.sparkContext.parallelize(lst)
for i in rdd.take(5):
    print(i)

# Practice -5 --Create RDD from range function
lst1 = range(10)
rdd = spark.sparkContext.parallelize(lst1)
for i in rdd.take(5):
    print(i)

# Practice -6 --Create RDD from a DataFrame
df = spark.createDataFrame(data=(('robert', 35), ('Mike', 45)), schema=('name', 'age'))
df.printSchema()
df.show()
rdd1 = df.rdd
type(rdd1)
for i in rdd1.take(2):
    print(i)
