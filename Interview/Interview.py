from pyspark.sql.functions import split, col, explode, count

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName(" Word Count RDD ")
sc = SparkContext.getOrCreate(conf=conf)

text = sc.textFile('word_cnt.txt') # return RDD object
print(type(text))
print(text.collect())

rdd2 = text.flatMap(lambda x: x.split(' '))
print(rdd2.collect())

rdd3 = rdd2.map(lambda x: (x, 1))
print(rdd3.collect())

rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
print(rdd4.collect())

# all operation in 1 line
print("Word Count in 1 line ")

counts = text.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y)

print(counts.collect())

print("Word Count DataFrame")
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName('Word Count RDD') \
    .getOrCreate()

sc = spark.sparkContext
df = spark.read.text("word_cnt.txt") # return DataFrame object
print(type(df))
df.show(5, truncate=False)

dfwords = df.withColumn('words', split(col('value'), ' ')) \
    .withColumn('word', explode(col('words'))) \
    .drop('value', 'words') \
    .groupby('word') \
    .agg(count('word').alias('count')) \
    .orderBy('count', ascending=False) \
    .show(10, truncate=False)

print("Word Count DataFrame Spark SQL")
sqllines = df.createOrReplaceTempView('lines')

spark.sql("""select word, count(word) count from
 (select explode(split(value,' '))  word from lines) words group by word order by count desc""").show(10)


# how to join 1 GB csv file to 2 GB of parquet file  with 2 GB of spark memory 
#https://stackoverflow.com/questions/46638901/how-spark-read-a-large-file-petabyte-when-file-can-not-be-fit-in-sparks-main
#https://stackoverflow.com/questions/63061208/what-happens-when-the-file-size-is-greater-than-the-cluster-memory-size-in-spark