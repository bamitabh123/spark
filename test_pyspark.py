# Create a Spark Object
import os

from pyspark.sql import SparkSession

os.environ['SPARK_HOME'] = '/opt/spark3'
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

print("Spark Object id created ...")
spark.stop()
