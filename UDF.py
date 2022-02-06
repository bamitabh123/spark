from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .master("local") \
    .appName("Data Frame UDF") \
    .getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv("OfficeData.csv")
df.printSchema()


# df.show(5, False)


def Total_Sal(salary, bonus):
    return salary + bonus


TotalSalUDF = udf(lambda x, y: Total_Sal(x, y), IntegerType())

df2 = df.withColumn("TotalSalary", TotalSalUDF(col("salary"), col("bonus")))
df2.printSchema()
df2.show(5, False)
