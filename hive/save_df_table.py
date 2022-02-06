from pyspark import SparkConf
from pyspark.sql import SparkSession

executorcores = 1
numexecuters = 1
executormemory = '1G'
drivermemory = '1G'

warehouseLocation = "hdfs:///localhost:9000/user/hive/warehouse/"

conf = SparkConf().setAll(
    [('spark.executor.memory', executormemory), ('spark.executor.cores', executorcores),
     ('spark.cores.max', numexecuters),
     ('spark.driver.memory', drivermemory)])
spark = SparkSession.builder.appName("Mubi movie dataset") \
    .config("spark.sql.warehouse.dir", warehouseLocation) \
    .enableHiveSupport() \
    .config(conf=conf).getOrCreate()

lists_data = spark.read.options(header='True', inferSchema='True').csv(
    "file:////media/hadoop/SOFT/datasets/Movies/MUBI/mubi_lists_data.csv")
lists_user_data = spark.read.options(header='True', inferSchema='True').csv(
    "file:////media/hadoop/SOFT/datasets/Movies/MUBI/mubi_lists_user_data.csv")
movie_data = spark.read.options(header='True', inferSchema='True').csv(
    "file:////media/hadoop/SOFT/datasets/Movies/MUBI/mubi_movie_data.csv")
ratings_data = spark.read.options(header='True', inferSchema='True', quote="\"").csv(
    "file:////media/hadoop/SOFT/datasets/Movies/MUBI/mubi_ratings_data.csv")
ratings_user_data = spark.read.options(header='True', inferSchema='True').csv(
    "file:////media/hadoop/SOFT/datasets/Movies/MUBI/mubi_ratings_user_data.csv")
lists_data.printSchema()
lists_user_data.printSchema()
movie_data.printSchema()
ratings_data.printSchema()
ratings_user_data.printSchema()

lists_data.count()  # 361,297
lists_user_data.count()  # 80,311
movie_data.count()  # 226,575
ratings_data.count()  # 15,584,184
ratings_user_data.count()  # 4,297,641

lists_user_data.write.mode("overwrite").format("parquet").saveAsTable("lists_user_data")
lists_data.write.mode("overwrite").format("parquet").saveAsTable("lists_data")

movie_data.write.mode("overwrite").format("parquet").saveAsTable("movie_data")
ratings_data.write.mode("overwrite").format("parquet").saveAsTable("ratings_data")

ratings_user_data.write.mode("overwrite").format("parquet").saveAsTable("ratings_user_data")


spark.sql("select count(*) from lists_user_data").show()
spark.sql("select count(*) from lists_data").show()

spark.sql("select count(*) from movie_data").show()
spark.sql("select count(*) from ratings_data").show()
spark.sql("select count(*) from ratings_user_data").show()
