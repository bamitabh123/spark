#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A simple example demonstrating Spark SQL data sources.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/datasource.py
"""
from __future__ import print_function

from pyspark.sql import SparkSession
# $example on:schema_merging$
from pyspark.sql import Row
# $example off:schema_merging$


def parquet_example(spark):
    # $example on:basic_parquet_example$
    peopleDF = spark.read.json("../resources/people.json")

    # DataFrames can be saved as Parquet files, maintaining the schema information.
    peopleDF.write.parquet("people.parquet")

    # Read in the Parquet file created above.
    # Parquet files are self-describing so the schema is preserved.
    # The result of loading a parquet file is also a DataFrame.
    parquetFile = spark.read.parquet("people.parquet")

    # Parquet files can also be used to create a temporary view and then used in SQL statements.
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.show()
    # +------+
    # |  name|
    # +------+
    # |Justin|
    # +------+
    # $example off:basic_parquet_example$



def jdbc_dataset_example(spark):
    # $example on:jdbc_dataset$
    # Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    # Loading data from a JDBC source
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .load()

    jdbcDF2 = spark.read \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})

    # Specifying dataframe column data types on read
    jdbcDF3 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .option("customSchema", "id DECIMAL(38, 0), name STRING") \
        .load()

    # Saving data to a JDBC source
    jdbcDF.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .save()

    jdbcDF2.write \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})

    # Specifying create table column data types on write
    jdbcDF.write \
        .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)") \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})
    # $example off:jdbc_dataset$


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    # basic_datasource_example(spark)
    parquet_example(spark)
    # parquet_schema_merging_example(spark)
    # json_dataset_example(spark)
    #jdbc_dataset_example(spark)

    spark.stop()
