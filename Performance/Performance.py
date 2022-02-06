from pyspark.sql import SparkSession
import os
import glob
import re
import sys
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
from timeit import default_timer as timer
from tvbase import setupLogger, setupLogDir, printEndBanner, printBeginBanner
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext


def get_spark_session(appName, conf):
    return SparkSession.builder.master("local[1]") \
        .appName(appName) \
        .config(conf=conf) \
        .getOrCreate()


def main():
    try:
        global LOGBASE, LOGDIR, APPNAME
        APPNAME = "Yellow Taxi"
        LOGBASE = "/home/hadoop/work/logs"  # ARGS.logdir
        INPUT_DATADIR = "file://" + "/media/hadoop/SOFT/datasets/"  # ARGS.datadir
        OUTPUT_DATADIR = INPUT_DATADIR + "/output"  # ARGS.datadir
        # loaddate = datetime.fromtimestamp(time.time()).strftime('%Y%m%d')

        executorcores = 1
        numexecuters = 1
        executormemory = '2G'
        drivermemory = '2G'

        start = timer()
        start_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print("start_time: " + start_time)

        conf = SparkConf().setAll(
            [('spark.executor.memory', executormemory), ('spark.executor.cores', executorcores),
             ('spark.cores.max', numexecuters),
             ('spark.driver.memory', drivermemory)])
        spark = get_spark_session(APPNAME, conf)
        # spark.sparkContext.stop()
        # spark.conf.set("spark.sql.hive.convertMetastoreParquet",false)
        LOGDIR = setupLogDir(LOGBASE)
        LOGGER = setupLogger(LOGDIR, APPNAME)
        printBeginBanner("Start Performance job ")
        sparkDF = spark.read.options(header='True', inferSchema='True').csv(INPUT_DATADIR)
        # df.cache()
        sparkDF.printSchema()
        # sparkDF.columns
        # sparkDF.filter(col("vendor_name") == 'VTS')\
        sparkDF.groupBy("vendor_name").agg(count("*"), sum("Total_Amt")) \
            .show()
        end = timer()
        end_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print("start_time: " + start_time)
        print("end_time: " + end_time)

    except Exception as e:
        print(str(e))
        sys.exit()


if __name__ == "__main__":
    main()
