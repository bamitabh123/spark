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
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def setupLogDir():
    print("Setting Up LogDir for logging ...")
    timestamp = datetime.fromtimestamp(time.time()).strftime('%Y%m%d')
    logdir = os.path.join(os.path.abspath(os.sep), LOGBASE, timestamp)
    if not os.path.exists(logdir):
        os.makedirs(logdir)
    return logdir


def get_spark_session():
    return SparkSession.builder.master("local[1]") \
        .appName('get_sample_parquet_data') \
        .getOrCreate()


def rename_parquet_file(OUTPUT_DATADIR, loaddate):
    myStatus = 0
    count = 0
    outdir = OUTPUT_DATADIR + "//" + loaddate
    filename_pattern = outdir + "//*.parquet"

    for file in os.listdir(outdir):
        if file.endswith(".txt"):
            count += 1
            print("Tag : Rename filename " + file)
            filename = file
            if count >= 2:
                raise Exception("More than one file found at [%s] path" % outdir)

    try:
        os.rename(outdir + "//" + filename, outdir + "//" + "SalesByCity.txt")
    except Exception as e:
        print("Process encountered an error [%s]! Existing..." % e)
        myStatus = 1

    return myStatus


def main():
    try:
        global LOGBASE, LOGDIR
        LOGBASE = "D://Saurabh//logs"  # ARGS.logdir
        INPUT_DATADIR = "D://Saurabh//datasets"  # ARGS.datadir
        OUTPUT_DATADIR = INPUT_DATADIR + "//output"  # ARGS.datadir
        LOGDIR = setupLogDir()
        loaddate = datetime.fromtimestamp(time.time()).strftime('%Y%m%d')
        executorcores = 1
        numexecuters = 1
        executormemory = '2G'
        drivermemory = '2G'

        start = timer()
        start_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print("start_time: " + start_time)

        spark = get_spark_session()
        # spark.conf.set("spark.sql.hive.convertMetastoreParquet",false)
        # spark.conf.set("spark.executer.cores",executorcores)
        # spark.conf.set("spark.executer.instances",numexecuters)
        # spark.conf.set("spark.executer.memory",executormemory)
        # spark.conf.set("spark.driver.memory",drivermemory)

        parquet_exists = 0

        for file in glob.glob(INPUT_DATADIR + "//SalesByCity*.parquet"):
            parquet_exists = 1
            print(file + " Exist..")
            break

        print("Tag : INPUT_DATADIR:" + INPUT_DATADIR + "//SalesByCity*.parquet")
        print("Tag : OUTPUT_DATADIR: " + OUTPUT_DATADIR)
        if parquet_exists == 1:
            try:
                df_distValue = spark.read.parquet(INPUT_DATADIR + "//SalesByCity*.parquet").withColumn("year1",
                                                                                                       spark_partition_id()).select(
                    concat_ws('|', "city", "year")).distinct().sort(col("city"), col("year").asc())
                df_distValue.show(5)
                df_distValue.coalesce(1).write.mode("overwrite").text(OUTPUT_DATADIR + "//" + loaddate)
                myStatus = rename_parquet_file(OUTPUT_DATADIR, loaddate)
            except Exception as e:
                print("Process encountered an error [%s]! Existing..." % e)
                return 1

        # filename_re = 'FILE\d+TEST.txt'
        # for filename in os.listdir(DATADIR):
        #     if re.search(filename_re, filename):

        end = timer()
        end_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print("start_time: " + start_time)
        print("end_time: " + end_time)

    except Exception as e:
        print(str(e))
        sys.exit()


if __name__ == "__main__":
    main()
