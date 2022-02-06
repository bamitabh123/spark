# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('sample.txt')
print('Original RDD')
print(rdd.collect())

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
print('Map with lambda')
print(rdd2.collect())


def foo(x):
    l = x.split()
    l2 = []
    for s in l:
        l2.append(int(s) + 10)
    return l2


rdd3 = rdd.map(foo)
print('Map with function')
print(rdd3.collect())
#
# # COMMAND ----------
