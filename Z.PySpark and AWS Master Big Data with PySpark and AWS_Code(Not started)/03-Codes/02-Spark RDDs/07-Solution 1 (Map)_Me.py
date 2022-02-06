# read the file in RDD
# Write a mapper that will provide length of each word in following format
# Hi how are you -- [2,3,3,3]

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Map Quiz")

sc = SparkContext.getOrCreate(conf=conf)

text = sc.textFile('Quiz_Sample.txt')

print(text.collect())


def quiz(str):
    words = str.split(' ')
    cnt = []
    for word in words:
        cnt.append(len(word))
    return cnt


rdd1 = text.map(quiz)
print(rdd1.collect())

print("using lambda")

rdd2 = text.map(lambda x: [len(s) for s in x.split(' ')])

print(rdd2.collect())

print("Flat map using lambda")

rdd2 = text.flatMap(lambda x: [len(s) for s in x.split(' ')])

print(rdd2.collect())