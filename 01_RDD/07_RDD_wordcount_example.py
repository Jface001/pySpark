# @Time : 2022/3/25 20:47 
# @Author : Jface 
# @desc :
# @coding: utf8
"""
wordcount example
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 1.env
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 2.source
    file_rdd = sc.textFile("../data/input/words.txt")

    # 3.transform
    word_rdd = file_rdd.flatMap(lambda x: x.split(" "))

    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    reuslt_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # sink
    print(reuslt_rdd.collect())
