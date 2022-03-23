# @Time : 2022/3/23 21:58 
# @Author : Jface 
# @desc :
# @coding: utf8
"""
flatMap
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 1.env
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 2.source
    rdd = sc.parallelize(["hadoop spark hadoop", "spark hadoop hadoop", "hadoop flink spark"])

    # 3.transform
    rdd2 = rdd.flatmap(lambda line: line.split(" "))

    # 4.sink
    print(rdd2.collect())
