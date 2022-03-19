# @Time : 2022/3/19 14:32 
# @Author : Jface 
# @desc :
# coding:utf8
"""
pySpark parallelize
"""
# load package
from pyspark import SparkConf, SparkContext

if __name__ = '__main__'
    # 1.env
    conf = SparkConf.setAppName("test").setMatser("local[*]")
    sc = SparkContext = (conf=conf)

    # 2.source
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print("默认分区数：", rdd.getNumPartitions())

    rdd = sc.parallelize([1, 2, 3], 3)
    print("分区数：", rdd.getNumPartitions())

    # collect方法, 是将RDD(分布式对象)中每个分区的数据, 都发送到Driver中, 形成一个Python List对象
    # collect: 分布式 转 -> 本地集合
    print("rdd的内容是: ", rdd.collect())