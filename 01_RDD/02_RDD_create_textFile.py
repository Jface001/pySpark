# @Time : 2022/3/20 12:18 
# @Author : Jface 
# @desc :
# coding: utf8
"""

"""
from pyspark import SparkConf, SparkContext

if __name == '__main__':
    # 1. env
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 2. source
    file_rdd1 = sc.textFile("../data/import/words.txt")
    print("默认分区数：", file_rdd1.getNumPartitions())
    print("file_rdd1 内容：", file_rdd1.collect())

    # 加最小分区数参数的测试
    file_rdd2 = sc.textFile("../data/input/words.txt", 3)
    # 最小分区数是参考值, Spark有自己的判断, 你给的太大Spark不会理会
    file_rdd3 = sc.textFile("../data/input/words.txt", 100)
    print("file_rdd2 分区数:", file_rdd2.getNumPartitions())
    print("file_rdd3 分区数:", file_rdd3.getNumPartitions())

    # 读取HDFS文件数据测试
    hdfs_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")
    print("hdfs_rdd 内容:", hdfs_rdd.collect())
