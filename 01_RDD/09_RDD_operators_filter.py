# @Time : 2022/3/27 12:04 
# @Author : Jface 
# @desc :
# @coding: utf8
"""

"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # env
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # source
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

    # transform
    result = rdd.filter(lambda x: x % 2 == 1)

    # sink
    print(result.collect())
