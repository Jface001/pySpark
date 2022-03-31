# @Time : 2022/3/31 23:11 
# @Author : Jface 
# @desc :
# @coding: utf8
"""

"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 2, 2, 2, 3, 3, 3])

    # distinct
    print(rdd.distinct().collect())

    rdd2 = sc.parallelize([('a', 1), ('a', 1), ('a', 3)])
    print(rdd2.distinct().collect())
