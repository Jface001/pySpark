# @Time : 2022/3/24 21:45 
# @Author : Jface 
# @desc :
# @coding: utf8
"""

"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 1.env
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 2.source
    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)])

    # 3.transform & sink
    # reduceByKey 对相同key 的数据执行聚合相加
    print(rdd.reduceByKey(lambda a, b: a + b).collect())
