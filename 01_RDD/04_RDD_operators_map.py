# @Time : 2022/3/22 20:52 
# @Author : Jface 
# @desc :
# @coding: utf8
"""

"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 1.env
    conf = SparkConf().setAppName("test").setMatser("local[*]")
    sc = SparkContext(conf=conf)

    # 2.source
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # 3. transform
    def add(data):
        return data * 10


    print(rdd.map(add).collect())

    # lambda
    print(rdd.map(lambda data: data * 10).collect())
