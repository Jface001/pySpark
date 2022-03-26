# @Time : 2022/3/26 15:54 
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
    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 2), ('b', 3)])

    # transform groupBy
    # groupBy传入的函数的 意思是: 通过这个函数, 确定按照谁来分组(返回谁即可)
    # 分组规则 和SQL是一致的, 也就是相同的在一个组(Hash分组)
    result = rdd.groupBy(lambda t: t[0])
    print(result.map(lambda t: (t[0], list(t[1]))).collect())
