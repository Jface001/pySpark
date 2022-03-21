# @Time : 2022/3/21 22:21 
# @Author : Jface 
# @desc :
# @coding: utf8
"""
source from TextFile
"""
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMatser("local[*]")
    sc = SparkContext(conf=conf)

    # read TextFile
    rdd = sc.wholeTextFiles("../data/input/tiny_files")
    print(rdd.map(lambda x: x[1]).collect())
