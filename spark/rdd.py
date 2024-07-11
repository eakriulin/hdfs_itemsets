import sys
import time
from pyspark.sql import SparkSession
from itertools import combinations
from operator import add

if len(sys.argv) != 3:
    sys.exit('Provide paths input file and output dir')

input_filepath = sys.argv[1]
output_dir = sys.argv[2]

spark: SparkSession = SparkSession.builder.appName('frequent_itemsets').getOrCreate()

SUPPORT_THRESHOLD = 2

def mapper(line: str) -> list[tuple[str, int]]:
    items = line.split(',')
    return [(','.join(triplet), 1) for triplet in combinations(items, 3)]

def filter(keyValue: tuple[str, int]) -> bool:
    return keyValue[1] >= SUPPORT_THRESHOLD

rdd = spark.sparkContext.textFile(input_filepath)
result = rdd.flatMap(mapper).reduceByKey(add).filter(filter)
result.saveAsTextFile(output_dir)
