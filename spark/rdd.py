import sys
import time
from pyspark.sql import SparkSession
from itertools import combinations
from operator import add

if len(sys.argv) != 2:
    sys.exit('Provide path input file')

spark: SparkSession = SparkSession.builder.appName('frequent_itemsets').getOrCreate()

INPUT_FILEPATH = sys.argv[1]
OUTPUT_DIR = f'./results/frequent_itemsets/rdd/{int(time.time())}'
SUPPORT_THRESHOLD = 2

def mapper(line: str) -> list[tuple[str, int]]:
    items = line.split(',')
    return [(','.join(triplet), 1) for triplet in combinations(items, 3)]

def filter(keyValue: tuple[str, int]) -> bool:
    return keyValue[1] >= SUPPORT_THRESHOLD

rdd = spark.sparkContext.textFile(INPUT_FILEPATH)
result = rdd.flatMap(mapper).reduceByKey(add).filter(filter)
result.saveAsTextFile(OUTPUT_DIR)
