import sys
from pyspark.sql import SparkSession
from itertools import combinations
from operator import add

if len(sys.argv) != 4:
    sys.exit('Provide HDFS URI, input filepath and output dir')

hdfs_uri = sys.argv[1]
input_filepath = sys.argv[2]
output_dir = sys.argv[3]

spark: SparkSession = SparkSession.builder \
    .appName('frequent_itemsets') \
    .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
    .getOrCreate()

SUPPORT_THRESHOLD = 10000

def mapper(line: str) -> list[tuple[str, int]]:
    items = line.split(',')
    return [(','.join(triplet), 1) for triplet in combinations(items, 3)]

def filter(keyValue: tuple[str, int]) -> bool:
    return keyValue[1] >= SUPPORT_THRESHOLD

rdd = spark.sparkContext.textFile(input_filepath)
result = rdd.flatMap(mapper).reduceByKey(add).filter(filter)
result.saveAsTextFile(output_dir)
