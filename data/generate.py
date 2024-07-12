import sys
from pyspark.sql import SparkSession
import random

if len(sys.argv) != 3:
    sys.exit('Provide HDFS URI and output dir')

hdfs_uri = sys.argv[1]
hdfs_path = sys.argv[2]

spark: SparkSession = SparkSession.builder \
    .appName('data_generator') \
    .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
    .getOrCreate()

items = [
    'milk', 'bread', 'eggs', 'butter', 'jam', 'cheese', 'apple', 'banana', 'chocolate', 'coffee',
    'tea', 'sugar', 'flour', 'rice', 'pasta', 'tomato', 'potato', 'onion', 'garlic', 'carrot',
    'cabbage', 'lettuce', 'orange', 'grape', 'yogurt', 'cream', 'honey', 'beef', 'chicken', 'pork',
    'mushroom', 'pepper', 'salt', 'vinegar', 'oil', 'buttermilk',
    'ketchup', 'mustard', 'mayonnaise', 'cereal', 'oatmeal', 'corn', 'peas', 'beans', 'lentils', 'broccoli',
    'cauliflower', 'spinach', 'kale', 'zucchini', 'cucumber', 'bell pepper', 'avocado', 'blueberries', 'raspberries',
    'water', 'soda', 'juice', 'wine', 'beer', 'whiskey', 'vodka', 'rum', 'gin', 'tonic', 'lemonade', 'smoothie'
]

def generate_basket(items=items, min_items=1, max_items=10):
    num_items = random.randint(min_items, max_items)
    return random.sample(items, num_items)

num_of_iterations = 100
num_of_baskets_per_iteration = 10000
baskets_rdd = spark.sparkContext.emptyRDD()

for _ in range(num_of_iterations):
    new_baskets = [','.join(generate_basket(items)) for _ in range(num_of_baskets_per_iteration)]
    new_baskets_rdd = spark.sparkContext.parallelize(new_baskets, numSlices=1)
    baskets_rdd = baskets_rdd.union(new_baskets_rdd)

baskets_rdd.repartition(1).saveAsTextFile(hdfs_path)

print(f'File written to HDFS at {hdfs_path}/part-00000')