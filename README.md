# Frequent Itemsets on HDFS

Given a list of baskets uploaded to HDFS, calculate frequent itemsets with Hadoop MapReduce and Pyspark RDDs.

## Example Usage

Hadoop MapReduce.

```zsh
mapred streaming \
    --file /path/to/mapper.py \
    --mapper 'python3 mapper.py' \
    --file /path/to/reducer.py \
    --reducer 'python3 reducer.py' \
    --input /path/to/input/file \
    --output /path/to/output/dir
```

Pyspark RDDs.

```zsh
python3 hdfs_itemsets/spark/rdd.py hdfs://namenode_host:namenode_port /path/to/input/file /path/to/output/dir
```

Generate 1.000.000 baskets.

```zsh
python3 data/generate.py hdfs://namenode_host:namenode_port /path/to/output/dir
```

## References

[Running Hadoop On Ubuntu Linux (Multi-Node Cluster)](https://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/)
