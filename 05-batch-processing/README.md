# Module 5 - Batch Proceessing (PySpark) Study Notes

## Table of contents
- [Introduction to Spark](#introduction-to-spark)
- [Initializing Spark](#initializing-spark)
- [Spark DataFrame](#spark-dataframe)
- [Spark SQL](#spark-sql)
- [Spark Core](#spark-core)
- [Spark Submit](#spark-submit)
- [Internals of Spark](#internals-of-spark)

## Introduction to Spark
Spark is a an open-source, distributed computation engine that:

- **Distributes workloads:** Splits data across multiple machines / nodes, enabling parallel processing and faster analysis.
- **Handles diverse data:** Works with various data formats, from structured databases to unstructured text files.
- **Offers multiple languages:** Can be program with Scala, Java, Python, R, or SQL.
- **Supports multiple use cases:** Tackle batch processing, real-time streaming, machine learning, and more. 

In this module, we would be using PySpark, a Python API for Apache Spark. It enables you to perform real-time, large-scale data processing in a distributed environment using Python.
PySpark supports all of Spark’s features such as Spark SQL, DataFrames, Structured Streaming, Machine Learning (MLlib) and Spark Core.

[Back to top](#table-of-contents)

## Initializing Spark
The entry point into all functionality in Spark is the SparkSession class. We could use the code below to initialize a SparkSession. 
```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
```
Spark configuration can be set with [`SparkConf()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkConf.html). Various Spark parameters can be set with key-value pairs. 

[Back to top](#table-of-contents)

## Spark DataFrame
Spark dataframe is an immutable distributed collection of data conceptually equivalent to a table in a relational database or a Pandas DataFrame, but with richer optimizations under the hood. 
### Transformations vs Actions
Spark dataframe can be manipulated with untyped dataset operations (aka DataFrame Operations). There are two types of dataframe operations, namely transformations and actions. `Transformations are lazily evaluated`, which means no Spark jobs are triggered, no matter the number of transformations are scheduled. `Actions are executed in eager manner`, where all unevaluated transformations are executed prior to the action.

Some common transformations and actions are shown in the table below.
|  |  |
|-------------------|-------------------|
| Transformations |Select,  Filter, Distinct, Repartition, Joins, GroupBy, udf |
| Actions | Count, Show, Take, Head, Write |
### Narrow vs Wide Transformations
| Narrow Transformations | Wide Transformations |
|-------------------|-------------------|
| Each partition at the parent RDD is used by at most one partition of the child RDD | Each partition at the parent RDD is used by multiple partitions of the child RDD
| Fast | Slow
| Does not require any data shuffling over the cluster network | Might require data shuffling over the cluster network
| Example: filter, map | Example: join, repartition

[Back to top](#table-of-contents)

## Spark SQL
Spark SQL is Spark’s module for working with structured data. Spark SQL code tells Spark what to do in a declarative manner. Code written in Spark SQL benefits from Spark’s [catalyst](https://www.databricks.com/glossary/catalyst-optimizer), which optimizes the performance. Thus, using Spark SQL with the structured APIs is easier to write performant code.

> [!NOTE] 
> Some common Spark SQL syntax can be found [here](https://spark.apache.org/docs/2.3.0/api/sql/index.html).

[Back to top](#table-of-contents)

## Spark Core
Spark Core is the underlying general execution engine for the Spark platform that all other functionality is built on top of. It provides RDDs (Resilient Distributed Datasets) and in-memory computing capabilities.

### Resilient Distributed Datasets (RDD)
RDD is a distributed collection of elements that can be operated on in parallel. It is the following properties
- Resilient - Fault-tolerant with the help of RDD lineage graph [DAG] and so able to recompute missing or damaged partitions due to node failures.
- Lazy evaluated - Data inside RDD is not available or transformed until an action triggers the execution.
- Cacheablec - All the data can be hold in a persistent “storage” like memory (default and the most preferred) or disk (the least preferred due to access speed).
- Immutable or Read-Only - It does not change once created and can only be transformed using transformations to new RDDs.

[Back to top](#table-of-contents)

## Spark Submit
Spark Submit is a command-line tool provided by Apache Spark for submitting Spark applications to a cluster. It is used to launch applications on a standalone Spark cluster, a Hadoop YARN cluster, or a Mesos cluster.
### Deployment Mode
| 	| Cluster	| Client |
| ----- | ----------- | ----------- | 
| Driver |	Runs on one of the worker nodes | Runs locally from where you are submitting your application using spark-submit command |
| When to use | Production | Interactive and debugging purposes |

> [!NOTE] 
> Default deployment mode is client mode

> [!NOTE] 
> In client mode only the driver runs locally and all tasks run on cluster worker nodes.

### Cluster Manager

In Spark application, cluster manager does the resource allocating work. 

To run on a cluster, `SparkContext` connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos, YARN or Kubernetes). Once connected with cluster, `SparkContext` sends the application code (defined by JAR for JAVA and Scala or Python files passed to SparkContext) to the executors. Finally, `SparkContext` sends tasks to the executors to run.

![](https://spark.apache.org/docs/latest/img/cluster-overview.png)

| Cluster Manager	| --master | Description |
| ----- | ----------- | ----------- | 
| Yarn | yarn | Use yarn if cluster resources are managed by Hadoop Yarn
| Mesos | mesos://HOST:PORT | Use mesos://HOST:PORT for Mesos cluster manager
| Standalone | spark://HOST:PORT | Use spark://HOST:PORT for Standalone cluster
| Kubernetes | k8s://HOST:PORT or k8s://https://HOST:PORT | Use k8s://HOST:PORT for Kubernetes
| local | local or local[k] or local[k, F] | k = number of cores, which translate to number of workers, F = number of attempts it should run when failed

Refer this [page](https://sparkbyexamples.com/spark/spark-submit-command/) for more info on spark-submit.

[Back to top](#table-of-contents)

## Internals of Spark

### Groupby
A groupby operation in Spark's physical plan will go through [`HashAggregate`](https://www.pgmustard.com/docs/explain/hash-aggregate) -> `Exchange` -> `HashAggregate`. The operations is as follows. 
1. The first HashAggregate is responsible for doing partial aggregation where GroupBy operation is done locally on the data in each executor (One partition of data as each executor will only have one partition of data). 
2. Exchange represents shuffle, which is the process of exchanging data between partitions. Data rows can move between worker nodes when their source partition and the target partition reside on a different machine.
3. The second HashAggregate represents the final aggregation (final merge) after the shuffle.

#### Example
For the query below, the operation is as follows
```python
query = f"""
    SELECT 
        PULocationID AS zone, 
        date_trunc('HOUR', lpep_pickup_datetime) AS pickup_datetime,
        DECIMAL(SUM(total_amount)) AS revenue,
        COUNT(1) AS number_of_records
    FROM 
        green_taxi
    WHERE lpep_pickup_datetime > '2019-01-01 00:00:00'
    GROUP BY 
        zone, pickup_datetime
"""

spark.sql(query).show()
```

`Stage 1` - Filtering of lpep_pickpup_datetime. Filtering in Spark is done by [predicate pushdown](https://airbyte.com/data-engineering-resources/predicate-pushdown#:~:text=%E2%80%8DTL%3BDR%3A,of%20data%20transmitted%20and%20processed). Then, partial aggregation of local data in each executor is performed. 



<img src=./screenshots/spark_groupby_stage1.png width='300'>

`Exchange` - Shuffling of data

<img src=./screenshots/spark_groupby_exchange.png width='300'>

`Stage 2` - Final merge of data where data with same key is put into same partition. Then final aggregation of data (sum, count, etc) is done here. 

<img src=./screenshots/spark_groupby_stage2.png width='300'>

### Joins
Spark Join Strategies can be thought of in 2 phases, Data Exchange Strategies and Join Algorithms.

#### Data Exchange Strategies
- Broadcast
    - The driver will collect all the data partitions for table A, and broadcast the entire table A to all of the executors. This `avoids an all-to-all communication (shuffle) between the executors` which is expensive. The data partitions for table B do not need to move unless explicitly told to do so.
- Shuffle
    - All the executors will communicate with every other executor to share the data partition of table A and table B. All the records with the same join keys will then be in the same executors. As this is an all-to-all communication, this is `expensive as the network can become congested and involve I/O`. After the shuffle, each executor holds data partitions (with the same join key) of table A and table B.

Two most common join strategy in Spark is sort-merge join and broadcast hash join.

#### Sort-merge Join
Sort-merge join uses `Sort Merge Algorithm` and `shuffle` data exchange. The algorithm is done as follows: 
1. Sort all the data partitions accordingly to the Join Key
2. Perform merge operation based on the sorted Join Key.

Sort-merge join is useful for large tables. However, it might cause spilling to disk if there is not enough execution memory.

#### Broadcast Hash Join
Broadcast Hash join uses `Hash Join Algorithm` and `broadcast` data exchange. The algorithm is done as follows: 
1. A Hash Table is built on the smaller table of the join. The Join Column value is used as the key in the Hash Table
2. For every row in the other data partitions, the Join Column value is used to query the Hash Table built in (1). When Hash Table returns some value, then there is a match.

Broadcast Hash join is useful if one of the table is able to fit into executor memory. The default broadcast size is 10mb. Broadcast Hash is the preferred way of joining tables as shuffling is not required. One disadvantage is that it doeese not support FULL OUTER JOIN.

[Back to top](#table-of-contents)
## Reference
1. [PySpark Overview](https://spark.apache.org/docs/latest/api/python/index.html)
2. [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds)
3. [RDD: Spark’s Fault Tolerant In-Memory weapon](https://medium.com/knoldus/rdd-sparks-fault-tolerant-in-memory-weapon-130f8df2f996)
4. [A Tale of Three Apache Spark APIs: RDDs vs DataFrames and Datasets](https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)
5. [Spark Transformation and Action: A Deep Dive](https://medium.com/codex/spark-transformation-and-action-a-deep-dive-f351bce88086)
6. [The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/overview/)
7. [Different Types of Spark Join Strategies](https://medium.com/@ongchengjie/different-types-of-spark-join-strategies-997671fbf6b0)

[Back to top](#table-of-contents)