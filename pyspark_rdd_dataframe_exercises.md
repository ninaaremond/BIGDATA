# PySpark RDD and DataFrame Exercises

This document covers four exercises involving PySpark RDD aggregations, joins, DataFrame operations, execution plans, and caching performance.

## 1. Calculate Order Statistics per User with `aggregateByKey`

### Objective

Rewrite `user_quantity` using `aggregateByKey` to calculate, in a single aggregation:

- The number of orders per user (`order_count`)
- The total quantity ordered (`total_quantity`)

Then calculate the average quantity per order for each user.

### Code

```python
user_stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),
        lambda acc, q: (acc[0] + 1, acc[1] + q),
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )
)

user_stats_with_average = user_stats.mapValues(
    lambda stats: (
        stats[0],
        stats[1],
        stats[1] / stats[0] if stats[0] else 0
    )
)

user_stats_with_average.take(5)
```

### Example output

```text
[
    ('f143262f-dc5c-4eed-8da0-365bf89897b9', (4, 12, 3.0)),
    ('969b6662-0562-4059-968c-c69b1064005c', (12, 40, 3.3333333333333335)),
    ('ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec', (85, 253, 2.976470588235294)),
    ('5563f616-00e8-4ece-8b49-452d46d483f3', (21, 57, 2.7142857142857144)),
    ('f94d6204-6808-493f-9fed-2c43e256a6dc', (49, 175, 3.5714285714285716))
]
```

### Explanation

Each result has the following structure:

```text
(user_id, (order_count, total_quantity, average_quantity))
```

For example, user `f143262f-dc5c-4eed-8da0-365bf89897b9` has:

- **4 orders**
- **12 units ordered in total**
- **3 units per order on average** (`12 / 4 = 3.0`)

`aggregateByKey` uses two functions:

1. The **sequence function** updates the accumulator for each value within a partition.
2. The **combiner function** merges the accumulators from different partitions.

The initial accumulator `(0, 0)` represents `(order_count, total_quantity)`. `mapValues` then adds the average to each user's statistics.

---

## 2. Find Orders with No Matching User Using `leftOuterJoin`

### Objective

Add a fake `user_id` to a copy of an order. Ensure this user does not exist in `users_kv`, perform a `leftOuterJoin`, filter for orders without a matching user, and count them.

### Code

```python
fake_order = orders_rdd.first().copy()
fake_order["user_id"] = "user-inexistant"

orders_with_fake_user = orders_rdd.map(
    lambda o: (o["user_id"], o)
).union(
    sc.parallelize([(fake_order["user_id"], fake_order)])
)

left_joined = orders_with_fake_user.leftOuterJoin(users_kv)

missing_users = left_joined.filter(
    lambda item: item[1][1] is None
)

print("Commandes sans utilisateur :", missing_users.count())
print(missing_users.take(1))
```

### Example output

```text
Commandes sans utilisateur : 1

[
    (
        'user-inexistant',
        (
            {
                'order_id': '2339ba19-2563-4cc3-8a97-ebf555d596af',
                'user_id': 'user-inexistant',
                'date': '2020-01-01 00:02:17.891488+00:00',
                'quantity': 4,
                'product': 'drink'
            },
            None
        )
    )
]
```

### Explanation

`leftOuterJoin` preserves every record from the left RDD, even if no matching key exists in the right RDD.

The joined value has this structure:

```text
(user_id, (order, user))
```

When a user is found, the `user` value contains the matching record. When no user is found, it is `None`.

The filter:

```python
lambda item: item[1][1] is None
```

selects records whose right-hand value—the user—is missing.

The result reports **one order without a matching user**, which is the fake order added for this exercise.

---

## 3. Reimplement `user_quantity` with the DataFrame API

### Objective

Reimplement the RDD aggregation using the DataFrame API:

```python
groupBy("user_id").sum("quantity")
```

Compare the number of lines of code, verify that both versions produce the same number of user groups, and inspect their execution plans using `.explain()` and `.toDebugString()`.

### DataFrame implementation

```python
orders_df = spark.createDataFrame(orders_rdd)

user_quantity_df = (
    orders_df
    .groupBy("user_id")
    .sum("quantity")
)

user_quantity_df.show(5, truncate=False)
```

### Example output

```text
+------------------------------------+-------------+
|user_id                             |sum(quantity)|
+------------------------------------+-------------+
|f53d4330-cdda-44ba-ad06-e8cf3805f907|250          |
|1190f938-a66f-47f7-b966-9fa759970043|92           |
|490617f2-747b-4dba-88fe-3ccdc8b8d9c6|43           |
|8a476a87-e49d-481d-91d8-7c6455fa1ab8|223          |
|ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec|253          |
+------------------------------------+-------------+
only showing top 5 rows
```

### Compare the number of user groups

```python
print("Nombre d'utilisateurs — RDD :", user_quantity.count())
print("Nombre d'utilisateurs — DataFrame :", user_quantity_df.count())
```

Output:

```text
Nombre d'utilisateurs — RDD : 50
Nombre d'utilisateurs — DataFrame : 50
```

Both implementations produce **50 user groups**.

### Inspect the DataFrame execution plan

```python
user_quantity_df.explain()
```

Example physical plan:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[user_id#4], functions=[sum(quantity#3L)])
   +- Exchange hashpartitioning(user_id#4, 200), ENSURE_REQUIREMENTS, [plan_id=239]
      +- HashAggregate(keys=[user_id#4], functions=[partial_sum(quantity#3L)])
         +- Project [quantity#3L, user_id#4]
            +- Scan ExistingRDD[date#0,order_id#1,product#2,quantity#3L,user_id#4]
```

### Inspect the RDD lineage

```python
print(user_quantity.toDebugString().decode())
```

Example output:

```text
(2) PythonRDD[104] at RDD at PythonRDD.scala:58 []
 |  MapPartitionsRDD[35] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[34] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[33] at reduceByKey at <python-input-21>:3 []
    |  PythonRDD[32] at reduceByKey at <python-input-21>:3 []
    |  PythonRDD[30] at RDD at PythonRDD.scala:58 []
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  orders.csv MapPartitionsRDD[17] at textFile at NativeMethodAccessorImpl.java:0 []
    |  orders.csv HadoopRDD[16] at textFile at NativeMethodAccessorImpl.java:0 []
```

### Comparison

| Aspect | RDD API | DataFrame API |
|---|---|---|
| Aggregation | Typically uses `reduceByKey` or `aggregateByKey` | Uses `groupBy` and `sum` |
| Execution inspection | `.toDebugString()` shows RDD lineage and dependencies | `.explain()` shows the logical or physical execution plan |
| Optimization | Lower-level operations; less SQL-level optimization | Catalyst can optimize the query plan |
| Result in this exercise | 50 user groups | 50 user groups |

**Line count:** The DataFrame implementation is more concise because the grouping and sum are expressed directly as declarative operations. The exact line-count comparison depends on whether blank lines, comments, and variable assignments are included.

**Execution-plan interpretation:**

- `HashAggregate` performs aggregation.
- `partial_sum` indicates partial aggregation before data is shuffled.
- `Exchange hashpartitioning(...)` represents redistribution of data by `user_id`.
- `Scan ExistingRDD` indicates that Spark is reading from the existing RDD-backed DataFrame.

In the RDD lineage, `ShuffledRDD` indicates a shuffle dependency. The `CachedPartitions` entry indicates that cached partitions are present in the lineage shown.

---

## 4. Measure the Effect of Caching

### Objective

Use `time.perf_counter()` to measure the execution time of:

- `orders_rdd.count()`
- `user_quantity.count()`

Run the measurements before and after removing `.cache()` from the parsing cell. Also inspect the **Storage** tab in the Spark UI in both cases.

### Timing code

The following code measures the actions while the RDD cache is enabled:

```python
import time

start = time.perf_counter()
orders_rdd.count()
print(
    "orders_rdd.count() avec cache :",
    time.perf_counter() - start,
    "secondes"
)

start = time.perf_counter()
user_quantity.count()
print(
    "user_quantity.count() avec cache :",
    time.perf_counter() - start,
    "secondes"
)
```

### Recorded results with caching enabled

```text
orders_rdd.count() avec cache : 0.33273567678406835 secondes
user_quantity.count() avec cache : 0.13822414306923747 secondes
```

### How to compare cached and uncached runs

1. **With caching enabled:** Run the timing cell and record both durations. Check the Spark UI's **Storage** tab to see which RDDs are persisted and how much memory they use.
2. **Without caching:** Remove `.cache()` from the parsing cell, rerun that cell to recreate the RDD without caching, and rerun the timing cell.
3. Compare the timings and the Storage tab between the two runs.

Use the same input data and the same actions in both cases. For a fairer comparison, consider repeating the measurements because Spark timings can vary with JVM warm-up, scheduling, and system load.

### Interpretation

Caching can improve performance when an RDD is reused by multiple actions, because Spark may reuse previously computed partitions instead of parsing the source data again.

However, caching is not guaranteed to make every action faster. The first action may need to compute and store the cached data, and caching also consumes memory.

**Important:** Only the timings for the cache-enabled run are provided here. The uncached timings and Storage-tab observations still need to be collected before drawing a measured conclusion about the performance difference.

---

## Summary

These exercises demonstrate four useful PySpark techniques:

1. **`aggregateByKey`** combines multiple per-user statistics in one aggregation.
2. **`leftOuterJoin`** identifies orders whose user records are missing.
3. **The DataFrame API** expresses aggregations concisely and exposes Spark's optimized execution plan.
4. **Caching and timing** help evaluate the performance impact of reusing intermediate RDDs.
