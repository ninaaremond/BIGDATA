    1. Rewrite user_quantity with aggregateByKey, producing (order_count, total_quantity) per user in a single pass, then compute the average order quantity per user from that.

>>> user_stats = (
...     orders_rdd
...     .map(lambda o: (o["user_id"], o["quantity"]))
...     .aggregateByKey(
...         (0, 0),
...         lambda acc, q: (acc[0] + 1, acc[1] + q),
...         lambda a, b: (a[0] + b[0], a[1] + b[1])
...     )
... )
... 
... user_stats_with_average = user_stats.mapValues(
...     lambda stats: (
...         stats[0],
...         stats[1],
...         stats[1] / stats[0] if stats[0] else 0
...     )
... )
... 
... user_stats_with_average.take(5)
... 
[('f143262f-dc5c-4eed-8da0-365bf89897b9', (4, 12, 3.0)), ('969b6662-0562-4059-968c-c69b1064005c', (12, 40, 3.3333333333333335)), ('ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec', (85, 253, 2.976470588235294)), ('5563f616-00e8-4ece-8b49-452d46d483f3', (21, 57, 2.7142857142857144)), ('f94d6204-6808-493f-9fed-2c43e256a6dc', (49, 175, 3.5714285714285716))]

explanation of the output : the user 'f143262f-dc5c-4eed-8da0-365bf89897b9' 
has 4 orders, a total quantity of 12, an average of 3 units per order (4, 12, 3.0)


    2. Inject one fake user_id into a copy of orders_rdd (one not present in users_kv) and redo the leftOuterJoin(). Filter for the orders with no matching user and count them.

>>> fake_order = orders_rdd.first().copy()
... fake_order["user_id"] = "user-inexistant"
... 
... orders_with_fake_user = orders_rdd.map(
...     lambda o: (o["user_id"], o)
... ).union(
...     sc.parallelize([(fake_order["user_id"], fake_order)])
... )
... 
... left_joined = orders_with_fake_user.leftOuterJoin(users_kv)
... 
... missing_users = left_joined.filter(
...     lambda item: item[1][1] is None
... )
... 
... print("Commandes sans utilisateur :", missing_users.count()\
)
... print(missing_users.take(1))
... 
[Stage 23:>                                                     [Stage 23:===================>                                  [Stage 23:======================================>                                                                               Commandes sans utilisateur : 1
[('user-inexistant', ({'order_id': '2339ba19-2563-4cc3-8a97-ebf555d596af', 'user_id': 'user-inexistant', 'date': '2020-01-01 00:02:17.891488+00:00', 'quantity': 4, 'product': 'drink'}, None))]
>>> 

    3. Reimplement user_quantity with the DataFrame API in a new cell: groupBy("user_id").sum("quantity"). Compare the two cells for line count, and compare .explain() on the DataFrame version with the RDD's .toDebugString().

>>> orders_df = spark.createDataFrame(orders_rdd)
... 
... user_quantity_df = (
...     orders_df
...     .groupBy("user_id")
...     .sum("quantity")
... )
... 
... user_quantity_df.show(5, truncate=False)
... 
[Stage 30:>                                                                                                                     +------------------------------------+-------------+
|user_id                             |sum(quantity)|
+------------------------------------+-------------+
|f53d4330-cdda-44ba-ad06-e8cf3805f907|250          |
|1190f938-a66f-47f7-b966-9fa759970043|92           |
|490617f2-747b-4dba-88fe-3ccdc8b8d9c6|43           |
|8a476a87-e49d-481d-91d8-7c6455fa1ab8|223          |
|ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec|253          |
+------------------------------------+-------------+
only showing top 5 rows
>>> print("Nombre d'utilisateurs — RDD :", user_quantity.count(\
))
... print("Nombre d'utilisateurs — DataFrame :", user_quantity_\
df.count())
... 
Nombre d'utilisateurs — RDD : 50
Nombre d'utilisateurs — DataFrame : 50

>>> user_quantity_df.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[user_id#4], functions=[sum(quantity#3L)])
   +- Exchange hashpartitioning(user_id#4, 200), ENSURE_REQUIREMENTS, [plan_id=239]
      +- HashAggregate(keys=[user_id#4], functions=[partial_sum(quantity#3L)])
         +- Project [quantity#3L, user_id#4]
            +- Scan ExistingRDD[date#0,order_id#1,product#2,quantity#3L,user_id#4]


>>> print(user_quantity.toDebugString().decode())
(2) PythonRDD[104] at RDD at PythonRDD.scala:58 []
 |  MapPartitionsRDD[35] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[34] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[33] at reduceByKey at <python-input-21>:3 []
    |  PythonRDD[32] at reduceByKey at <python-input-21>:3 []
    |  PythonRDD[30] at RDD at PythonRDD.scala:58 []
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  orders.csv MapPartitionsRDD[17] at textFile at NativeMethodAccessorImpl.java:0 []
    |  orders.csv HadoopRDD[16] at textFile at NativeMethodAccessorImpl.java:0 []
>>> 

    4. Time orders_rdd.count() and user_quantity.count() before and after removing .cache() from the parsing cell, with time.perf_counter(). Check the Storage tab of the Spark UI in both cases.

>>> import time
... 
... start = time.perf_counter()
... orders_rdd.count()
... print("orders_rdd.count() avec cache :", time.perf_counter() - start, "secondes")
... 
... start = time.perf_counter()
... user_quantity.count()
... print("user_quantity.count() avec cache :", time.perf_counter() - start, "secondes")
... 
orders_rdd.count() avec cache : 0.33273567678406835 secondes
user_quantity.count() avec cache : 0.13822414306923747 secondes
>>> 