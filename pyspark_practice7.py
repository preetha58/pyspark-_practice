Create two DataFrames:
df = spark.range(0, 100000).withColumn(
    "grp",
    (F.col("id") % 10).cast("int")
)

df2 = (
    spark.range(0, 100000)
    .withColumnRenamed("id", "id2")
    .withColumn(
        "grp",
        (F.col("id2") % 10).cast("int")
    )
)
Now join:
joined = df.join(df2, "grp")
Then:
joined.explain("formatted")
You may see something like:
Exchange
   ↓
Sort
   ↓
SortMergeJoin
   ↑
Sort
   ↑
Exchange
Why Exchange?
Exchange generally indicates a shuffle.
Spark has data spread across different partitions:
Broadcast join plan comparison
Now suppose one DataFrame is small and the other is large.
For example:
df  → 100 million rows
df2 → 1,000 rows
We don't want to shuffle the huge DataFrame unnecessarily.
Instead, we can broadcast the small DataFrame.
from pyspark.sql.functions import broadcast

joined = df.join(
    broadcast(df2),
    "grp"
)

joined.explain("formatted")
Broadcast join is useful when one side of the join is small enough to fit in executor memory. Spark broadcasts the small DataFrame to the executors, which can avoid a large shuffle and improve join performance.
Partition counts after repartition / coalesce
This question is checking whether you understand how the number of partitions changes.
Start with:
print(df.rdd.getNumPartitions())
Suppose it says:
4
So:
df → 4 partitions
repartition(8)
wide = df.repartition(8)
Now:
print(wide.rdd.getNumPartitions())
Output:
repartition() causes a shuffle.
coalesce() is generally used to reduce partitions without a full shuffle.

    One skew example + mitigation
What is data skew?
Data skew means:
Some partitions contain much more data than others.
Suppose you're joining sales by customer_id.
Normally:
Customer A → 10,000 rows
Customer B → 9,000 rows
Customer C → 11,000 rows
Pretty balanced.
But suppose:
Customer A → 90,000,000 rows ❌
Customer B → 10,000 rows
Customer C → 15,000 rows
Customer D → 8,000 rows
Customer A is a hot key.
If Spark partitions based on customer_id, one partition can become huge.
Partition 1 → 90 GB ❌
Partition 2 → 2 GB
Partition 3 → 2 GB
Partition 4 → 2 GB
Now:
Task 1 → 20 minutes ❌
Task 2 → 1 minute
Task 3 → 1 minute
Task 4 → 1 minute
The entire stage has to wait for Task 1.
How do we mitigate skew?
There are several approaches.
1. Broadcast the small side
If you're joining:
Huge sales table
        +
Small customer table
use:
df.join(
    broadcast(customer_df),
    "customer_id"
)
This can avoid a large shuffle.
2. Use AQE skew join handling
Spark's Adaptive Query Execution (AQE) can detect skewed shuffle partitions and handle them by splitting skewed partitions.
Depending on your Spark configuration/version, you can enable:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
3. Salting
For severe skew, you can add a random salt to distribute a hot key across multiple partitions.
Conceptually:
customer A
    ↓
A_0
A_1
A_2
A_3
Instead of putting all of Customer A's data into one partition, we distribute it across several partitions.
This is called salting.
