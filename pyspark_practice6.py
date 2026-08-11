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
8
