A1. Filter IN orders; columns order_id, customer_id, amount_inr; sort amount desc.
A2. Add order_size: Small <100, Medium <300, else Large.
A3. Comment which ops are transformations vs actions; explore with show/count.
A4. Answer: lazy evaluation; RDD vs DataFrame; why collect() is dangerous.
Stretch. Function high_value_orders(df, min_amount)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("intreview").getOrCreate()
orders =  [
        (1001, 'c01', 'IN', 250.0, '2024-01-05'),
        (1002, 'c02', 'US', 40.0, '2024-01-06'),
        (1003, 'c01', 'IN', 120.0, '2024-01-07'),
        (1004, 'c03', 'UK', 500.0, '2024-01-07'),
        (1005, 'c02', 'US', 15.0, '2024-01-08'),
        (1006, 'c04', 'IN', 300.0, '2024-01-08'),
    ],
   cols= ['order_id', 'customer_id', 'country', 'amount', 'order_date'],
df=spark.CreateDataframe("orders","cols")
df.show()

TASK ONE
df.filter(F.col("country")=="IN")\
    .select("order_id","customer_id",F.col("amount").alias("amount_inr"))\
        .orderBy(F.col("amount_inr").desc())\
            .show()

TASK 2

orders.withColumn("order_size",F.when(F.col("amount")<100, "small")
                  .when(F.col("amount")< 300,"medium")
                  .otherwise("large")
                  ).show()
 Why is collect() dangerous?
collect() brings all data from worker nodes to the driver. If the dataset is very large, it can cause an OutOfMemory error. For large datasets, prefer show(), take()
Lazy Evaluation
Spark does not execute transformations immediately. It waits until an action like show() or count() is called.LASY

from pyspark.sql.functions import col

def high_value_orders(df, min_amount):
    return df.filter(col("amount") >= min_amount)

high_value_orders(orders, 100).show()



WHAT IS DIFFERENT BLW RDD V\S DATAFRAME
RDD (Resilient Distributed Dataset) is a low-level data structure in Spark. It does not have a schema,
so Spark doesn't know the column names and data types. RDD does not use the Catalyst Optimizer, so it is slower and requires more code.
DataFrame is a high-level data structure. It has a schema, so Spark knows the column names and data types. It uses the Catalyst Optimizer, which makes it faster. It also supports SQL-like operations, so the code is easier to write and understand.
In real-time projects, we mostly use DataFrames because they are faster and easier to work with.





        




