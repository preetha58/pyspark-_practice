from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import col
events = spark.createDataFrame(
    [
        ('{"customer_id":"c1","profile":{"city":"Pune","age":28},"tags":["new","in"],"amount":"100"}',),
        ('{"customer_id":"c2","profile":{"city":"Delhi","age":null},"tags":["vip"],"amount":null}',),
        ('{"customer_id":"c3","profile":{"city":"Pune","age":35},"tags":[],"amount":"250.5"}',),
    ],
    ['raw_json'],
)
events.show(truncate=False)
Tasks
A1. Parse JSON with explicit schema.
A2. Flatten city/age; cast amount; fill null amount with 0.
A3. explode_outer tags.
A4. Why explicit schema over inferSchema in production?

1.here we have 3 rows 
and data frame has one column nmaed raw_json
1. Parse JSON with explicit schema.
schema = T.StructType([
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("profile", T.StructType([ # nested object 
        T.StructField("city", T.StringType(), True),
        T.StructField("age", T.IntegerType(), True)
    ]), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True),#array of strings
    T.StructField("amount", T.StringType(), True)
])

parsed = events.select(
    F.from_json(F.col("raw_json"), schema).alias("data")
)
F.from_json=Use this schema to convert the JSON string into structured columns
 parsed is the DataFrame variable name
It stores the entire DataFrame.
 Data is Inside the DataFrame, we have one column named data.
2. Flatten city/age; cast amount; fill null amount with 0.
flat = parsed.select(
    F.col("data.customer_id"),
    F.col("data.profile.city").alias("city"),
    F.col("data.profile.age").alias("age"),
    F.col("data.amount").cast("double").alias("amount"),
    F.col("data.tags")
)
flat = flat.na.fill({"amount":0})
flat.show()

3.explode_outer tags.
flat.select(
    "customer_id",
    F.explode_outer("tags").alias("tag")#covert array into rows
).show()

If tags are empty ([]), explode_outer() still keeps the row and returns NULL for the tag
.
In production, we use an explicit schema because it is faster and more reliable. 
Spark doesn't need to scan the data to guess the data types, which improves performance. It also avoids incorrect data type inference."


