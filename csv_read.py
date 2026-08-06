from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("id", T.IntegerType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("salary", T.DoubleType(), True),
    T.StructField("department", T.StringType(), True)
])
csv file read
df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/my_volume/employees.csv")
 df = spark.read \
    .schema(schema) \
    .option("multiline", "true") \
    .json("/path/employees.json")
parquet
 df = spark.read \
    .schema(schema) \
    .parquet("/path/employees.parquet")
orc
 df = spark.read \
    .schema(schema) \
    .orc("/path/employees.orc")

how to write data

Append – Adds new data to the existing data.
df.write.mode("append").csv("/path/output")
Overwrite – Deletes the existing data and writes new data.
df.write.mode("overwrite").csv("/path/output")
Ignore – If the output already exists, Spark does nothing.
df.write.mode("ignore").csv("/path/output")
Error (default) – Throws an error if the output already exists.
df.write.mode("error").csv("/path/output")
Interview example
Suppose your table already contains:
id
name
1
Alice
2
Bob
Your new DataFrame contains:
id
name
3
Carol
4
David
If you use:
df.write.mode("append").parquet("/path/employees")
The output becomes:
id
name
1
Alice
2
Bob
3
Carol
4
David
If you use:
df.write.mode("overwrite").parquet("/path/employees")
The old data is removed, and only this remains:
id
name
3
Carol
4
David
