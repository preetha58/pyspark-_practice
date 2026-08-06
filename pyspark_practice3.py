from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = SparkSession.builder
    .appName('AzureDE-InterviewPrep')
    .getOrCreate()

schema = T.StructType([
    T.StructField('id', T.IntegerType(), True),
    T.StructField('name', T.StringType(), False),
    T.StructField('salary', T.DoubleType(), True),
])
df = spark.createDataFrame(
    [(1, 'Alice', 70000.0), (2, 'Bob', None), (3, 'Carol', 72000.0)],
    schema,
)
df.printSchema()
df.show()

Nulls, cast, rename
(
    df.withColumnRenamed('name', 'employee_name')
      .na.fill({'salary': 0.0})
      .na.drop(subset=['employee_name'])
      .show()
)

Nested structs and arrays
nested_schema = T.StructType([
    T.StructField('id', T.IntegerType(), False),
    T.StructField('address', T.StructType([
        T.StructField('city', T.StringType(), True),
        T.StructField('pin', T.StringType(), True),
    ]), True),
    T.StructField('skills', T.ArrayType(T.StringType()), True),
])
nested = spark.createDataFrame(
    [
        (1, {'city': 'Pune', 'pin': '411001'}, ['spark', 'sql']),
        (2, {'city': 'London', 'pin': 'SW1A'}, ['adf', 'fabric']),
    ],
    nested_schema,
)
nested.select('id', 'address.city', 'address.pin').show()
nested.select('id', F.explode('skills').alias('skill')).show()
