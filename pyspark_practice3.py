from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName('AzureDE-InterviewPrep')
    .getOrCreate()
