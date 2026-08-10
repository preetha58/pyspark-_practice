sales = spark.createDataFrame(
    [
        ('c1', '2024-01-01', 100),
        ('c1', '2024-01-02', 150),
        ('c1', '2024-01-02', 150),
        ('c2', '2024-01-01', 80),
        ('c2', '2024-01-03', 200),
        ('c3', '2024-01-01', 50),
    ],
    ['customer_id', 'sale_date', 'amount'],
)
dim = spark.createDataFrame(
    [('c1', 'East'), ('c2', 'West'), ('c3', 'East'), ('c4', 'North')],
    ['customer_id', 'region'],
)
Tasks
A1. Left join + anti join for customers with no sales.
A2. Totals by region.
A3. lag previous amount + day-over-day change.
A4. Dedup customer+date with row_number.

Left join + anti join for customers with no sales
Left join means: keep all customers from dim, even if they don't have sales.
joined = dim.join(
    sales,
    on="customer_id",
    how="left"
)
joined.show()
For customers with no sales, use a left anti join:
no_sales = dim.join(
    sales,
    on="customer_id",
    how="left_anti"
)

no_sales.show()
What is left_anti?
Think:
"Give me rows from the LEFT table that have NO matching row in the RIGHT table."
So:
Totals by region
First join sales with the region information:
from pyspark.sql import functions as F

region_sales = sales.join(
    dim,
    on="customer_id",
    how="left"
)

totals = region_sales.groupBy("region").agg(
    F.sum("amount").alias("total_sales")
)

totals.show()
East
Customers c1 and c3.
c1 = 100 + 150 + 150 = 400
c3 = 50
East total:
450
West
c2 = 80 + 200 = 280
North
c4 has no sales, so it won't appear in this aggregation.
lag() previous amount + day-over-day change
Here we want to compare each customer's current sale with their previous sale.
Use a window:
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("sale_date")
Then:
result = sales.withColumn(
    "previous_amount",
    F.lag("amount").over(w)
)
Now calculate the difference:
result = result.withColumn(
    "day_over_day_change",
    F.col("amount") - F.col("previous_amount")
)

result.show()
Dedup customer + date with row_number()
Your data contains:
c1  2024-01-02  150
c1  2024-01-02  150
We want only one row for each:
customer_id + sale_date
Create a window:
w = Window.partitionBy(
    "customer_id",
    "sale_date"
).orderBy(F.col("amount").desc())
Then:
deduped = sales.withColumn(
    "rn",
    F.row_number().over(w)
).filter(
    F.col("rn") == 1
).drop("rn")
deduped.show
