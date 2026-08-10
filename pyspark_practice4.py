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
