# Initiating the Spark SQL Session
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df1 = spark.read.load("chessgames_january.parquet")
df2 = spark.read.load("chessgames_february.parquet")

result = df1.union(df2)
df3 = spark.read.load("chessgames_march.parquet")
result = result.union(df3)
result.write.save("chessgames.parquet")