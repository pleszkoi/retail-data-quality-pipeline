from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("TestSparkSession")
    .master("local[*]")
    .getOrCreate()
)

print("Spark session started successfully.")
print(spark.range(5).collect())

spark.stop()
