import pyspark.sql
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.range(20, 25)
try:
    data.write.format("delta").mode("append").save("delta-table-2")
except Exception as e:
    print(f"error writing in delta table: {e}")
#data.write.format("delta").mode("append").save("delta-table")

#Same spark session, overwrite the same table.
