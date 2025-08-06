from pyspark.sql import SparkSession
from delta import *

import os
os.environ["HADOOP_HOME"] = "D:\\hadoop"


builder = SparkSession.builder \
    .appName("csv app") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    authors = spark.read.csv('authors.csv', sep=',', inferSchema=True, header=True)
    print("CSV loaded successfully")
    authors.show(5)  
except Exception as e:
    print(f"Error parsing csv: {e}")

try:
    df = authors.toPandas()
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
except Exception as e:
    print(f"Error converting to pandas: {e}")

# Write to Delta Table - this should work now
try:
    authors.write.format("delta").mode("overwrite").save("delta-table-3")
    print("Delta table created successfully!")
    
    delta_df = spark.read.format("delta").load("delta-table-3")
    
    print(f"Delta table has {delta_df.count()} rows")
    print(f"Delta table columns: {delta_df.columns}")
    delta_df.select("AUTHOR_NAME").show(5)
except Exception as e:
    print(f"Error writing delta table: {e}")

spark.stop()