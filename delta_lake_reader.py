from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

from delta import configure_spark_with_delta_pip
import time, sys, os

class Loader():
    def __init__(self, spark:SparkSession):
        self.spark = spark
    def read_delta_table(self, delta_path: str = None) -> DataFrame:
        """
        Reads a Delta table from the given path and returns a DataFrame.
        If no path is provided, it defaults to the global delta_table_output_path.
        """
        try:
            target_path = delta_path
            print(f"Reading Delta table from: {target_path}")
            delta_df = self.spark.read.format("delta").load(target_path)
            print(f"Number of rows: {delta_df.count()}")
            delta_df.write.format("noop").mode("overwrite").save()
            print("Delta table loaded successfully!")
            return delta_df
        except Exception as e:
            print(f"Error reading Delta table => {e}")
            return None
    def get_table_name(self,delta_path: str = None) -> str:
        return os.path.basename(delta_path.rstrip("\\/"))
    
    
def main():
    
    """Configuring spark connection"""

    builder = SparkSession.builder \
            .appName("SQL Schema app") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true") \
            .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("Spark connection created!\n\n")
    
    delta_table_path = "C:\\Users\\Cyber\\Downloads\\smallTable_5000_10_50"          
    #log_file_txt = sys.argv[2] 

    loader = Loader(spark)
    start_time = time.time()
    loader.read_delta_table(delta_path=delta_table_path)
    end_time = time.time()
    elapsed_time = end_time - start_time
    table_name = loader.get_table_name(delta_path=delta_table_path)
    """
    with open(log_file_txt, "a") as f:
        f.write("\n")
        f.write(f"SPARK READS | {table_name} | IN {(elapsed_time*1000):.2f} Milli SECONDS")"""
    
    print(f"Spark Execution time: {end_time - start_time}")
if __name__ == '__main__':
    main()