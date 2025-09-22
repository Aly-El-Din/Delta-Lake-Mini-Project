from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import trim, col, concat_ws, md5
from datetime import datetime

from delta import configure_spark_with_delta_pip
import os, sys, time, shutil

class DeltaLakeFromSqlSchema():
    def __init__(self, spark:SparkSession):
        self.spark = spark

    def export_delta_to_csv(self, delta_table_output_path: str, output_dir_path: str, table_name:str, overwrite: bool = True,):
        try:
            print("Reading Delta table...")
            start_time = time.time()
            delta_df = self.spark.read.format("delta").load(delta_table_output_path)
            end_time = time.time()
            print(f"Delta lake read in:{(end_time-start_time)*1000.0} ms")
            # Format date columns to match Java output (M/d/yyyy format)
            print("Formatting date columns...")
            for column_name, data_type in delta_df.dtypes:
                if data_type == 'date':
                    # Convert from YYYY-MM-DD to M/d/yyyy format
                    delta_df = delta_df.withColumn(
                        column_name, 
                        date_format(col(column_name), "M/d/yyyy")
                    )
                    print(f"Formatted date column: {column_name}")

            print("Writing Delta table to one CSV file...")
            # Always write to a temporary subdir
            temp_dir = os.path.join(output_dir_path, "temp_output")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

            mode = "overwrite" if overwrite else "append"
            (delta_df
                .coalesce(1)
                .write
                .option("header", "true")
                .mode(mode)
                .csv(temp_dir))

            # Find the part file Spark wrote
            part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".csv")][0]

            final_name = f"{table_name}_spark.csv"
            final_path = os.path.join(output_dir_path, final_name)

            # Move/rename to final name
            shutil.move(os.path.join(temp_dir, part_file), final_path)
            shutil.rmtree(temp_dir)

            print(f"Delta table successfully written to: {final_path}")

        except Exception as e:
            print(f"Error exporting Delta table to CSV => {e}")

def extract_table_name(delta_table_path: str):
    """Extract the last directory name from the Delta table path"""
    return os.path.basename(os.path.normpath(delta_table_path))

def main():

    if len(sys.argv) < 3:
        print("Usage: python script.py <delta-table-path> <output-path>")
        sys.exit(1)

    delta_table_path = sys.argv[1]

    delta_table_name =  extract_table_name(delta_table_path)

    output_dir_path = sys.argv[2]
    
    
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
    
    loader = DeltaLakeFromSqlSchema(spark)
    
    try:
        if os.path.exists(os.path.join(delta_table_path, "_delta_log")):
            print("Delta table already exists. Proceeding to operation...")
        else:
            print("Delta table doesn't exist. You need to create it before inserting.")
            return
        loader.export_delta_to_csv(delta_table_output_path=delta_table_path, output_dir_path=output_dir_path, table_name=delta_table_name)
        spark.stop()
    except Exception as e:
        print(f"Error during operation => {e}")
    
if __name__ == '__main__':
    main()

