from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from delta import *
import json


import os
os.environ["HADOOP_HOME"] = "D:\\hadoop"

class DeltaLakeCSV():
    def __init__(self):
        pass
    def get_table_dim(self, delta_df:DataFrame):
        row_count = delta_df.count()
        col_count = delta_df.columns()
        print(f"Delta table dimenstion: {row_count} x {col_count}")

    def delta_table_schema(self, delta_df:DataFrame):
        for i, field in enumerate(delta_df.schema.fields, 1):
            print(f"column {i}:")
            print(f"{field.name}\n{field.dataType}\n{field.nullable}\n{field.metadata}\n\n\n")

    def get_table_history(self, delta_df:DataFrame):
        delta_log_dir = os.path.join("delta-table-4", "_delta_log")
        if os.path.exists(delta_log_dir):
            log_files = sorted([f for f in os.listdir(delta_log_dir) if f.endswith('.json')])
            for i, log_file in enumerate(log_files):
                file_path = os.path.join(delta_log_dir, log_file)
                with open(file_path, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        if line.strip():
                            entry = json.loads(line)
                            
                            if 'commitInfo' in entry:
                                commitInfo = entry['commitInfo']
                                print(f"timestampe: {commitInfo['timestamp']}, operation: {commitInfo['operation']}, BlindAppend: {commitInfo['isBlindAppend']}, operation metrics: {commitInfo['operationMetrics']}")
                        
                            elif 'metadata' in entry:
                                metadata = entry['metaData']
                                print(f"id: {metadata['id']}")
                                print(f"format: {metadata['format']['provider']}")
                                schema = json.loads(metadata['schemaString'])
                                print(f"schema: {len(schema['fields'])} fields")
                            elif 'add' in entry:
                                add_info = entry['add']
                                print(f"added file: {add_info['path']}")
                                print(f"{add_info['size']:,} bytes")
                                if 'stats' in add_info:
                                    stats = json.loads(add_info['stats'])
                                    print(f"Records: {stats.get('numRecords', 'N/A')}")
                                    print(f"Minimum values: {stats.get('minValues', 'N/A')}")
                                    print(f"Maximum values: {stats.get('maxValues', 'N/A')}")


def main():
    builder = SparkSession.builder \
        .appName("csv app") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    deltaLakeCSV = DeltaLakeCSV()

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
        authors.write.format("delta").mode("overwrite").save("delta-table-4")
        print("Delta table created successfully!")
        
        #loading from delta table to pyspark dataframe
        delta_df = spark.read.format("delta").load("delta-table-3")
        
        #Exploring loaded delta table
        print(f"Delta table columns: {delta_df.columns}")
        try:
            deltaLakeCSV.get_table_dim(delta_df)
        except Exception as e:
            print(f"Error getting table dimensions: {e}")

        try:    
            deltaLakeCSV.delta_table_schema(delta_df)
        except Exception as e:
            print(f"Error getting schema: {e}")
        
        try:
            deltaLakeCSV.get_table_history(delta_df)
        except Exception as e:
            print(f"Error getting history: {e}")

        delta_df.select("AUTHOR_NAME").show(5)

    except Exception as e:
        print(f"Error writing delta table: {e}")

    spark.stop()



if __name__ == '__main__':
    main()

