from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import trim, col, concat_ws, md5
from datetime import datetime

from delta import configure_spark_with_delta_pip
import os, sys, time, re

class DeltaLakeFromSqlSchema():
    def __init__(self, spark:SparkSession):
        self.spark = spark
    def normalize_dataframe_like_excel(self, df):
        """
        Normalize DataFrame the same way Excel does when opening CSV files
        """
        print("Applying Excel-like normalization...")
        
        for column in df.columns:
            
            #Handle empty strings and nulls consistently
            df = df.withColumn(column, when(col(column) == "", None).otherwise(col(column)))
            
            #Detect and normalize numeric columns
            # Check if column contains numeric data by sampling
            sample_values = df.select(column).limit(100).collect()
            sample_strings = [row[0] for row in sample_values if row[0] is not None and row[0] != ""]
            
            if sample_strings:
                # Check if this looks like a numeric column
                is_numeric = self._is_numeric_column(sample_strings)
                is_datetime = self._is_datetime_column(sample_strings)
                
                if is_numeric:
                    df = self._normalize_numeric_column(df, column)
                elif is_datetime:
                    df = self._normalize_datetime_column(df, column)
                else:
                    df = self._normalize_string_column(df, column)
            else:
                print(f"  -> Empty/null column")
        
        return df
    
    def _is_numeric_column(self, sample_values, threshold=0.8):
        """Check if column appears to be numeric based on samples"""
        if not sample_values:
            return False
            
        numeric_count = 0
        for val in sample_values[:20]:  # Check first 20 samples
            if val and self._is_numeric_string(str(val)):
                numeric_count += 1
        
        return (numeric_count / len(sample_values[:20])) >= threshold
    
    def _is_datetime_column(self, sample_values):
        """Check if column appears to be datetime"""
        if not sample_values:
            return False
            
        # Look for common datetime patterns
        datetime_patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
            r'\d{4}-\d{2}-\d{2}',  # Date format
            r'\d{2}/\d{2}/\d{4}',  # US date format
        ]
        
        for val in sample_values[:5]:
            if val:
                for pattern in datetime_patterns:
                    if re.search(pattern, str(val)):
                        return True
        return False
    
    def _is_numeric_string(self, s):
        """Check if string represents a number"""
        try:
            float(s)
            return True
        except (ValueError, TypeError):
            return False
    
    def _normalize_numeric_column(self, df, column):
        """Normalize numeric column like Excel does"""
        # Convert to double, then back to string with consistent formatting
        # This handles: scientific notation, extra zeros, precision issues
        return df.withColumn(column, 
            when(col(column).isNull() | (col(column) == ""), None)
            .otherwise(
                # Convert to double then format consistently
                format_number(col(column).cast("double"), 15).cast("string")
            )
        )
    
    def _normalize_datetime_column(self, df, column):
        """Normalize datetime column like Excel does"""
        # Try to parse common datetime formats and standardize
        return df.withColumn(column,
            when(col(column).isNull() | (col(column) == ""), None)
            .otherwise(
                # Attempt to parse and reformat datetime
                # This is simplified - you might need more specific parsing
                regexp_replace(col(column), r'T(\d{2}:\d{2}:\d{2})\.\d+Z?', r'T$1Z')
            )
        )
    
    def _normalize_string_column(self, df, column):
        """Normalize string column like Excel does"""
        return df.withColumn(column,
            when(col(column).isNull() | (col(column) == ""), None)
            .otherwise(
                # Remove extra whitespace, normalize case if needed
                trim(regexp_replace(col(column), r'\s+', ' '))
            )
        )
    
    def validate_csv_files(self, memory_csv_file_path, reference_pyspark_csv_file_path):    
        memory_df = self.spark.read.option("header", "true").csv(memory_csv_file_path)
        reference_df = self.spark.read.option("header", "true").csv(reference_pyspark_csv_file_path)
        
        memory_df = self.normalize_dataframe_like_excel(memory_df)
        reference_df = self.normalize_dataframe_like_excel(reference_df)
        
        # Trim all columns
        for column in memory_df.columns:
            memory_df = memory_df.withColumn(column, trim(col(column)))
            reference_df = reference_df.withColumn(column, trim(col(column)))
        
        # Create hash of all columns combined for each row
        all_cols = memory_df.columns
        memory_df = memory_df.withColumn("row_hash", 
                                    md5(concat_ws("|", *[col(c) for c in all_cols])))
        reference_df = reference_df.withColumn("row_hash", 
                                            md5(concat_ws("|", *[col(c) for c in all_cols])))
        
        # Count unique hashes
        memory_hashes = memory_df.select("row_hash").distinct()
        reference_hashes = reference_df.select("row_hash").distinct()
        
        print(f"Memory CSV: {memory_df.count()} rows, {memory_hashes.count()} unique")
        print(f"Reference CSV: {reference_df.count()} rows, {reference_hashes.count()} unique")
        
        # Find differences
        only_in_memory = memory_hashes.subtract(reference_hashes)
        only_in_reference = reference_hashes.subtract(memory_hashes)
        
        if only_in_memory.count() == 0 and only_in_reference.count() == 0:
            print("Files contain identical data!")
        else:
            print("\n\nMISMATCHES FOUND...\n\n")
            print(f"Rows only in memory: {only_in_memory.count()}")
            print(f"Rows only in reference: {only_in_reference.count()}")
            
            # Show some examples of different rows
            if only_in_memory.count() > 0:
                print("\nSample rows only in memory:")
                memory_df.join(only_in_memory, "row_hash", "inner").drop("row_hash").show(3, truncate=False)
            
            if only_in_reference.count() > 0:
                print("\nSample rows only in reference:")
                reference_df.join(only_in_reference, "row_hash", "inner").drop("row_hash").show(3, truncate=False)


def main():

    if len(sys.argv) < 3:
        print("Usage: python script.py <spark-csv-path> <actor3-csv-path>")
        sys.exit(1)

    spark_csv_path =  sys.argv[1]
    actor3_csv_path = sys.argv[2]
    
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
        loader.validate_csv_files(memory_csv_file_path=actor3_csv_path, reference_pyspark_csv_file_path=spark_csv_path)
        spark.stop()
    except Exception as e:
        print(f"Error during operation => {e}")
    
if __name__ == '__main__':
    main()


