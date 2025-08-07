from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from datetime import datetime
from delta import *
import os, json

#Database configs
DB_HOST_DEV="localhost"
DB_PORT_DEV="3306"
DB_DATABASE_DEV="zad"
DB_USER_DEV="ZAD-aly"
DB_PASSWORD_DEV="1234"
TABLE_NAME = "student"

os.environ["HADOOP_HOME"] = "D:\\hadoop"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_2.13:4.0.0,"
    "mysql:mysql-connector-java:8.0.33 pyspark-shell"
)


#Database configurations
jdbc_url = f"jdbc:mysql://{DB_HOST_DEV}:{DB_PORT_DEV}/{DB_DATABASE_DEV}?useSSL=false"

connenction_properties = {
    "user": DB_USER_DEV,
    "password": DB_PASSWORD_DEV,
    "driver":"com.mysql.cj.jdbc.Driver"
}

delta_table_output_path = f"delta-table-{TABLE_NAME}"


class DeltaLakeFromSqlSchema():
    def __init__(self, spark:SparkSession):
        self.spark = spark
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

    def insert_operation(self, initial_delta_table:DataFrame):
        
        def parse_date(date_str):
            return datetime.strptime(date_str, "%Y-%m-%d")
        
        new_students_data = [
            (9999, "john.doe@newschool.edu", "encrypted_password", 1, "John Doe", "John",
            "Male", parse_date("2019-01-01"), "/uploads/profile-john.jpg", parse_date("2024-01-01"),
            parse_date("2024-01-01"), parse_date("2024-01-01"), "", "b", 100, True),

            (9998, "jane.smith@newschool.edu", "encrypted_password", 1, "Jane Smith", "Jane",
            "Female", parse_date("2019-02-01"), "/uploads/profile-jane.jpg", parse_date("2024-01-01"),
            parse_date("2024-01-01"), parse_date("2024-01-01"), "", "c", 150, True),

            (9997, "bob.wilson@newschool.edu", "encrypted_password", 2, "Bob Wilson", "Bob",
            "Male", parse_date("2019-03-01"), "/uploads/profile-bob.jpg", parse_date("2024-01-01"),
            parse_date("2024-01-01"), parse_date("2024-01-01"), "", "d", 75, False)
        ]

        current_schema = initial_delta_table.schema
        new_df = self.spark.createDataFrame(new_students_data, current_schema)
        new_df.write.format("delta").mode("append").save(delta_table_output_path)
    
    def update_operation(self):
        delta_table = DeltaTable.forPath(self.spark, delta_table_output_path)
        try:
            delta_table.update(
                condition = col("id") >= 9997,
                set = {"coins": col("coins") + 50}
            )
        except Exception as e:
            print("Error updating table")

    def create_table_with_column_mapping(self, df:DataFrame, mode: str='id'):
        try:
            (df.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", mode).save(delta_table_output_path))
            print(f"Delta table with column mapping created at: {delta_table_output_path}")
            # Verify column mapping is enabled
            delta_table = DeltaTable.forPath(self.spark, delta_table_output_path)
            table_properties = delta_table.detail().select("properties").collect()[0]["properties"]
            
            print(f"📋 Table Properties:")
            for key, value in table_properties.items():
                if "columnMapping" in key or "minReaderVersion" in key or "minWriterVersion" in key:
                    print(f"   {key}: {value}")
        except Exception as e:
            print(f"Error creating Delta table with column mapping: {e}")
    def alter_table(self):
        try:
            delta_table = DeltaTable.forPath(self.spark, delta_table_output_path)
            self.spark.sql(f"""
                ALTER TABLE delta.`{delta_table_output_path}`
                RENAME COLUMN id TO student_id
            """)
            
            self.spark.sql(f"""
                ALTER TABLE delta.`{delta_table_output_path}`
                RENAME COLUMN email TO student_email
            """)
        except Exception as e:
            print()
def main():
    builder = SparkSession.builder \
            .appName("SQL Schema app") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("Spark connection created!\n\n")
    
    loader = DeltaLakeFromSqlSchema(spark)    
    try:
        df = spark.read.jdbc(url=jdbc_url, table=TABLE_NAME, properties=connenction_properties)
        print(f"Spark dataframe created successfully\n")
        df.show(5)
    except Exception as e:
        print(f"Error in reading database table:{TABLE_NAME}=>:{e}")
    """try:
        df = spark.read.jdbc(url=jdbc_url, table=TABLE_NAME, properties=connenction_properties)
        print(f"Spark dataframe created successfully\n")
        df.show(5)
    except Exception as e:
        print(f"Error in reading database table:{TABLE_NAME}=>:{e}")    

    try:
        df.write.format("delta").mode("overwrite").save(delta_table_output_path)
        print("Delta table is written successfully!")
        try:
            delta_df = spark.read.format("delta").load(delta_table_output_path)
            print("Delta table loaded successfully!")
            loader.get_table_dim(delta_df)
            loader.delta_table_schema(delta_df)
            loader.get_table_history(delta_df)
        except Exception as e:
            print(f"Error loading delta table=> {e}")"""
    try:
        if os.path.exists(os.path.join(delta_table_output_path, "_delta_log")):
            print("Delta table already exists. Proceeding to insert...")
        else:
            print("Delta table doesn't exist. You need to create it before inserting.")
            return

        #loader.insert_operation(initial_delta_table=df)
        loader.update_operation()
    except Exception as e:
        print(f"Error during insert operation => {e}")    
    """except Exception as e:
        print(f"Error writting delta table => {e}")"""
if __name__ == '__main__':
    main()


