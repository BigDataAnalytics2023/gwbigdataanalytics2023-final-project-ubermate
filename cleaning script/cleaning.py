from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType, DoubleType, TimestampType
from functools import reduce

# Initialize Spark Session
spark = SparkSession.builder.appName("Preprocess Parquet Files").getOrCreate()

# Function to list files in an HDFS directory
def list_files_in_hdfs(spark, directory):
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(directory)
    return [str(f.getPath()) for f in fs.get(conf).listStatus(path)]

# Function to cast columns to desired types
def cast_column(df, column_name, target_type):
    for column in columnDict[column_name]:
        if column in df.columns:
            return df.withColumn(column_name, df[column].cast(target_type))
    return df.withColumn(column_name, lit(None).cast(target_type))

# Column name mapping
columnDict = {
    "PULocationID": ["PULocationID"],
    "DOLocationID": ["DOLocationID"],
    "base_passenger_fare": ["base_passenger_fare", "fare_amount"],
    "pickup_datetime": ["pickup_datetime", "lpep_pickup_datetime", "tpep_pickup_datetime"],
    "dropoff_datetime": ["dropoff_datetime", "lpep_dropoff_datetime", "tpep_dropoff_datetime"]
}

# Directory where your parquet files are stored
input_directory = "hdfs://localhost:9000/preprocess_test"

# Directory where you want to save the preprocessed files
output_directory = "hdfs://localhost:9000/parquet_preprocessed"

# List all files in the directory
file_paths = list_files_in_hdfs(spark, input_directory)

# Process each file and collect DataFrames
dataframes = []
for file_path in file_paths:
    df = spark.read.parquet(file_path)
    for column in columnDict:
        df = cast_column(df, column, LongType() if column in ["PULocationID", "DOLocationID"] else DoubleType() if column == "base_passenger_fare" else TimestampType())
    
    # Drop rows where any of the specified columns is NULL
    df = df.dropna(how='any', subset=["PULocationID", "DOLocationID", "base_passenger_fare", "pickup_datetime", "dropoff_datetime"])
    
    dataframes.append(df.select("PULocationID", "DOLocationID", "base_passenger_fare", "pickup_datetime", "dropoff_datetime"))

# Union all the DataFrames
if dataframes:
    final_df = dataframes[0]
    for df in dataframes[1:]:
        final_df = final_df.union(df)

    # Write the processed data back to parquet files
    final_df.write.mode("overwrite").parquet(output_directory)

# Stop the Spark Session
spark.stop()
