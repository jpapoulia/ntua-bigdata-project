from pyspark.sql import SparkSession

# Starting Spark Session
spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()\

# Converting csv to parquet
csv_files = [
    ("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", "/home/user/hadoop_data/Crime_Data_from_2010_2019.parquet"),
    ("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", "/home/user/hadoop_data/Crime_Data_from_2020_present.parquet"),
    ("/home/user/hadoop_data/revgecoding.csv", "/home/user/hadoop_data/revgecoding.parquet"),
    ("home/user/hadoop_data/LA_income_2015.csv","/home/user/hadoop_data/LA_income_2015.parquet"),
    ("/home/user/hadoop_data/LAPD_Police_Stations.csv", "/home/user/hadoop_data/LAPD_Police_Stations.parquet")]
for csv_file, parquet_output_path in csv_files:
        df = spark.read.csv(csv_file, header=True, inferSchema=True)
        df.write.mode("overwrite").parquet(parquet_output_path)

# Terminating Spark Session
spark.stop()
