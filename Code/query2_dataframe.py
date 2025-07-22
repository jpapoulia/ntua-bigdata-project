from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

#Create SparkSession
spark = SparkSession.builder.appName("Query2_Dataframe_Final").getOrCreate()

#Clean cache  before execution
spark.catalog.clearCache()

#Read the data
crime_data_2010_to_2019 = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)

#Concat the data
df = crime_data_2010_to_2019.union(crime_data_2020_to_present)

# Filter the data for crimes that took place on the street
df_street = df.filter(col("Premis Desc") == "STREET")


# Create a new column for parts of the day
df_street = df_street.withColumn(
    "TimeOfDay",
    when((col("TIME OCC").substr(1, 2).cast("int") >= 5) & (col("TIME OCC").substr(1, 2).cast("int") < 12), "Πρωί")
    .when((col("TIME OCC").substr(1, 2).cast("int") >= 12) & (col("TIME OCC").substr(1, 2).cast("int") < 17), "Απόγευμα")
    .when((col("TIME OCC").substr(1, 2).cast("int") >= 17) & (col("TIME OCC").substr(1, 2).cast("int") < 21), "Βράδυ")
    .otherwise("Νύχτα")
)

# Grouping and counting of crimes by part of the day
time_of_day_counts = df_street.groupBy("TimeOfDay").agg(count("*").alias("CrimeCount"))

# Sort the parts of the day in descending order
sorted_time_of_day_counts = time_of_day_counts.orderBy(col("CrimeCount").desc())

# Save the results to a CSV file
sorted_time_of_day_counts.write.format('csv').option('header', 'true').mode('overwrite').save('hdfs:///home/user/query2_dataframe')
sorted_time_of_day_counts.show()

# Terminating Spark Session
spark.stop()
