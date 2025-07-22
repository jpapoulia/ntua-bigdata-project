from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Query2_RDD_Final").getOrCreate()

#Clean cache  before execution
spark.catalog.clearCache()

# Read data from CSV files into RDDs
crime_data_2010_to_2019_rdd = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True).rdd
crime_data_2020_to_present_rdd = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True).rdd

# Merge the RDDs
crime_data_rdd = crime_data_2010_to_2019_rdd.union(crime_data_2020_to_present_rdd)

# Filter the data for crimes that took place on the street
crime_data_street_rdd = crime_data_rdd.filter(lambda row: row['Premis Desc'] == 'STREET')

# Function to determine the day part
def get_time_of_day(time_occ):
    hour = int(str(time_occ)[:2]) 
    if 5 <= hour < 12:
        return "Πρωί"
    elif 12 <= hour < 17:
        return "Απόγευμα"
    elif 17 <= hour < 21:
        return "Βράδυ"
    else:
        return "Νύχτα"

# Map the data to (TimeOfDay, 1)
time_of_day_rdd = crime_data_street_rdd.map(lambda row: (get_time_of_day(row['TIME OCC']), 1))

# Aggregation and counting of crimes by part of the day
time_of_day_counts_rdd = time_of_day_rdd.reduceByKey(lambda x, y: x + y)

# Sort results in descending order
sorted_time_of_day_counts_rdd = time_of_day_counts_rdd.sortBy(lambda x: x[1], ascending=False)

# Convert results to DataFrame to save to CSV
sorted_time_of_day_counts_df = sorted_time_of_day_counts_rdd.toDF(["TimeOfDay", "CrimeCount"])

# Save results to CSV
sorted_time_of_day_counts_df.write.format('csv').option('header', 'true').mode('overwrite').save('hdfs:///home/user/query2_rdd')

# Display results
sorted_time_of_day_counts_df.show()

# Stop SparkSession
spark.stop()
