from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp, avg, count, round, desc, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import math

# Creating Spark Session
spark = SparkSession.builder \
    .appName("Query4_Final") \
    .getOrCreate()

# Clearing Cache
spark.catalog.clearCache()

# Reading CSV files
crime_data_2010_to_2019_df = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present_df = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
police_stations = spark.read.csv("/home/user/hadoop_data/LAPD_Police_Stations.csv", header=True, inferSchema=True)

# Concatenating crime data
crime_data = crime_data_2010_to_2019_df.union(crime_data_2020_to_present_df)

# Turning 'DATE OCC' to TimestampType
crime_data = crime_data.withColumn('DATE OCC', to_timestamp('DATE OCC', 'MM/dd/yyyy hh:mm:ss a'))

# Filtering null LAT and LON
crime_data = crime_data.filter((col("LAT").isNotNull()) & (col("LON").isNotNull()))
police_stations = police_stations.filter((col("X").isNotNull()) & (col("Y").isNotNull()))

# Filtering crime with gun usage
firearm_crimes = crime_data.filter(col('Weapon Used Cd').startswith('1'))

# Filtering Null Island 
filtered_firearm_crimes = firearm_crimes.filter((col('LAT') != 0) & (col('LON') != 0))

# Connecting crime data with police stations data
merged_data = filtered_firearm_crimes.join(police_stations, filtered_firearm_crimes['AREA '] == police_stations['PREC'])

# Geo distance function
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius
    
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance

# Register UDF
haversine_udf = udf(haversine, DoubleType())

# Calculating distances
merged_data = merged_data.withColumn('distance', haversine_udf(col('LAT'), col('LON'), col('Y'), col('X')))

# Grouping for every police station and calculating average distance
result = merged_data.groupBy('DIVISION').agg(
    round(avg('distance'), 2).alias('average_distance'),
    count('DR_NO').alias('incidents_total')
)

sorted_result = result.orderBy(desc('incidents_total'))

# Showing Results
sorted_result.show(n=21)

# Save the results to a CSV file
sorted_result.write.format('csv').option('header', 'true').option('delimiter', '|').mode('overwrite').save('hdfs:///home/user/query4')

# Terminating Spark Session
spark.stop()
