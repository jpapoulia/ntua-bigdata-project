from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp, avg, count, round, desc, udf
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType, TimestampType
import math

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Broadcast Join Crime Analysis") \
    .getOrCreate()

# Function to compute the Haversine distance
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Register UDF
haversine_udf = udf(haversine, DoubleType())

# Read CSV files
crime_data_2010_to_2019_df = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present_df = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
lapd_df = spark.read.csv("/home/user/hadoop_data/LAPD_Police_Stations.csv", header=True, inferSchema=True).drop("LOCATION")

# Union crime data
crime_df = crime_data_2010_to_2019_df.union(crime_data_2020_to_present_df)

# Filter records with non-null coordinates
crime_df = crime_df.filter((col("LAT").isNotNull()) & (col("LON").isNotNull()))
lapd_df = lapd_df.filter((col("X").isNotNull()) & (col("Y").isNotNull()))

# Filter firearm crimes
firearm_crimes = crime_df.filter(col('Weapon Used Cd').startswith('1'))

# Filter out crimes at Null Island
filtered_firearm_crimes = firearm_crimes.filter((col('LAT') != 0) & (col('LON') != 0))

# Convert DATE OCC to TimestampType
filtered_firearm_crimes = filtered_firearm_crimes.withColumn('DATE OCC', to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))

# Broadcast the LAPD police stations dataframe
broadcast_lapd = spark.sparkContext.broadcast(lapd_df.collect())

# Define schema for the final DataFrame
schema = StructType([
    StructField("AREA", IntegerType(), True),
    StructField("distance", DoubleType(), True),
    StructField("DR_NO", IntegerType(), True),
    StructField("DIVISION", StringType(), True)
])

# Map Phase
def map_phase(record):
    area = record['AREA ']
    lat, lon = record['LAT'], record['LON']
    for station in broadcast_lapd.value:
        if station['PREC'] == area:
            station_lat, station_lon = station['Y'], station['X']
            distance = haversine(lat, lon, station_lat, station_lon)
            yield (area, (distance, record['DR_NO'], station['DIVISION']))

# Apply map phase
crime_rdd = filtered_firearm_crimes.rdd.flatMap(map_phase)

# Convert to DataFrame
result_df = spark.createDataFrame(crime_rdd.map(lambda x: Row(AREA=x[0], distance=x[1][0], DR_NO=x[1][1], DIVISION=x[1][2])), schema=schema)

# Group by DIVISION and compute average distance and total incidents
final_result = result_df.groupBy('DIVISION').agg(
    round(avg('distance'), 2).alias('average_distance'),
    count('DR_NO').alias('incidents_total')
)

# Sort results
sorted_result = final_result.orderBy(desc('incidents_total'))

# Show results
sorted_result.show(n=21)

# Save the results to a CSV file
sorted_result.write.format('csv').option('header', 'true').option('delimiter', '|').mode('overwrite').save('hdfs:///home/user/query4')

# Terminating Spark Session
spark.stop()
