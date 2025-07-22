from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp, avg, count, round, desc
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType, TimestampType
import math

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Query4_Improved_Repartition_Join_Final") \
    .getOrCreate()

spark.catalog.clearCache()

# Reading CSV files
crime_data_2010_to_2019_df = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present_df = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
lapd_df = spark.read.csv("/home/user/hadoop_data/LAPD_Police_Stations.csv", header=True, inferSchema=True)
lapd_df = lapd_df.drop("LOCATION")

crime_df = crime_data_2010_to_2019_df.union(crime_data_2020_to_present_df)

# Filtering null LAT and LON
crime_df = crime_df.filter((col("LAT").isNotNull()) & (col("LON").isNotNull()))
lapd_df = lapd_df.filter((col("X").isNotNull()) & (col("Y").isNotNull()))

# Filtering crime with gun usage
firearm_crimes = crime_df.filter(col('Weapon Used Cd').startswith('1'))

# Filtering Null Island 
filtered_firearm_crimes = firearm_crimes.filter((col('LAT') != 0) & (col('LON') != 0))


# Convert DATE OCC to TimestampType
filtered_firearm_crimes = filtered_firearm_crimes.withColumn('DATE OCC', to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))

# Map Phase
def map_phase(record, source):
    if source == 'crime':
        join_key = record['AREA ']
        tag = 'crime'
    else:
        join_key = record['PREC']
        tag = 'station'
    return (join_key, (tag, record.asDict()))

# Transforming DataFrames to RDDs
crime_rdd = filtered_firearm_crimes.rdd.map(lambda record: map_phase(record, 'crime'))
lapd_rdd = lapd_df.rdd.map(lambda record: map_phase(record, 'station'))

# Union the mapped RDDs
combined_rdd = crime_rdd.union(lapd_rdd)

# Partitioning the combined_rdd
num_partitions = combined_rdd.getNumPartitions()
partitioned_rdd = combined_rdd.partitionBy(num_partitions, lambda key: hash(key))


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

# Reduce Phase
def reduce_phase(records):
    crime_records = [rec for rec in records if rec[0] == 'crime']
    station_records = [rec for rec in records if rec[0] == 'station']

    results = []
    for crime in crime_records:
        for station in station_records:
            distance = haversine(crime[1]['LAT'], crime[1]['LON'], station[1]['Y'], station[1]['X'])
            results.append((crime[1]['AREA '], (distance, crime[1]['DR_NO'], station[1]['DIVISION'])))
    return results

# Group by key and reduce
grouped_rdd = partitioned_rdd.groupByKey().flatMap(lambda x: reduce_phase(list(x[1])))


# Transfroming the result to  DataFrame
schema = StructType([
    StructField("AREA", IntegerType(), True),
    StructField("distance", DoubleType(), True),
    StructField("DR_NO", IntegerType(), True),
    StructField("DIVISION", StringType(), True)
])

result_df = spark.createDataFrame(grouped_rdd.map(lambda x: Row(AREA=x[0], distance=x[1][0], DR_NO=x[1][1], DIVISION=x[1][2])), schema=schema)


# Grouping for every police station and calculating average distance
final_result = result_df.groupBy('DIVISION').agg(
    round(avg('distance'), 2).alias('average_distance'),
    count('DR_NO').alias('incidents_total')
)


sorted_result = final_result.orderBy(desc('incidents_total'))

# Showing results
print("Final sorted result:")
sorted_result.show(n=21)

# Terminating Spark Session
spark.stop()
