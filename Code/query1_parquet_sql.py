from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Query1_Parquet_SQL_Final").getOrCreate()

#Clean cache  before execution
spark.catalog.clearCache()

# Read the data from Parquet files
crime_data_2010_to_2019 = spark.read.parquet("/home/user/hadoop_data/Crime_Data_from_2010_2019.parquet")
crime_data_2020_to_present = spark.read.parquet("/home/user/hadoop_data/Crime_Data_from_2020_present.parquet")

# Create temporary views for the Parquet data
crime_data_2010_to_2019.createOrReplaceTempView("CrimeData2010to2019")
crime_data_2020_to_present.createOrReplaceTempView("CrimeData2020toPresent")

# Union the data from both periods
union_query = """
SELECT * FROM CrimeData2010to2019
UNION ALL
SELECT * FROM CrimeData2020toPresent
"""
crime_data = spark.sql(union_query)
crime_data.createOrReplaceTempView("CrimeData")

# Convert the "DATE OCC" column to timestamp
crime_data = spark.sql("""
SELECT *, TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a') as DateOcc
FROM CrimeData
""")
crime_data.createOrReplaceTempView("CrimeDataWithTimestamp")

# Group by year and month and count the number of crimes
crime_counts = spark.sql("""
SELECT
    YEAR(DateOcc) as Year,
    MONTH(DateOcc) as Month,
    COUNT(*) as CrimeTotal
FROM
    CrimeDataWithTimestamp
GROUP BY
    Year, Month
""")
crime_counts.createOrReplaceTempView("CrimeCounts")

# Rank the months by the number of crimes within each year
ranked_crime_counts = spark.sql("""
SELECT
    Year,
    Month,
    CrimeTotal,
    ROW_NUMBER() OVER (PARTITION BY Year ORDER BY CrimeTotal DESC) as Ranking
FROM
    CrimeCounts
""")
ranked_crime_counts.createOrReplaceTempView("RankedCrimeCounts")

# Select the top 3 months with the highest number of crimes for each year
top_months_per_year = spark.sql("""
SELECT
    Year,
    Month,
    CrimeTotal,
    Ranking
FROM
    RankedCrimeCounts
WHERE
    Ranking <= 3
ORDER BY
    Year ASC,
    CrimeTotal DESC
""")

# Save the results to a CSV file
top_months_per_year.write.format('csv').option('header', 'true').option('delimiter', '|').mode('overwrite').save('hdfs:///home/user/query1_parquet_sql')

# Show the results
top_months_per_year.show(n=60)

# Terminating Spark Session
spark.stop()
