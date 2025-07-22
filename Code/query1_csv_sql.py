from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp,unix_timestamp
from pyspark.sql.window import Window

#Create SparkSession
spark = SparkSession.builder.appName("Query1_Csv_SQL_Final").getOrCreate()

#Clean cache  before execution
print("Clearing Spark cache...")
spark.catalog.clearCache()
print("Cache cleared.")

#Read the data
crime_data_2010_to_2019 = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)

#Concat the data
crime_data = crime_data_2010_to_2019.union(crime_data_2020_to_present)

#Convert the column "DATE OCC" in datetime
crime_data = crime_data.withColumn("DATE OCC", to_timestamp("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))
crime_data.createOrReplaceTempView("crime_data")

#Group the data by year and month and calculate the number of crimes
crime_counts = spark.sql("""
    SELECT year(`DATE OCC`) AS Year, month(`DATE OCC`) AS Month, COUNT(*) AS `Crime Total`
    FROM crime_data
    GROUP BY year(`DATE OCC`), month(`DATE OCC`)
""")
crime_counts.createOrReplaceTempView("crime_counts")

#Use window function for the classification of the data
sorted_crime_counts = spark.sql("""
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Year ORDER BY `Crime Total` DESC) AS Rank
    FROM crime_counts
""")
sorted_crime_counts.createOrReplaceTempView("sorted_crime_counts")

#select the three months with the highest number of crimes for each year
top_months_per_year = spark.sql("""
    SELECT Year, Month, `Crime Total`, Rank
    FROM sorted_crime_counts
    WHERE Rank <= 3
    ORDER BY Year, Rank
""")

#Save the results to a CSV file
top_months_per_year.write.format('csv').option('header', 'true').option('delimiter', '|').mode('overwrite').save('hdfs:///home/user/query1_csv_sql')

print("The 3 months with the highest number of recorded crimes in ascending order of the year and descending order of the number of records:\n")
top_months_per_year.show(n=60)
