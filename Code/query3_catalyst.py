from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, regexp_replace, first
from pyspark.sql.types import StringType, IntegerType

# Starting SparkSession
spark = SparkSession.builder.appName("Query3_Catalyst").getOrCreate()

# Clearing Cache
spark.catalog.clearCache()

# Reading Crime Data
crime_data_2010_to_2019 = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present = spark.read.csv("/home/user/hadoop_data/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)

# Concatenating Data
crime_data = crime_data_2010_to_2019.union(crime_data_2020_to_present)

# Reading Income Data
income_data = spark.read.csv("/home/user/hadoop_data/LA_income_2015.csv", header=True, inferSchema=True)
income_data = income_data.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[^0-9]", ""))
income_data = income_data.withColumn("Estimated Median Income", col("Estimated Median Income").cast(IntegerType()))

# Reading Zip Code Data
zip_code_df = spark.read.csv("/home/user/hadoop_data/revgecoding.csv", header=True, inferSchema=True)

# Keep only one ZIP Code per coordinate pair
zip_code_df = zip_code_df.groupBy("LAT", "LON").agg(first("ZIPcode").alias("ZIPcode"))

# Filtering Crime Data for the year 2015 and valid Victim Descent
crime_2015 = crime_data.filter((col("Date Rptd").substr(7, 4) == "2015") & col("Vict Descent").isNotNull())


# Joining Crime Data with Zip Code Data
crime_with_zip = crime_2015.alias("crime").join(
    zip_code_df.alias("zip"), 
    (col("crime.LAT") == col("zip.LAT")) & (col("crime.LON") == col("zip.LON")), 
    "inner"
)

# Creating 'all_data' DataFrame for subsequent left_semi join
all_data = crime_with_zip.join(
    income_data.alias("income"), 
    col("zip.ZIPcode") == col("income.Zip Code"), 
   "inner"
)

# Joining Crime Data with Income Data using left_semi
crimes_with_income = all_data.join(
    income_data.alias("income2"), 
    col("zip.ZIPcode") == col("income2.Zip Code"), 
    "left_semi"
)

def get_distinct_top_bottom(df, column, order, n=3):
    distinct_df = df.select(column).distinct().orderBy(order)
    unique_values = []
    seen_values = set()
    for row in distinct_df.collect():
        value = row[column]
        if value not in seen_values:
            unique_values.append(value)
            seen_values.add(value)
        if len(unique_values) == n:
            break
    return unique_values

# Get top 3 unique ZIP codes by highest income
top_3_income_values = get_distinct_top_bottom(all_data, "Estimated Median Income", desc("Estimated Median Income"))
top_3_zipcodes = crimes_with_income.filter(col("Estimated Median Income").isin(top_3_income_values)).select("zip.ZIPcode").distinct()

# Get bottom 3 unique ZIP codes by lowest income
bottom_3_income_values = get_distinct_top_bottom(all_data, "Estimated Median Income", asc("Estimated Median Income"))
bottom_3_zipcodes = crimes_with_income.filter(col("Estimated Median Income").isin(bottom_3_income_values)).select("zip.ZIPcode").distinct()


# Filtering Crimes for Top and Bottom 3 Zip Codes
top_3_crimes = crimes_with_income.join(top_3_zipcodes, crimes_with_income["ZIPcode"] == top_3_zipcodes["ZIPcode"], "inner")
bottom_3_crimes = crimes_with_income.join(bottom_3_zipcodes, crimes_with_income["ZIPcode"] == bottom_3_zipcodes["ZIPcode"], "inner")

# Grouping and Sorting by Victim Descent
top_3_victims = top_3_crimes.groupBy("Vict Descent").count().orderBy(desc("count"))
bottom_3_victims = bottom_3_crimes.groupBy("Vict Descent").count().orderBy(desc("count"))

# Mapping Initials to Descent
descent_dict = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

# Map Function
def map_descent(descent_code):
    return descent_dict.get(descent_code, descent_code)

# Applying Mapping
map_descent_udf = spark.udf.register("mapDescent", map_descent, StringType())
top_3_victims = top_3_victims.withColumn("victim descent", map_descent_udf(col("Vict Descent"))).drop("Vict Descent")
bottom_3_victims = bottom_3_victims.withColumn("victim descent", map_descent_udf(col("Vict Descent"))).drop("Vict Descent")

# Creating new DataFrames with correct format
top_3_victims_df = top_3_victims.select(col("victim descent"), col("count").alias("total victims"))
bottom_3_victims_df = bottom_3_victims.select(col("victim descent"), col("count").alias("total victims"))

# Displaying Results
print("Top 3 ZIP Codes with highest income:")
top_3_victims_df.show(truncate=False)

print("Top 3 ZIP Codes with lowest income:")
bottom_3_victims_df.show(truncate=False)

# Terminating SparkSession
spark.stop()
