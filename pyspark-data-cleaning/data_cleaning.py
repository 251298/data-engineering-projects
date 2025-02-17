from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace

# Initialize Spark Session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("data/sample_data.csv", header=True, inferSchema=True)

# Data Cleaning Steps

# 1. Remove Duplicates
df = df.dropDuplicates()

# 2. Handle Missing Values
df = df.fillna({
    "rating": 0,  # Default rating
    "payment_method": "Unknown",  # Default payment method
    "driver_name": "Unknown",  # Default driver name
    "trip_date": "2024-01-01",  # Default trip date
})

# 3. Remove Invalid Fare Amounts
df = df.withColumn("fare_amount", when(col("fare_amount") < 0, 0).otherwise(col("fare_amount")))

# 4. Clean Driver Names (Remove extra spaces or special characters)
df = df.withColumn("driver_name", trim(regexp_replace(col("driver_name"), "[^a-zA-Z ]", "")))

# 5. Convert trip_date to Proper Format (yyyy-MM-dd)
df = df.withColumn("trip_date", col("trip_date").cast("date"))

# 6. Standardize Payment Methods (Replacing NULL or inconsistent values)
df = df.withColumn("payment_method", when(col("payment_method").isNull(), "Cash").otherwise(col("payment_method")))

# Show cleaned data
df.show()

# Save the cleaned data (you can also store it in Parquet, CSV, or any format)
df.write.csv("data/cleaned_data.csv", header=True, mode="overwrite")

# Stop Spark session
spark.stop()
