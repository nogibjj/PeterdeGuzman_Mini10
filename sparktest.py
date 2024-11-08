from pyspark import SparkContext
from pyspark.sql import SparkSession

# Stop any active SparkContext
if SparkContext._active_spark_context is not None:
    SparkContext._active_spark_context.stop()

# Initialize the Spark session with appropriate configurations
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.security.manager.enabled", "false") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Set log level to ERROR for more clarity
spark.sparkContext.setLogLevel("ERROR")

print("Spark session created successfully!")
