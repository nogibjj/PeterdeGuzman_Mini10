"""
library functions
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def end_spark(spark):
    spark.stop()
    return "stopped spark session"


# maybe update to extract rugby data from kagglehub


def extract(
    url="""
   https://github.com/fivethirtyeight/data/blob/master/daily-show-guests/daily_show_guests.csv?raw=true 
    """,
    file_path="data/serve_times.csv",
    directory="data",
):
    """Extract data"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    return file_path


def load_data(spark, data="data/serve_times.csv", name="DailyShowGuests"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType(
        [
            StructField("YEAR", IntegerType(), True),
            StructField("GoogleKnowlege_Occupation", StringType(), True),
            StructField("Show", DateType(), True),
            StructField("Group", StringType(), True),
            StructField("Raw_Guest_List", StringType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name):
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()


def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()


def example_transform(df):
    """does an example transformation on a predefiend dataset"""
    conditions = [
        (col("GoogleKnowlege_Occupation") == "actor")
        | (col("GoogleKnowlege_Occupation") == "actress"),
        (col("GoogleKnowlege_Occupation") == "comedian")
        | (col("GoogleKnowlege_Occupation") == "comic"),
    ]

    categories = ["Acting", "Comedy"]

    df = df.withColumn(
        "Occupation_Category",
        when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .otherwise("Other"),
    )

    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df.show()
