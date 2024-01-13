import os
import sys
from pyspark.sql import SparkSession

def execute_sql_queries(spark, sql_directory):
    sql_files = [f for f in os.listdir(sql_directory) if f.endswith(".sql")]

    for sql_file in sql_files:
        with open(os.path.join(sql_directory, sql_file), "r") as f:
            query = f.read()
            result_df = spark.sql(query)
            result_df.write.csv(f"{sql_file}.csv", header=True)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python exportDataFromHdpToLocalOnCsv.py <sql_directory>")
        sys.exit(1)

    sql_directory = sys.argv[1]

    spark = SparkSession.builder.appName("ExportDataFromHdpToLocal").getOrCreate()

    try:
        execute_sql_queries(spark, sql_directory)
    finally:
        spark.stop()