# test_spark_job.py
from pyspark.sql import SparkSession
from datetime import datetime
import pytz

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AirflowTestJob") \
        .getOrCreate()
    
    try:
        # Simple Spark operation - create a DataFrame and show it
        # data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        # df = spark.sql("select * from spark_catalog.default.brz_films")
        
        # print("=== Sample DataFrame ===")
        # df.printSchema()
        # df.show()
        
        # Count the rows
        # count = df.count()
        # print(f"Total rows: {count}")
        print(f"=========={datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime('%Y-%m-%d %H:%M:%s')}")
        return 0
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    main()