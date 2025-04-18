# test_spark_job.py
from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AirflowTestJob") \
        .getOrCreate()
    
    try:
        # Simple Spark operation - create a DataFrame and show it
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        df = spark.createDataFrame(data, ["Name", "Value"])
        
        print("=== Sample DataFrame ===")
        df.show()
        
        # Count the rows
        count = df.count()
        print(f"Total rows: {count}")
        
        return 0
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    main()