from datetime import datetime, date
from pyspark.sql import Row, SparkSession

def main():
    # Connect to the Spark Connect server (Default port is 15002)
    # SparkSession.builder.master("local[*]").getOrCreate().stop()
    spark = (
        SparkSession.builder
            .remote("sc://0.0.0.0:15002")
            .appName("Testing a Session")
            .getOrCreate()
    )

    df = spark.range(100)
    df.write.mode("overwrite").parquet("./data/example")
    spark.stop()

if __name__ == "__main__":
    main()
