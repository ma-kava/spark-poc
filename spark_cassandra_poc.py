from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    """
    Proof of Concept (PoC) for Spark DataFrames and Cassandra interaction.
    """
    
    # ==========================================
    # 0. Initialize SparkSession
    # ==========================================
    # We include the Cassandra connector package here.
    # Note: Using version 3.4.1 of the connector, which is compatible with Spark 3.4.x.
    #       Adjust the version depending on your Spark installation.
    # Note: If you are using Spark Connect, you can replace .master(...) with .remote("sc://0.0.0.0:15002")
    #       as demonstrated in your spark-interaction.ipynb
    
    spark = SparkSession.builder \
        .appName("SparkCassandraPoC") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .config("spark.sql.catalog.casscatalog.spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.sql.catalog.casscatalog.spark.cassandra.connection.port", "9042") \
        .config("spark.sql.catalog.casscatalog.spark.cassandra.connection.localDC", "DC1") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.localDC", "DC1") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("\n--- 1. Creating and Manipulating DataFrames ---\n")
    
    # Create some sample data matching chip_metadata
    data = [
        ("chip_001", "sensor_A", "tree_v1"),
        ("chip_002", "sensor_A", "tree_v2"),
        ("chip_003", "sensor_B", "tree_v1"),
        ("chip_004", "sensor_C", "tree_v3"),
        ("chip_005", "sensor_B", "tree_v2")
    ]
    columns = ["chip_id", "device_type", "calibration_tree"]
    
    # Create the DataFrame
    df = spark.createDataFrame(data, schema=columns)
    print("Original DataFrame:")
    df.show()
    
    # --- Data Manipulation Examples ---
    
    # Example A: Add a new column
    # Using lit() to assign a literal (constant) value to all rows
    df_transformed = df.withColumn("is_processed", lit(True))
    
    # Example B: Filter rows based on a condition
    df_filtered = df_transformed.filter(col("device_type") == "sensor_A")
    
    # Example C: Group by and aggregate
    df_grouped = df_transformed.groupBy("device_type").count()
    
    print("Transformed DataFrame (Added 'is_processed' column):")
    df_transformed.show()
    
    print("Filtered DataFrame (Only sensor_A):")
    df_filtered.show()
    
    print("Aggregated DataFrame (Count per device_type):")
    df_grouped.show()


    # ==========================================
    # Cassandra Connection Details
    # ==========================================
    keyspace_name = "calibration_ks"
    table_name = "chip_metadata"


    print("\n--- 2. Setting up Cassandra Keyspace and Table ---\n")
    
    try:
        # Create a Keyspace using the Cassandra Catalog in Spark SQL
        spark.sql(f"CREATE DATABASE IF NOT EXISTS casscatalog.{keyspace_name} WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1')")
        print(f">>> Ensured keyspace '{keyspace_name}' exists! <<<")
        
        # Create Table using Spark SQL. 
        # PARTITIONED BY (chip_id, device_type) translates to the Cassandra Primary Key
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS casscatalog.{keyspace_name}.{table_name} (
                chip_id STRING,
                device_type STRING,
                calibration_tree STRING,
                is_processed BOOLEAN
            ) USING cassandra PARTITIONED BY (chip_id, device_type)
        """
        spark.sql(create_table_query)
        print(f">>> Ensured table '{table_name}' exists! <<<")

    except Exception as e:
        print(f"\n>>> Error creating keyspace/table in Cassandra: <<<\n{e}")
        print("\nNOTE: Ensure your Cassandra container is running and accessible on 127.0.0.1:9042.")


    print("\n--- 3. Storing DataFrame to Cassandra ---\n")
    
    print(f"Writing data to table '{keyspace_name}.{table_name}'...")
    try:
        # Write DataFrame to Cassandra using the catalog
        # Mode 'append' is typical for Cassandra, acting as an upsert based on the primary key
        df_transformed.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table_name, keyspace=keyspace_name) \
            .save()
            
        print(">>> Successfully wrote data to Cassandra! <<<")
        
    except Exception as e:
        print(f"\n>>> Error writing to Cassandra: <<<\n{e}")
        
    print("\n--- 4. Loading DataFrame from Cassandra ---\n")
    
    try:
        # Read DataFrame from Cassandra
        df_loaded = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=keyspace_name) \
            .load()
            
        print("Data loaded from Cassandra:")
        df_loaded.show()
        
        # You can now manipulate the loaded DataFrame just like any other Spark DataFrame
        print("Querying the loaded data (device_type = 'sensor_B'):")
        df_loaded.filter(col("device_type") == "sensor_B").show()
        
        # Cassandra-specific: Spark pushes down filters to Cassandra when possible.
        # Queries on the partition key are very fast.
        print("Querying by Partition Key (chip_id = 'chip_003'):")
        df_loaded.filter(col("chip_id") == "chip_003").show()
        
    except Exception as e:
        print(f"\n>>> Error reading from Cassandra: <<<\n{e}")

    # Always stop the Spark session when done
    spark.stop()

if __name__ == "__main__":
    main()
