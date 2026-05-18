from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    """
    Proof of Concept (PoC) for Spark DataFrames and PostgreSQL interaction.
    """
    
    # ==========================================
    # 0. Initialize SparkSession
    # ==========================================
    # We include the PostgreSQL JDBC driver package here so Spark can download it automatically.
    # Note: If you are using Spark Connect, you can replace .master(...) with .remote("sc://0.0.0.0:15002")
    #       as demonstrated in your spark-interaction.ipynb
    
    spark = SparkSession.builder \
        .appName("SparkPostgresPoC") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("\n--- 1. Creating and Manipulating DataFrames ---\n")
    
    # Create some sample data
    data = [
        (1, "Alice", 28, "Engineering"),
        (2, "Bob", 35, "Sales"),
        (3, "Charlie", 32, "Engineering"),
        (4, "Diana", 29, "Marketing"),
        (5, "Evan", 41, "Sales")
    ]
    columns = ["id", "name", "age", "department"]
    
    # Create the DataFrame
    df = spark.createDataFrame(data, schema=columns)
    print("Original DataFrame:")
    df.show()
    
    # --- Data Manipulation Examples ---
    
    # Example A: Add a new column
    # Using lit() to assign a literal (constant) value to all rows
    df_transformed = df.withColumn("is_active", lit(True))
    
    # Example B: Filter rows based on a condition
    df_engineering = df_transformed.filter(col("department") == "Engineering")
    
    # Example C: Group by and aggregate
    df_grouped = df_transformed.groupBy("department").count()
    
    print("Transformed DataFrame (Added 'is_active' column):")
    df_transformed.show()
    
    print("Filtered DataFrame (Only Engineering):")
    df_engineering.show()
    
    print("Aggregated DataFrame (Count per Department):")
    df_grouped.show()


    # ==========================================
    # PostgreSQL Connection Details
    # ==========================================
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres" # jdbc:postgresql://<host>:<port>/<database>
    connection_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    table_name = "employees_poc"


    print("\n--- 2. Storing DataFrame to PostgreSQL ---\n")
    
    print(f"Writing data to table '{table_name}'...")
    try:
        # Write DataFrame to PostgreSQL
        # mode("overwrite") drops the table if it exists and creates a new one.
        # Other useful modes: "append", "ignore", "errorifexists"
        df_transformed.write \
            .mode("overwrite") \
            .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            
        print(">>> Successfully wrote data to PostgreSQL! <<<")
        
    except Exception as e:
        print(f"\n>>> Error writing to PostgreSQL: <<<\n{e}")
        print("\nNOTE: This is expected if your PostgreSQL server isn't running or credentials are wrong.")
        print("Please update 'jdbc_url', 'user', and 'password' in this script and try again.")
        

    print("\n--- 3. Loading DataFrame from PostgreSQL ---\n")
    
    try:
        # Read DataFrame from PostgreSQL
        df_loaded = spark.read \
            .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            
        print("Data loaded from PostgreSQL:")
        df_loaded.show()
        
        # You can now manipulate the loaded DataFrame just like any other Spark DataFrame
        print("Querying the loaded data (Age > 30):")
        df_loaded.filter(col("age") > 30).show()
        
    except Exception as e:
        print(f"\n>>> Error reading from PostgreSQL: <<<\n{e}")

    # Always stop the Spark session when done
    spark.stop()

if __name__ == "__main__":
    main()
