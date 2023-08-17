from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ShowTablesQuery").enableHiveSupport().getOrCreate()

# Run the "show tables" query
hive_database = "your_database_name"
query = "show tables"
tables = spark.sql(f"USE {hive_database}; {query}")
tables.show()

# Stop the Spark session
spark.stop()
