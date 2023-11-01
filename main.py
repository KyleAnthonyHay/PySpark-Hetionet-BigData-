from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count

# Create a Spark session
spark = SparkSession.builder.appName("drug_associations").getOrCreate()

# # Read the CSV file into a DataFrame
nodes = spark.read.csv('nodes_test.csv', header=True)  # store nodes csv
edges = spark.read.csv('edges_test.csv', header=True)  # store nodes csv

# # Show the DataFrame
for row in nodes.take(4):
    print(row)
