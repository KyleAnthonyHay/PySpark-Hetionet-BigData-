from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count
from pyspark.sql.functions import col, count, desc

# Create a Spark session
spark = SparkSession.builder.appName("drug_associations").getOrCreate()

# # Read the CSV file into a DataFrame
nodes = spark.read.csv('nodes_test.csv', header=True)  # store nodes csv
edges = spark.read.csv('edges_test.csv', header=True)  # store nodes csv

# -------------- Display the result Quesiton 1 --------------

# Filter nodes to get only the ones that are of type "Compound" (drugs)
drugs_df = nodes.filter(nodes["kind"] == "Compound")

# Filter edges where the target node is either a Gene or a Disease
filtered_edges = edges.filter(
    (edges.target.startswith("Gene::")) | (
        edges.target.startswith("Disease::"))
)

# Left join drugs DataFrame with filtered_edges on drug IDs
# and then count the number of genes and diseases associated with each drug
result = drugs_df.join(filtered_edges, drugs_df["id"] == filtered_edges["source"], "left_outer") \
                 .groupBy(drugs_df["id"]) \
                 .agg(
                     count(when(filtered_edges.target.startswith(
                         "Gene::"), 1)).alias("Number_of_Genes"),
                     count(when(filtered_edges.target.startswith("Disease::"), 1)).alias(
                         "Number_of_Diseases")
)

# Sort the drugs based on their associated gene count in descending order
sorted_result = result.orderBy("Number_of_Genes", ascending=False)
sorted_result.show(5)  # Display the top 5 results

# -------------- Display the result Quesiton 2 --------------

# Filter edges for relationships where compounds are associated with diseases
compound_disease_edges = edges.filter(
    # This may need to be adjusted based on your actual metaedge codes
    edges["metaedge"] == "CtD"
)

# Group by the compound and count the number of diseases
compound_disease_count = compound_disease_edges.groupBy("source") \
                                               .agg(count("target").alias("Number_of_Diseases"))

# Order by the count of diseases and take the top 5
top_compounds = compound_disease_count.orderBy(
    col("Number_of_Diseases").desc()).limit(5)

# Show the results
top_compounds.show()

# -------------- Display the result Quesiton 3 --------------
result2 = drugs_df.join(filtered_edges, drugs_df["id"] == filtered_edges["source"], "left_outer") \
    .groupBy(drugs_df["name"]) \
    .agg(
    count(when(filtered_edges.target.startswith(
        "Gene::"), 1)).alias("Number_of_Genes"),
    count(when(filtered_edges.target.startswith("Disease::"), 1)).alias(
        "Number_of_Diseases")
)
top_drugs = result2.orderBy("Number_of_Genes", ascending=False).limit(5)
top_drugs.select("name").show()


spark.stop()  # Stop the Spark session
