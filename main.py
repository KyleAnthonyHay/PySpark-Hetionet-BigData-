from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count

# Create a Spark session
spark = SparkSession.builder.appName("drug_associations").getOrCreate()

# # Read the CSV file into a DataFrame
nodes = spark.read.csv('nodes_test.csv', header=True)  # store nodes csv
edges = spark.read.csv('edges_test.csv', header=True)  # store nodes csv

# # Show the DataFrame
# nodes.take(4)
# edges.take(4)


# Display the result Quesiton 1

# Filter rows where source is a Compound and target is either Gene or Disease
filtered_edges = edges.filter(
    (edges.source.startswith("Compound::")) &
    ((edges.target.startswith("Gene::")) | (edges.target.startswith("Disease::")))
)

# Compute number of genes and diseases associated with each drug
result = filtered_edges.groupBy("source").agg(

    count(when(filtered_edges.target.startswith(
        "Gene::"), 1)).alias("Number_of_Genes"),

    count(when(filtered_edges.target.startswith("Disease::"), 1)).alias(
        "Number_of_Diseases")
)

sorted_result = result.orderBy("Number_of_Genes", ascending=False)
sorted_result.show(5)


# Question 2
# Filter for drugs from nodes
drugs_df = nodes.filter(nodes["kind"] == "Compound")
# Filter for drug-disease relationships from edges
drug_disease_edge_df = edges.filter(edges["metaedge"] == "CdD")

# Join on drug IDs and count diseases for each drug
result_df = drugs_df.join(drug_disease_edge_df, drugs_df["id"] == drug_disease_edge_df["source"]) \
                    .groupBy(drugs_df["name"]) \
                    .agg(count(drug_disease_edge_df["target"]).alias("disease_count"))

sorted_result_df = result_df.orderBy("disease_count", ascending=False)
sorted_result_df.show(5)


# Question 3
sorted_result = result.orderBy("Number_of_Genes", ascending=False)
sorted_result.select("source", "Number_of_Genes").show(5)
