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
# Filter edges to get only those that are disease-related (assuming that 'Disease::' prefix is used for disease-related edges)
disease_edges = edges.filter(edges["target"].startswith("Disease::"))

# Count how many drugs are associated with each disease
disease_drug_count = disease_edges.groupBy("target") \
                                  .agg(count("source").alias("num_drugs"))

# Now count how many diseases have the same number of associated drugs
disease_count_per_drug_number = disease_drug_count.groupBy("num_drugs") \
                                                  .agg(count("target").alias("num_diseases"))

# Get the top 5 disease counts in descending order
top_disease_counts = disease_count_per_drug_number.orderBy(
    desc("num_diseases")).limit(5)

# Show the results
top_disease_counts.show()

# -------------- Display the result Quesiton 3 --------------
top_drugs = sorted_result.orderBy("Number_of_Genes", ascending=False).limit(5)
top_drugs.select("id").show()


spark.stop()  # Stop the Spark session
