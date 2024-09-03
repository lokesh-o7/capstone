from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnull

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HealthCareDataCleaning") \
    .getOrCreate()

# Load datasets from S3
claims_df = spark.read.json("s3://input-data-capstone/input-data/claims.json")
disease_df = spark.read.csv("s3://input-data-capstone/input-data/disease.csv", header=True, inferSchema=True)
subscriber_df = spark.read.csv("s3://input-data-capstone/input-data/subscriber.csv", header=True, inferSchema=True)
grpsubgrp_df = spark.read.csv("s3://input-data-capstone/input-data/grpsubgrp.csv", header=True, inferSchema=True)

# Check for null values and replace with 'NA'
cleaned_subscriber_df = subscriber_df.fillna('NA')

# Remove duplicates
cleaned_subscriber_df = cleaned_subscriber_df.dropDuplicates()

# Save cleaned data to Redshift
cleaned_subscriber_df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://your-redshift-cluster:5439/yourdbname") \
    .option("dbtable", "subscriber_cleaned") \
    .option("user", "yourusername") \
    .option("password", "yourpassword") \
    .save()

# Determine which disease has the maximum number of claims
max_claim_disease_df = claims_df.groupBy("disease_name").count().orderBy(col("count").desc()).limit(1)
max_claim_disease_df.show()

# Save the result back to Redshift
max_claim_disease_df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://your-redshift-cluster:5439/yourdbname") \
    .option("dbtable", "max_claim_disease") \
    .option("user", "yourusername") \
    .option("password", "yourpassword") \
    .save()