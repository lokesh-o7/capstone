# Find subscribers under 30 who subscribe to any subgroup
from pyspark.sql.functions import current_date, datediff

# Calculate age and filter subscribers
subscriber_df = subscriber_df.withColumn("age", (datediff(current_date(), col("Birth_date")) / 365).cast("integer"))
subscribers_under_30_df = subscriber_df.filter((col("age") < 30) & (col("Subgrp_id").isNotNull()))

# Save the result back to Redshift
subscribers_under_30_df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://your-redshift-cluster:5439/yourdbname") \
    .option("dbtable", "subscribers_under_30") \
    .option("user", "yourusername") \
    .option("password", "yourpassword") \
    .save()