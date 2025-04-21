# example_data_processing.py
import pyspark

# Initialize the Spark session
spark = pyspark.sql.SparkSession.builder.appName(
    "Example Data Processing"
).getOrCreate()

# Example: Dynamic file paths using variables and f-strings
month = "march"
year = 2024
file_path = f"/data/{year}/{month}_sales.csv"

# Read CSV file
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Example: Filter data for a specific region using SQL and DataFrame API
region = "US"
df_filtered = df.filter(df["region"] == region)

# Create a temporary SQL view
df_filtered.createOrReplaceTempView("sales_data")

# SQL query to aggregate sales data
sql_query = """
SELECT 
    product_id, 
    SUM(sales_amount) AS total_sales
FROM 
    sales_data
GROUP BY 
    product_id
"""
df_sql = spark.sql(sql_query)

# Example: Using ternary expression to set output path
output_path = (
    f"/data/{year}/processed_sales" if region == "US" else "/data/global_sales"
)

# Write the result to CSV with dynamic path using variables and string formatting
df_sql.write.mode("overwrite").csv(output_path)

# Example: Using string formatting for table names in Spark SQL
table_name = f"sales_{year}_{month}"
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA AS SELECT * FROM sales_data"
)

# Example: Dynamic path with multiple concatenations
base_path = "/mnt/data"
final_path = base_path + f"/sales_{year}_{month}_summary"

# Write summary to a different format (Parquet)
df_sql.write.mode("overwrite").parquet(final_path)

# Example of handling a list of tables and performing actions on them
tables = ["sales_q1", "sales_q2", "sales_q3"]
for table in tables:
    query = f"SELECT * FROM {table} WHERE region = '{region}'"
    df_table = spark.sql(query)
    df_table.show()

# Using f-string to combine paths dynamically
dynamic_path = f"/tmp/processed/{year}/{month}/{region}_summary.parquet"
df_sql.write.mode("overwrite").parquet(dynamic_path)

output_type = "json"
output_dir = f"/data/{year}/{month}/{output_type}"

# Write output in the desired format (either JSON or Parquet)
if output_type == "json":
    df_sql.write.mode("overwrite").json(output_dir)
else:
    df_sql.write.mode("overwrite").parquet(output_dir)

# Example of using the `join` method in string formatting
file_prefix = "sales_report"
report_name = f"{file_prefix}_{region}_{month}_{year}"

print(f"Report generated at {report_name}")
