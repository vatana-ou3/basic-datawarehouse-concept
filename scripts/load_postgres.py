from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Load_Postgres") \
    .getOrCreate()

# PostgreSQL connection
postgres_url = "jdbc:postgresql://localhost:5433/banking_warehouse"
connection_properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

# ------------------ Load processed data ------------------
df_dim_customer = spark.read.parquet("/data/processed/dim_customer/")
df_dim_account = spark.read.parquet("/data/processed/dim_account/")
df_dim_loan = spark.read.parquet("/data/processed/dim_loan/")
df_fact_txn = spark.read.parquet("/data/processed/fact_transaction/")

# ------------------ Write to PostgreSQL ------------------
df_dim_customer.write.jdbc(
    url=postgres_url,
    table="dim_customer",
    mode="overwrite",
    properties=connection_properties
)

df_dim_account.write.jdbc(
    url=postgres_url,
    table="dim_account",
    mode="overwrite",
    properties=connection_properties
)

df_dim_loan.write.jdbc(
    url=postgres_url,
    table="dim_loan",
    mode="overwrite",
    properties=connection_properties
)

df_fact_txn.write.jdbc(
    url=postgres_url,
    table="fact_transaction",
    mode="overwrite",
    properties={**connection_properties, "batchsize": "500"}  # increase for large dataset
)
spark.stop()

