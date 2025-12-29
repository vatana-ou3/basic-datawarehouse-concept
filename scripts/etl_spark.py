from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, year, month, dayofmonth, current_date, expr, when

spark = SparkSession.builder \
    .appName("Banking_ETL_Realistic") \
    .getOrCreate()

# ---------------- DIM CUSTOMER ----------------
df_customers = spark.read.csv("/data/raw/customers/customers.csv", header=True, inferSchema=True)

# Convert dates
df_customers = df_customers.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd")) \
                           .withColumn("customer_created_at", to_date(col("created_at"), "yyyy-MM-dd"))

# Derived columns
df_dim_customer = df_customers.select(
    col("customer_id"),
    col("name"),
    col("email"),
    col("phone"),
    col("dob"),
    col("job_status"),
    col("customer_created_at")
).withColumn("age", year(current_date()) - year(col("dob"))) \
 .withColumn("is_adult", (col("age") >= 18).cast("int"))  # sanity check

# Write dimension table
df_dim_customer.write.mode("overwrite").parquet("/data/processed/dim_customer/")

# ---------------- DIM ACCOUNT ----------------
df_accounts = spark.read.csv("/data/raw/accounts/accounts.csv", header=True, inferSchema=True)

df_accounts = df_accounts.withColumn("account_created_at", to_date(col("created_at"), "yyyy-MM-dd"))

# Derived columns & cleaning
df_dim_account = df_accounts.select(
    col("account_id"),
    col("customer_id"),
    col("account_type"),
    col("balance"),
    col("account_created_at")
).withColumn("low_balance_flag", (col("balance") < 500).cast("int")) \
 .withColumn("balance", when(col("balance") < 0, 0).otherwise(col("balance")))  # no negative balance

# Write dimension table
df_dim_account.write.mode("overwrite").parquet("/data/processed/dim_account/")

# ---------------- DIM LOAN ----------------
df_loans = spark.read.csv("/data/raw/loans/loans.csv", header=True, inferSchema=True)

df_loans = df_loans.withColumn("loan_start_date", to_date(col("start_date"), "yyyy-MM-dd"))

# Derived columns
df_dim_loan = df_loans.select(
    col("loan_id"),
    col("customer_id"),
    col("loan_amount"),
    col("interest_rate"),
    col("loan_start_date")
).withColumn("loan_end_date", expr("loan_start_date + interval 3 years")) \
 .withColumn("monthly_payment", col("loan_amount") * (1 + col("interest_rate")/100)/36)  # simplified

# Write dimension table
df_dim_loan.write.mode("overwrite").parquet("/data/processed/dim_loan/")

# ---------------- FACT TRANSACTION ----------------
df_txn = spark.read.parquet("/data/raw/transactions_partitioned/")  # partitioned

# Convert date
df_txn = df_txn.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

# Sanity checks & derived columns
df_fact_txn = df_txn.select(
    col("transaction_id"),
    col("account_id"),
    col("transaction_type"),
    col("transaction_category"),
    col("amount"),
    col("transaction_date")
).withColumn("year", year(col("transaction_date"))) \
 .withColumn("month", month(col("transaction_date"))) \
 .withColumn("day", dayofmonth(col("transaction_date"))) \
 .withColumn("is_large_txn", (col("amount") > 1000).cast("int")) \
 .withColumn("amount", when(col("amount") < 0, 0).otherwise(col("amount")))  # no negative txns

# Write fact table partitioned
df_fact_txn.write.partitionBy("year","month","day").mode("overwrite") \
    .parquet("/data/processed/fact_transaction/")

spark.stop()







