from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_date

spark = SparkSession.builder \
    .appName("Partition_Transactions") \
    .getOrCreate()

# Step 2.1: Read raw CSV
df_txn = spark.read.csv("/data/raw/transactions/transactions.csv", header=True, inferSchema=True)

# Step 2.2: Convert transaction_date to proper date
df_txn = df_txn.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

# Step 2.3: Extract partition columns
df_txn = df_txn.withColumn("year", year(col("transaction_date"))) \
               .withColumn("month", month(col("transaction_date"))) \
               .withColumn("day", dayofmonth(col("transaction_date")))

# Step 2.4: Write to HDFS partitioned by year/month/day
df_txn.write.partitionBy("year","month","day") \
            .mode("overwrite") \
            .parquet("/data/raw/transactions_partitioned/")

spark.stop()

