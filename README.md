# Banking Data Warehouse ETL Project

## Overview
This project implements a **simple full-load banking data warehouse ETL pipeline concept** using Python,Hadoop, PySpark, Airflow and PostgreSQL.  
 
The pipeline is designed for **batch processing** (500 records per batch) and is structured to support future expansions, including real-time streaming with Kafka.  

### Key Features
- Generate synthetic banking data  
- Transform and clean data using Spark  
- Load transformed data into PostgreSQL  
- Full-load batch processing (500 records per batch)  
- Automated ETL orchestration via Airflow DAG  
- Manual execution support for each ETL task  

---

## Folder Setup
Before running the project, create the following directories in the project root:  
/data # Place raw input files here
/log # Store ETL process logs here
## Make sure you have installed pyspark if no 
```bash
sudo apt update
sudo apt upgrade
sudo apt install pyspark
````
## Also install JDBC for spark to communicate with your database
```bash
wget https://jdbc.postgresql.org/download/postgresql-42.7.8.jar
sudo mkdir -p /usr/local/spark/jars/
sudo mv postgresql-42.7.8.jar /usr/local/spark/jars/
````

## Option 1: Run via Airflow DAG (Automated)
To automate the ETL pipeline using Airflow:  
1. Install and configure Airflow if not already installed.  
2. Copy the DAG file (`banking_etl.py`) to your Airflow `dags/` folder.  
3. Initialize the Airflow database:
```bash
airflow db init
airflow scheduler
airflow webserver
````
## Option 2: Run ETL manually (best case practise)
1.create folder data and log
```bash
mkdir -p data log
````
2. run python script to generate data and put in data folder
```bash
python3 scrpts/banking_data.py 
````
3. create a directory in hadoop and copy theem into those directory
```base
# Create directories
hdfs dfs -mkdir -p /data/raw/customers
hdfs dfs -mkdir -p /data/raw/accounts
hdfs dfs -mkdir -p /data/raw/loans
hdfs dfs -mkdir -p /data/raw/transactions

# Copy CSVs
hdfs dfs -put banking_datawarehouse_project/data/customers.csv /data/raw/customers/
hdfs dfs -put banking_datawarehouse_project/data/accounts.csv /data/raw/accounts/
hdfs dfs -put banking_datawarehouse_project/data/loans.csv /data/raw/loans/
hdfs dfs -put banking_datawarehouse_project/data/transactions.csv /data/raw/transactions/
````
4. run the partition transactios split the transactions
```base
spark-submit \
  --jars /usr/local/spark/jars/postgresql-42.7.8.jar \
  partition_transactions.py
````

5. run the ETL spark
```base
spark-submit \
  --jars /usr/local/spark/jars/postgresql-42.7.8.jar \
  etl_spark.py
````
6. load to postgres
```base
spark-submit \
  --jars /usr/local/spark/jars/postgresql-42.7.8.jar \
  load_postgres.py
````
7. finalize you can create view in postgres and set permission to specific role for specific task




