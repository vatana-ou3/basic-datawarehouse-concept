from faker import Faker
import pandas as pd
import numpy as np
import os
from datetime import date

fake = Faker()
os.makedirs("data", exist_ok=True)

# ---------------- Customers ----------------
customers = []
for i in range(70000):
    dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
    customers.append({
        "customer_id": i + 1,
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "dob": dob,
        "job_status": np.random.choice(
            ["EMPLOYED", "SELF_EMPLOYED", "STUDENT", "UNEMPLOYED"],
            p=[0.55, 0.2, 0.15, 0.1]
        ),
        "created_at": fake.date_between(start_date="-5y", end_date="today")
    })

df_customers = pd.DataFrame(customers)
df_customers.to_csv("data/customers.csv", index=False)

# ---------------- Accounts ----------------
accounts = []
for i in range(105000):
    accounts.append({
        "account_id": i + 1,
        "customer_id": np.random.randint(1, 70000),
        "account_type": np.random.choice(["SAVINGS", "CHECKING"]),
        "balance": round(np.random.uniform(100, 500000), 2),
        "created_at": fake.date_between(start_date="-5y", end_date="today")
    })

df_accounts = pd.DataFrame(accounts)
df_accounts.to_csv("data/accounts.csv", index=False)

# ---------------- Loans ----------------
loans = []
for i in range(30000):
    loans.append({
        "loan_id": i + 1,
        "customer_id": np.random.randint(1, 70000),
        "loan_amount": round(np.random.uniform(1000, 50000), 2),
        "interest_rate": round(np.random.uniform(5, 18), 2),
        "start_date": fake.date_between(start_date="-3y", end_date="today")
    })

df_loans = pd.DataFrame(loans)
df_loans.to_csv("data/loans.csv", index=False)

# ---------------- Transactions ----------------
transactions = []
for i in range(2000000):
    category = np.random.choice(
        ["PHONE_TOPUP", "LOAN_DISBURSEMENT", "LOAN_REPAYMENT",
         "TRANSFER", "CASH_WITHDRAWAL"]
    )

    txn_type = "CREDIT" if category in ["LOAN_DISBURSEMENT", "TRANSFER"] else "DEBIT"

    transactions.append({
        "transaction_id": i + 1,
        "account_id": np.random.randint(1, 105000),
        "transaction_type": txn_type,
        "transaction_category": category,
        "amount": round(np.random.uniform(5, 3000), 2),
        "transaction_date": fake.date_between(start_date="-2y", end_date="today")
    })

df_transactions = pd.DataFrame(transactions)
df_transactions.to_csv("data/transactions.csv", index=False)

print("Realistic synthetic banking data generated successfully.")

