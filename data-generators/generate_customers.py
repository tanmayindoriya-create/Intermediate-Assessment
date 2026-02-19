import pandas as pd
import random
from datetime import datetime, timedelta

N = 50000
regions = ["North", "South", "East", "West", "Central"]

data = []
start = datetime(2024, 1, 1)

for i in range(1, N + 1):
    signup = start + timedelta(days=random.randint(0, 700))

    # Initial record
    data.append([
        i,
        f"Customer{i}",
        random.choice(regions),
        signup.strftime("%Y-%m-%d"),
        signup.strftime("%Y-%m-%d")  # event timestamp
    ])

# Simulate region change for 10% customers
updates = random.sample(range(1, N + 1), int(N * 0.1))

for customer_id in updates:
    change_date = datetime(2026, 1, 1)

    data.append([
        customer_id,
        f"Customer{customer_id}",
        random.choice(regions),
        None,  # signup_date not changed
        change_date.strftime("%Y-%m-%d")
    ])

df = pd.DataFrame(data, columns=[
    "customer_id",
    "name",
    "region",
    "signup_date",
    "event_ts"
])

df.to_csv("data/raw/customers.csv", index=False)

print("Generated customers with change events (no SCD columns in raw).")
