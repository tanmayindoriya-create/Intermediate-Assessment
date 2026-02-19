import json
import random
from datetime import datetime, timedelta

services = ["auth","order","payment","inventory"]
records = []
start = datetime(2026,1,1)

for i in range(100000):
    ts = start + timedelta(seconds=i)

    status = random.choices(
        [200,201,400,500,503],
        weights=[60,15,5,15,5]
    )[0]

    records.append({
        "timestamp": ts.isoformat(),
        "service": random.choice(services),
        "status_code": status,
        "response_time_ms": random.randint(50,2000)
    })

with open("data/logs/app_logs.json","w") as f:
    json.dump(records,f)

print("Generated 100K logs.")
