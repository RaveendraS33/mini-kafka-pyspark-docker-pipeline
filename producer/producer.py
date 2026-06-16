import random
from datetime import datetime, timezone

from faker import Faker


fake = Faker()


def create_good_record(record_id):
    return {
        "transaction_id": f"TXN{record_id:04d}",
        "user_id": random.randint(1000, 9999),
        "name": fake.name(),
        "email": fake.email(),
        "amount": round(random.uniform(5, 500), 2),
        "currency": "USD",
        "city": fake.city(),
        "device": random.choice(["mobile", "web", "tablet"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(["SUCCESS", "FAILED", "PENDING"]),
    }


def add_bad_values(record):
    bad_field = random.choice(["transaction_id", "user_id", "email", "amount", "event_time"])

    if bad_field in ["transaction_id", "user_id", "event_time"]:
        record[bad_field] = None
    elif bad_field == "email":
        record[bad_field] = "invalid_email"
    elif bad_field == "amount":
        record[bad_field] = -50

    return record
