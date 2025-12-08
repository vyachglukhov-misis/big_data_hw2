import json
import time
import uuid
import random
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "transactions_stream"

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def random_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1_000_000),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(1, 1000), 2),
        "currency": random.choice(["RUB", "USD", "EUR"]),
        "transaction_type": random.choice(["purchase", "refund", "withdrawal", "deposit"]),
        "status": random.choice(["success", "failed", "pending"]),
    }


if __name__ == "__main__":
    print("Starting events generator...")
    try:
        while True:
            evt = random_transaction()
            producer.send(TOPIC, evt)
            producer.flush()
            print("Sent:", evt)
            time.sleep(0.2)  # ~5 событий в секунду
    except KeyboardInterrupt:
        print("Stopping generator...")
    finally:
        producer.close()
