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


# Глобальные переменные для подсчёта среднего
_running_sum = 0.0
_running_count = 0

def random_transaction():
    global _running_sum, _running_count

    # 1. Сначала сгенерим «нормальную» сумму
    base_amount = round(random.uniform(1, 1000), 2)

    # 2. Обновим наше среднее (по нормальным значениям)
    _running_sum += base_amount
    _running_count += 1
    current_avg = _running_sum / _running_count if _running_count > 0 else base_amount

    # 3. С шансом 10% делаем аномалию: 2–3 раза больше текущего среднего
    if random.random() < 0.10:
        amount = round(random.uniform(2 * current_avg, 3 * current_avg), 2)
    else:
        amount = base_amount

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1_000_000),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
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
