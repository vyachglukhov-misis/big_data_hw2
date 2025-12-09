import time
import json
import random
from confluent_kafka import Producer
import uuid

conf = {
    'bootstrap.servers': 'localhost:29092', 
}

producer = Producer(**conf)

# Функция обратного вызова для подтверждения доставки сообщения
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Генерация и отправка событий
def generate_events():
    while True:
        event = {
    'transaction_id': str(uuid.uuid4()),                # Уникальный идентификатор транзакции
    'user_id': random.randint(1, 10000),                # Идентификатор пользователя
    'timestamp': int(time.time()),                      # Время транзакции (UNIX timestamp)
    'amount': round(random.uniform(10, 10000), 2),      # Сумма транзакции
    'currency': random.choice(['USD', 'EUR', 'RUB']),   # Валюта транзакции
    'transaction_type': random.choice(['purchase', 'refund', 'withdrawal', 'deposit']),  # Тип транзакции
    'status': random.choice(['success', 'failed', 'pending']),  # Статус транзакции
}

        event_json = json.dumps(event)

        producer.produce('transactions', event_json.encode('utf-8'), callback=delivery_report) # название вашего топика

        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    print(1)
    generate_events()