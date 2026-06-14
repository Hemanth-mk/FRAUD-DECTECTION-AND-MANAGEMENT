"""Synthetic transaction producer -> Kafka topic 'transactions'."""
import json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

MERCHANTS = ["Amazon","Uber","Steam","Apple","Walmart","Shell","Netflix","Airbnb"]
COUNTRIES = ["US","GB","DE","IN","BR","NG","RU","JP","FR"]
DEVICES   = ["ios","android","web","pos"]
USERS     = [f"u_{i:04d}" for i in range(500)]

def make_tx():
    fraudish = random.random() < 0.05
    return {
        "tx_id":    str(uuid.uuid4()),
        "user_id":  random.choice(USERS),
        "amount":   round(random.uniform(2000, 9000) if fraudish else random.uniform(1, 400), 2),
        "merchant": random.choice(MERCHANTS),
        "country":  random.choice(COUNTRIES if fraudish else ["US","GB","DE"]),
        "device":   random.choice(DEVICES),
        "ts":       datetime.now(timezone.utc).isoformat(),
    }

def main():
    p = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20,
    )
    print("streaming -> transactions")
    while True:
        tx = make_tx()
        p.send("transactions", tx)
        print(tx["tx_id"], tx["amount"], tx["country"])
        time.sleep(random.uniform(0.1, 0.6))

if __name__ == "__main__":
    main()
