import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "broker:29092"
TOPIC = "pipeline-events"

symbols = {
    "AAPL": 228.10,
    "AMZN": 182.50,
    "MSFT": 419.35,
    "NVDA": 125.40,
    "TSLA": 241.75,
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Producing stock events to '{TOPIC}'...")

try:
    while True:
        symbol = random.choice(list(symbols))
        change = random.uniform(-1.50, 1.50)
        symbols[symbol] = round(max(0.01, symbols[symbol] + change), 2)

        event = {
            "symbol": symbol,
            "price": symbols[symbol],
            "source": "simulated-producer",
            "event_time": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(TOPIC, value=event)
        producer.flush()

        print(event)
        time.sleep(2)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.close()