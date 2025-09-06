import os, json, logging
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "sensor-data")
GROUP_ID  = os.getenv("GROUP_ID", "debug-consumer")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("consumer")

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",     # change to "earliest" to read from beginning
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    log.info("Connected to Kafka at %s, topic=%s, group=%s", BOOTSTRAP, TOPIC, GROUP_ID)
    for msg in consumer:
        log.info("Got event key=%s value=%s", msg.key, msg.value)

if __name__ == "__main__":
    main()
