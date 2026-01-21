import logging
import json
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def process_message(message: dict):
    logging.info(f"[PLACEHOLDER] Processing message: {message}")

def save_to_mongodb(data: dict):
    logging.info(f"[PLACEHOLDER] Saving to MongoDB: {data}")

def main():
    logging.info("Consumer service started (placeholder mode)")
    while True:
        # Simulate waiting for Kafka messages
        time.sleep(5)
        fake_message = {"example": "data"}
        process_message(fake_message)
        save_to_mongodb(fake_message)

if __name__ == "__main__":
    main()
