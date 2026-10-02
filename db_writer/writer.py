import os
import json
import time
import logging

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud")
}

INSERT_QUERY = """
    INSERT INTO scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def connect_db(retries=30, delay=2):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database is not ready ({attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to the database")


class DbWriterService:
    def __init__(self):
        self.conn = connect_db()
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'db-writer',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([SCORING_TOPIC])

    def save(self, record):
        with self.conn.cursor() as cur:
            cur.execute(
                INSERT_QUERY,
                (
                    record['transaction_id'],
                    float(record['score']),
                    int(record['fraud_flag'])
                )
            )

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                records = data if isinstance(data, list) else [data]
                for record in records:
                    self.save(record)
            except psycopg2.Error as e:
                logger.error(f"Database error: {e}")
                self.conn = connect_db()
            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    logger.info('Starting scores writer service...')
    service = DbWriterService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
