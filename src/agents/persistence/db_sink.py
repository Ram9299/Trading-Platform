import json
import logging
import signal
import sys
import os
import psycopg2
from psycopg2.extras import execute_batch
from confluent_kafka import Consumer, KafkaError

from src.core.schemas import MarketTick, AgentSignal, ExecutionOrder

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [DBSinkAgent] - %(levelname)s - %(message)s")
logger = logging.getLogger("DBSink")

class DBSinkAgent:
    """
    Sub-Agent: Consumes events from all Kafka topics and persists them into TimescaleDB.
    """
    def __init__(self, kafka_broker: str = "localhost:9092"):
        self.running = True
        self.kafka_broker = kafka_broker
        
        # Database connection settings
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")
        self.db_user = os.getenv("POSTGRES_USER", "trader")
        self.db_pass = os.getenv("POSTGRES_PASSWORD", "trader_password")
        self.db_name = os.getenv("POSTGRES_DB", "market_data")

        self.conn = None
        self._connect_db()

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _connect_db(self):
        """Establishes connection to TimescaleDB and runs initial schema creation."""
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_pass,
                dbname=self.db_name
            )
            self.conn.autocommit = True
            logger.info("Connected successfully to TimescaleDB.")
            self._initialize_schema()
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")

    def _initialize_schema(self):
        """Executes init_db.sql if SQL script exists."""
        sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "core", "init_db.sql")
        if os.path.exists(sql_path):
            with open(sql_path, "r") as f:
                sql_script = f.read()
            with self.conn.cursor() as cur:
                cur.execute(sql_script)
            logger.info("TimescaleDB hypertable schema verified/initialized.")

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Closing database connections...")
        self.running = False
        if self.conn:
            self.conn.close()
        sys.exit(0)

    def save_market_tick(self, payload: dict):
        tick = MarketTick(**payload)
        query = """
            INSERT INTO market_ticks (timestamp, symbol, price, volume)
            VALUES (%s, %s, %s, %s);
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (tick.timestamp, tick.symbol, tick.price, tick.volume))

    def save_agent_signal(self, payload: dict):
        sig = AgentSignal(**payload)
        query = """
            INSERT INTO agent_signals (timestamp, agent_id, commodity, action, confidence, reasoning)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (sig.timestamp, sig.agent_id, sig.commodity, sig.action, sig.confidence, sig.reasoning))

    def save_execution_order(self, payload: dict):
        order = ExecutionOrder(**payload)
        query = """
            INSERT INTO execution_orders (order_id, timestamp, commodity, action, quantity, order_type, stop_loss, take_profit, synthesized_confidence, reasoning)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (
                order.order_id, order.timestamp, order.commodity, order.action,
                order.quantity, order.order_type, order.stop_loss, order.take_profit,
                order.synthesized_confidence, order.reasoning
            ))

    def start(self):
        """Starts multi-topic Kafka consumer and routes events to corresponding SQL tables."""
        consumer = Consumer({
            'bootstrap.servers': self.kafka_broker,
            'group.id': 'timescaledb_persistence_group',
            'auto.offset.reset': 'earliest'
        })

        topics = ['market-ticks', 'quant-signals', 'news-signals', 'execution-orders']
        consumer.subscribe(topics)
        logger.info(f"Subscribed to topics {topics}. Streaming events into TimescaleDB...")

        try:
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka Consumer Error: {msg.error()}")
                    continue

                topic = msg.topic()
                payload = json.loads(msg.value().decode('utf-8'))

                if topic == 'market-ticks':
                    self.save_market_tick(payload)
                elif topic in ['quant-signals', 'news-signals']:
                    self.save_agent_signal(payload)
                elif topic == 'execution-orders':
                    self.save_execution_order(payload)

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

if __name__ == "__main__":
    agent = DBSinkAgent()
    agent.start()