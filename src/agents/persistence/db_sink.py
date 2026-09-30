import logging
import signal
import sys
import os
import json
import psycopg2
from confluent_kafka import Consumer, KafkaError

from src.core.schemas import MarketTick, AgentSignal, ExecutionOrder

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [DBSinkAgent] - %(levelname)s - %(message)s")
logger = logging.getLogger("DBSink")

class DBSinkAgent:
    """
    Sub-Agent: Consumes events from all Kafka topics and persists them into TimescaleDB/PostgreSQL.
    """
    def __init__(self, kafka_broker: str = "localhost:9092"):
        self.running = True
        self.kafka_broker = kafka_broker
        
        # Database connection parameters (supports local Postgres default fallback)
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")
        self.db_user = os.getenv("POSTGRES_USER", "postgres")
        self.db_pass = os.getenv("POSTGRES_PASSWORD", "1234")
        self.db_name = os.getenv("POSTGRES_DB", "market_data")

        self.conn = None
        self._connect_db()

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _connect_db(self):
        """Establishes connection to PostgreSQL/TimescaleDB."""
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_pass,
                dbname=self.db_name
            )
            self.conn.autocommit = True
            logger.info("Connected successfully to PostgreSQL/TimescaleDB.")
            self._initialize_schema()
        except Exception as e:
            logger.warning(f"Could not connect to database ({e}). DBSink running in standby mode.")
            self.conn = None

    def _initialize_schema(self):
        if not self.conn:
            return
        sql_path = os.path.join(os.path.dirname(__file__), "..", "..", "core", "init_db.sql")
        if os.path.exists(sql_path):
            try:
                with open(sql_path, "r") as f:
                    sql_script = f.read()
                with self.conn.cursor() as cur:
                    cur.execute(sql_script)
                logger.info("Database schema verified/initialized.")
            except Exception as e:
                logger.error(f"Error initializing SQL schema: {e}")

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Closing database connections...")
        self.running = False
        if self.conn:
            self.conn.close()
        sys.exit(0)

    def save_market_tick(self, payload: dict):
        if not self.conn:
            return
        try:
            tick = MarketTick(**payload)
            query = """
                INSERT INTO market_ticks (timestamp, symbol, price, volume)
                VALUES (%s, %s, %s, %s);
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (tick.timestamp, tick.symbol, tick.price, tick.volume))
        except Exception as e:
            logger.error(f"Failed to insert market tick into DB: {e}")

    def save_agent_signal(self, payload: dict):
        if not self.conn:
            return
        try:
            sig = AgentSignal(**payload)
            query = """
                INSERT INTO agent_signals (timestamp, agent_id, commodity, action, confidence, reasoning)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (sig.timestamp, sig.agent_id, sig.commodity, sig.action, sig.confidence, sig.reasoning))
        except Exception as e:
            logger.error(f"Failed to insert agent signal into DB: {e}")

    def save_execution_order(self, payload: dict):
        if not self.conn:
            return
        try:
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
        except Exception as e:
            logger.error(f"Failed to insert execution order into DB: {e}")

    def start(self):
        consumer = Consumer({
            'bootstrap.servers': self.kafka_broker,
            'group.id': 'timescaledb_persistence_group',
            'auto.offset.reset': 'earliest'
        })

        topics = ['market-ticks', 'quant-signals', 'news-signals', 'execution-orders']
        consumer.subscribe(topics)
        logger.info(f"Subscribed to topics {topics}. Ready for streaming...")

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