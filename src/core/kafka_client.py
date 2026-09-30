import json
import logging
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from typing import Callable, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EventBroker")

class EventBroker:
    """
    Central nervous system for the Multi-Agent Trading Platform.
    Handles asynchronous publishing and subscribing to Kafka topics.
    """
    def __init__(self, broker_url: str = "localhost:9092", group_id: str = "trading_group"):
        self.broker_url = broker_url
        self.group_id = group_id
        
        # Check if running in offline mock backtesting mode
        self.is_mock = broker_url in [None, "mock", "offline", ""]

        if not self.is_mock:
            # Initialize Producer for real/live environment
            self.producer = Producer({
                'bootstrap.servers': self.broker_url,
                'client.id': 'agent_producer'
            })
        else:
            self.producer = None
            logger.info("EventBroker initialized in MOCK/OFFLINE mode (Kafka producer disabled).")

    def ensure_topic_exists(self, topic_name: str):
        if self.is_mock:
            return
        admin_client = AdminClient({'bootstrap.servers': self.broker_url})
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()
            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    logger.debug(f"Topic creation notice for '{topic_name}': {e}")

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def publish_event(self, topic: str, payload: Dict[str, Any]):
        if self.is_mock:
            return # Skip network calls during backtesting
        try:
            json_payload = json.dumps(payload, default=str)
            self.producer.produce(
                topic, 
                json_payload.encode('utf-8'), 
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish event to {topic}: {e}")

    def flush(self):
        if self.is_mock or not self.producer:
            return
        logger.info("Flushing pending events to Kafka...")
        self.producer.flush()

    def start_consumer(self, topic: str, on_message: Callable[[Dict[str, Any]], None]):
        if self.is_mock:
            logger.info(f"Mock mode active: Consumer for '{topic}' bypassed.")
            return

        self.ensure_topic_exists(topic)

        consumer = Consumer({
            'bootstrap.servers': self.broker_url,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        })
        
        consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}. Waiting for events...")

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    on_message(payload)
                except json.JSONDecodeError:
                    logger.error("Failed to decode message payload.")
                    
        except KeyboardInterrupt:
            logger.info("Consumer manually stopped.")
        finally:
            consumer.close()