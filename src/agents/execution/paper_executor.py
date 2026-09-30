import logging
import signal
import sys
from datetime import datetime, timezone

from src.core.schemas import ExecutionOrder
from src.core.kafka_client import EventBroker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [ExecutionAgent] - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PaperExecutionAgent")

class PaperExecutionAgent:
    """
    Sub-Agent: Subscribes to 'execution-orders', simulates order fills (paper trading),
    and logs trade confirmations.
    """
    def __init__(self, kafka_broker: str = "localhost:9092"):
        self.running = True
        self.broker = EventBroker(broker_url=kafka_broker, group_id="paper_executor_group")
        self.trade_history = []

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Closing Paper Execution Agent...")
        self.running = False
        self.broker.flush()
        sys.exit(0)

    def process_order(self, payload: dict):
        """Processes and fills incoming execution orders."""
        try:
            order = ExecutionOrder(**payload)
            
            # Simulate execution delay / fill confirmation
            logger.info("=" * 60)
            logger.info(f"⚡ ORDER RECEIVED FOR EXECUTION [{order.order_id}]")
            logger.info(f"   Symbol:     {order.commodity}")
            logger.info(f"   Action:     {order.action} {order.quantity} contracts @ {order.order_type}")
            logger.info(f"   Stop-Loss:  ${order.stop_loss:.2f} | Take-Profit: ${order.take_profit:.2f}")
            logger.info(f"   Confidence: {order.synthesized_confidence * 100:.1f}%")
            logger.info(f"   Reasoning:  {order.reasoning}")
            logger.info("   STATUS:     [FILLED - PAPER TRADE]")
            logger.info("=" * 60)

            self.trade_history.append(order)

        except Exception as e:
            logger.error(f"Failed to execute order payload: {e}")

    def start(self):
        logger.info("Starting Paper Execution Sub-Agent... Subscribing to 'execution-orders'")
        self.broker.start_consumer(topic="execution-orders", on_message=self.process_order)

if __name__ == "__main__":
    agent = PaperExecutionAgent()
    agent.start()