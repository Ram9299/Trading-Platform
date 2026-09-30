import time
import logging
import signal
import sys
from datetime import datetime, timezone
import yfinance as yf

# Import core infrastructure components
from src.core.schemas import MarketTick
from src.core.kafka_client import EventBroker

# Configure Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Agent 1A DataIngestion] - %(levelname)s - %(message)s")
logger = logging.getLogger("DataIngestionAgent")


class DataIngestionAgent:
    """
    Sub-Agent 1A: Fetches real-time commodity futures market data 
    and streams MarketTick events to Kafka.
    """
    def __init__(self, symbols: list[str], kafka_broker: str = "localhost:9092", poll_interval: int = 5):
        self.symbols = symbols
        self.poll_interval = poll_interval
        self.running = True
        self.topic = "market-ticks"
        
        # Initialize Kafka Producer
        self.broker = EventBroker(broker_url=kafka_broker, group_id="agent_1_ingestion")
        
        # Attach graceful shutdown handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Cleaning up...")
        self.running = False
        self.broker.flush()
        sys.exit(0)

    def fetch_latest_tick(self, symbol: str) -> MarketTick | None:
        """
        Fetches the latest tick/bar for a given ticker using yfinance.
        """
        try:
            ticker = yf.Ticker(symbol)
            # Fetch 1-day fast historical data with 1m interval
            data = ticker.history(period="1d", interval="1m")
            
            if data.empty:
                logger.warning(f"No market data returned for symbol: {symbol}")
                return None

            latest_row = data.iloc[-1]
            price = float(latest_row["Close"])
            volume = float(latest_row["Volume"])
            
            tick = MarketTick(
                symbol=symbol,
                price=price,
                volume=volume,
                timestamp=datetime.now(timezone.utc)
            )
            return tick

        except Exception as e:
            logger.error(f"Error fetching ticker {symbol}: {e}")
            return None

    def start(self):
        """
        Main execution loop continuously fetching prices and publishing to Kafka.
        """
        logger.info(f"Starting Data Ingestion Engine for symbols: {self.symbols}")
        
        while self.running:
            for symbol in self.symbols:
                tick = self.fetch_latest_tick(symbol)
                
                if tick:
                    # Convert Pydantic model to dictionary and publish
                    payload = tick.model_dump()
                    self.broker.publish_event(topic=self.topic, payload=payload)
                    logger.info(f"Published MarketTick: {symbol} @ ${tick.price:.2f} | Vol: {tick.volume}")
            
            self.broker.flush()
            time.sleep(self.poll_interval)


if __name__ == "__main__":
    # Standard Commodity Futures Tickers:
    # CL=F (Crude Oil), GC=F (Gold), NG=F (Natural Gas)
    TARGET_COMMODITIES = ["CL=F", "GC=F", "NG=F"]
    
    agent = DataIngestionAgent(symbols=TARGET_COMMODITIES, poll_interval=5)
    agent.start()