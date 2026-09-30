import logging
import signal
import sys
import pandas as pd
from collections import defaultdict, deque
import yfinance as yf

# Import core infrastructure
from src.core.schemas import MarketTick, AgentSignal
from src.core.kafka_client import EventBroker

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - [Agent 1B IndicatorEngine] - %(levelname)s - %(message)s")
logger = logging.getLogger("IndicatorEngine")

class IndicatorEngine:
    """
    Sub-Agent 1B: Consumes live tick data, calculates technical indicators (RSI, EMA),
    and publishes trade signals based on quantitative rules.
    """
    def __init__(self, kafka_broker: str = "localhost:9092"):
        self.running = True
        
        # We store the last 50 ticks for each symbol to calculate indicators
        self.history_window = 50 
        self.price_history = defaultdict(lambda: deque(maxlen=self.history_window))
        
        # Kafka Clients: We need a consumer to listen to ticks, and a producer to send signals
        self.broker = EventBroker(broker_url=kafka_broker, group_id="agent_1_indicators")
        
        # Attach graceful shutdown handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        # PRE-WARM AT THE VERY END
        logger.info("Pre-warming indicator engine with historical data...")
        for symbol in ["CL=F", "GC=F", "NG=F"]:
            try:
                hist = yf.Ticker(symbol).history(period="1d", interval="1m")
                if not hist.empty:
                    recent_closes = hist["Close"].tail(20).tolist()
                    self.price_history[symbol].extend(recent_closes)
                    logger.info(f"Loaded {len(recent_closes)} historical bars for {symbol}")
            except Exception as e:
                logger.warning(f"Could not pre-warm data for {symbol}: {e}")

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Flushing broker...")
        self.running = False
        self.broker.flush()
        sys.exit(0)

    def calculate_indicators(self, prices: list[float]) -> tuple[float, float]:
        """Calculates 14-period RSI and 20-period EMA using Pandas."""
        series = pd.Series(prices)
        
        # Calculate 20-period Exponential Moving Average (EMA)
        ema_20 = series.ewm(span=20, adjust=False).mean().iloc[-1]
        
        # Calculate 14-period Relative Strength Index (RSI)
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        
        avg_gain = gain.ewm(com=13, min_periods=14).mean()
        avg_loss = loss.ewm(com=13, min_periods=14).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # If we don't have enough data to calculate RSI, pandas returns NaN
        current_rsi = rsi.iloc[-1]
        
        return ema_20, current_rsi

    def evaluate_strategy(self, symbol: str, current_price: float, ema: float, rsi: float) -> AgentSignal | None:
        """
        Pure Quantitative Logic:
        - BUY: RSI < 30 (Oversold) AND Price > EMA (Uptrend starting)
        - SELL: RSI > 70 (Overbought) AND Price < EMA (Downtrend starting)
        """
        if pd.isna(rsi) or pd.isna(ema):
            return None # Not enough data points yet

        action = None
        reasoning = ""
        
        if rsi < 30 and current_price > ema:
            action = "BUY"
            reasoning = f"Oversold (RSI: {rsi:.1f}) and price crossed above EMA ({ema:.2f})"
        elif rsi > 70 and current_price < ema:
            action = "SELL"
            reasoning = f"Overbought (RSI: {rsi:.1f}) and price crossed below EMA ({ema:.2f})"
            
        if action:
            return AgentSignal(
                agent_id="agent_1_quant",
                commodity=symbol,
                action=action,
                confidence=0.85, # In a real system, this could scale based on volume or RSI severity
                reasoning=reasoning
            )
        return None

    def on_tick_received(self, tick_dict: dict):
        """Callback function executed every time a new MarketTick arrives from Kafka."""
    
        tick = MarketTick(**tick_dict)
        symbol = tick.symbol
        
        # Add the newest price to our rolling window
        self.price_history[symbol].append(tick.price)
        prices = list(self.price_history[symbol])
        
        # We need at least 20 ticks to calculate a 20-period EMA
        if len(prices) < 20:
            logger.info(f"[{symbol}] Warming up rolling window: {len(prices)}/20 ticks received...")
            return
            
        # 1. Calculate Mathematics
        ema, rsi = self.calculate_indicators(prices)
        logger.info(f"[{symbol}] Price: ${tick.price:.2f} | EMA(20): ${ema:.2f} | RSI(14): {rsi:.1f}")
        
        # 2. Evaluate Trade Logic
        signal = self.evaluate_strategy(symbol, tick.price, ema, rsi)
        
        # 3. Publish to Synthesizer if a signal was generated
        if signal:
            logger.info(f"🚨 SIGNAL GENERATED [{symbol}]: {signal.action} | Reason: {signal.reasoning}")
            self.broker.publish_event(topic="quant-signals", payload=signal.model_dump())

    def start(self):
        """Starts the Kafka consumer loop to listen for ticks."""
        logger.info("Starting Sub-Agent 1B: Listening to 'market-ticks' topic...")
        self.broker.start_consumer(topic="market-ticks", on_message=self.on_tick_received)

if __name__ == "__main__":
    agent = IndicatorEngine()
    agent.start()