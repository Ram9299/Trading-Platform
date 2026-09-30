import logging
import signal
import sys
import uuid
from datetime import datetime, timezone
from collections import defaultdict

# Import core infrastructure
from src.core.schemas import AgentSignal, ExecutionOrder
from src.core.kafka_client import EventBroker

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - [Agent 3 Synthesizer] - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PortfolioManager")

class PortfolioManagerAgent:
    """
    Core Agent 3: Synthesizes quantitative and news signals, resolves conflicts,
    applies risk management controls, and emits final execution orders.
    """
    def __init__(self, kafka_broker: str = "localhost:9092", account_balance: float = 100000.0):
        self.account_balance = account_balance
        self.max_risk_per_trade = 0.02  # Maximum 2% risk per trade
        self.daily_drawdown_limit = 0.05 # 5% daily circuit breaker
        self.current_daily_loss = 0.0
        
        # Signal Memory: Stores recent signals per commodity
        # Structure: { "GC=F": { "quant": AgentSignal, "news": AgentSignal } }
        self.signal_cache = defaultdict(dict)
        
        # Kafka Broker initialization
        self.broker = EventBroker(broker_url=kafka_broker, group_id="agent_3_synthesizer")
        
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Flushing broker...")
        self.broker.flush()
        sys.exit(0)

    def synthesize_signals(self, commodity: str) -> tuple[str, float, str]:
        """
        Conflict Resolution Logic:
        - Quant Weight: 60% (Technical momentum/RSI)
        - News Weight: 40% (Macro sentiment/LLM)
        
        Returns: (final_action ["BUY", "SELL", "HOLD"], net_confidence [0.0-1.0], reasoning)
        """
        cached = self.signal_cache[commodity]
        quant_sig: AgentSignal = cached.get("quant")
        news_sig: AgentSignal = cached.get("news")

        if not quant_sig and not news_sig:
            return "HOLD", 0.0, "No active signals."

        # Case 1: Only one signal exists
        if quant_sig and not news_sig:
            return quant_sig.action, quant_sig.confidence * 0.7, f"Quant Signal Only: {quant_sig.reasoning}"
        if news_sig and not quant_sig:
            return news_sig.action, news_sig.confidence * 0.5, f"News Signal Only: {news_sig.reasoning}"

        # Case 2: Both signals exist — Evaluate agreement
        quant_score = (1.0 if quant_sig.action == "BUY" else -1.0) * quant_sig.confidence
        news_score = (1.0 if news_sig.action == "BUY" else -1.0) * news_sig.confidence

        # Weighted Synthesis
        combined_score = (0.6 * quant_score) + (0.4 * news_score)

        if combined_score > 0.25:
            action = "BUY"
            confidence = abs(combined_score)
            reason = f"Concurrence [BUY]: Quant ({quant_sig.reasoning}) + News ({news_sig.reasoning})"
        elif combined_score < -0.25:
            action = "SELL"
            confidence = abs(combined_score)
            reason = f"Concurrence [SELL]: Quant ({quant_sig.reasoning}) + News ({news_sig.reasoning})"
        else:
            action = "HOLD"
            confidence = 0.0
            reason = f"Conflict / Low Confidence: Quant ({quant_sig.action}) vs News ({news_sig.action})"

        return action, confidence, reason

    def apply_risk_management(self, commodity: str, action: str, confidence: float) -> tuple[bool, float, float, float]:
        """
        Risk Control Engine:
        - Checks Daily Circuit Breaker
        - Calculates dynamic position size based on confidence and max portfolio risk
        """
        # Circuit Breaker Check
        if self.current_daily_loss >= (self.account_balance * self.daily_drawdown_limit):
            logger.error("CIRCUIT BREAKER TRIGGERED: Daily loss limit reached. Trading halted.")
            return False, 0.0, 0.0, 0.0

        # Position Sizing: Base allocation scaled by confidence
        risk_amount = self.account_balance * self.max_risk_per_trade * confidence
        
        # Example Contract Multipliers (Simplified for Futures Contracts)
        estimated_contract_price = 2000.0  # Placeholder reference price
        quantity = round(risk_amount / (estimated_contract_price * 0.1), 2)

        if quantity <= 0:
            return False, 0.0, 0.0, 0.0

        # Bracket Order Calculations (1:2 Risk/Reward)
        stop_loss = estimated_contract_price * (0.99 if action == "BUY" else 1.01)
        take_profit = estimated_contract_price * (1.02 if action == "BUY" else 0.98)

        return True, quantity, stop_loss, take_profit

    def process_signal_event(self, payload: dict, signal_type: str):
        """Processes incoming signal events from either Kafka topic."""
        signal_obj = AgentSignal(**payload)
        commodity = signal_obj.commodity

        # Update cache
        self.signal_cache[commodity][signal_type] = signal_obj
        logger.info(f"Received {signal_type.upper()} signal for {commodity}: {signal_obj.action} ({signal_obj.confidence:.2f})")

        # Synthesize decision
        action, confidence, reasoning = self.synthesize_signals(commodity)

        if action == "HOLD":
            logger.info(f"Synthesizer Decision [{commodity}]: HOLD | {reasoning}")
            return

        # Pass through Risk Engine
        passed_risk, quantity, stop_loss, take_profit = self.apply_risk_management(commodity, action, confidence)

        if passed_risk:
            order = ExecutionOrder(
                order_id=str(uuid.uuid4())[:8],
                commodity=commodity,
                action=action,
                quantity=quantity,
                order_type="MARKET",
                stop_loss=stop_loss,
                take_profit=take_profit,
                synthesized_confidence=confidence,
                reasoning=reasoning
            )

            logger.info(f"ORDER APPROVED & EMITTED [{order.order_id}]: {action} {quantity} x {commodity} | StopLoss: {stop_loss}")
            self.broker.publish_event("execution-orders", order.model_dump())
        else:
            logger.warning(f"Order Rejected by Risk Engine for {commodity}")

    def start(self):
        """Starts Kafka consumer listening to both signal topics."""
        logger.info("Starting Core Agent 3 (Synthesizer & Portfolio Manager)...")
        
        # Note: In production, use multi-topic subscription or dedicated worker threads
        # Listening to quant-signals
        self.broker.start_consumer(
            topic="quant-signals", 
            on_message=lambda msg: self.process_signal_event(msg, "quant")
        )

if __name__ == "__main__":
    agent = PortfolioManagerAgent()
    agent.start()