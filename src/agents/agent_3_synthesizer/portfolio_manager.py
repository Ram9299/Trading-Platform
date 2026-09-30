import logging
import signal
import sys
import threading
from typing import Dict, Tuple

from src.core.schemas import AgentSignal, ExecutionOrder
from src.core.kafka_client import EventBroker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [PortfolioManager] - %(levelname)s - %(message)s")
logger = logging.getLogger("PortfolioManager")

class PortfolioManagerAgent:
    """
    Core Agent 3: Synthesizes signals, manages risk, processes human overrides,
    and publishes final execution orders to Kafka.
    """
    def __init__(self, kafka_broker: str = "localhost:9092", account_balance: float = 100000.0):
        self.running = True
        self.account_balance = account_balance
        self.risk_per_trade = 0.02  # Risk 2% of capital per trade
        
        # State tracking
        self.kill_switch_active = False
        self.signal_cache: Dict[str, Dict[str, AgentSignal]] = {
            "CL=F": {}, "GC=F": {}, "NG=F": {}
        }
        
        self.broker = EventBroker(broker_url=kafka_broker, group_id="agent_3_synthesizer")

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Cleaning up...")
        self.running = False
        self.broker.flush()
        sys.exit(0)

    def process_system_command(self, payload: dict):
        """Catches Kill Switch broadcasts from the dashboard."""
        command = payload.get("command")
        if command == "ACTIVATE_KILL_SWITCH":
            self.kill_switch_active = True
            logger.error("🚨 GLOBAL KILL SWITCH ACTIVATED! All automated trading suspended.")
        elif command == "DEACTIVATE_KILL_SWITCH":
            self.kill_switch_active = False
            logger.info("🟢 KILL SWITCH DEACTIVATED! Automated trading resumed.")

    def process_incoming_signal(self, payload: dict):
        """Routes incoming signals to the synthesis engine."""
        try:
            sig = AgentSignal(**payload)
            symbol = sig.commodity
            agent_type = "quant" if "quant" in sig.agent_id else "news"
            
            # --- MANUAL OVERRIDE LOGIC ---
            if sig.agent_id == "human_trader_override":
                logger.warning(f"⚡ HUMAN OVERRIDE RECEIVED for {symbol}: {sig.action}")
                # Bypass synthesis and risk limits for direct human execution
                self._execute_order(symbol, sig.action, sig.confidence, sig.reasoning)
                return

            if self.kill_switch_active:
                logger.debug(f"Signal ignored for {symbol}. System is HALTED.")
                return

            # --- AUTOMATED LOGIC ---
            self.signal_cache[symbol][agent_type] = sig
            logger.info(f"Received {agent_type.upper()} signal for {symbol}: {sig.action} ({sig.confidence:.2f})")
            
            # Check if we have both signals to synthesize
            if "quant" in self.signal_cache[symbol] and "news" in self.signal_cache[symbol]:
                action, conf, reason = self.synthesize_signals(symbol)
                
                if action != "HOLD":
                    passed, qty, sl, tp = self.apply_risk_management(symbol, action, conf)
                    if passed:
                        self._execute_order(symbol, action, conf, reason, qty, sl, tp)
                        
                # Clear cache after synthesis attempt
                self.signal_cache[symbol] = {}

        except Exception as e:
            logger.error(f"Error processing signal payload: {e}")

    def synthesize_signals(self, symbol: str) -> Tuple[str, float, str]:
        """Evaluates alignment between Quant and News agents."""
        quant_sig = self.signal_cache[symbol].get("quant")
        news_sig = self.signal_cache[symbol].get("news")

        if quant_sig.action == news_sig.action:
            combined_conf = (quant_sig.confidence + news_sig.confidence) / 2
            reason = f"Consensus Reached. Quant: {quant_sig.reasoning} | News: {news_sig.reasoning}"
            return quant_sig.action, combined_conf, reason
        
        return "HOLD", 0.0, "Conflicting signals between Quant and News."

    def apply_risk_management(self, symbol: str, action: str, confidence: float) -> Tuple[bool, float, float, float]:
        """Calculates position sizing and bracket stops based on account risk."""
        if confidence < 0.25:
            return False, 0.0, 0.0, 0.0
            
        risk_amount = self.account_balance * self.risk_per_trade
        # Simplified placeholder math for position sizing
        quantity = round(risk_amount / 1000, 2)
        if quantity <= 0:
            quantity = 1.0
            
        # Simplified bracket order values (to be replaced by ATR in production)
        stop_loss = 1.5 
        take_profit = 3.0
        
        return True, quantity, stop_loss, take_profit

    def _execute_order(self, symbol, action, conf, reason, qty=1.0, sl=1.5, tp=3.0):
        """Constructs and fires the ExecutionOrder to Kafka."""
        import uuid
        order = ExecutionOrder(
            order_id=str(uuid.uuid4())[:8],
            commodity=symbol,
            action=action,
            quantity=qty,
            stop_loss=sl,
            take_profit=tp,
            synthesized_confidence=conf,
            reasoning=reason
        )
        logger.info(f"ORDER APPROVED & EMITTED [{order.order_id}]: {action} {qty} contracts x {symbol}")
        self.broker.publish_event("execution-orders", order.model_dump())

    def start(self):
        logger.info("Starting Core Agent 3 (Synthesizer & Portfolio Manager)...")
        
        # Subscribe to multiple topics concurrently in separate threads
        threading.Thread(
            target=self.broker.start_consumer, 
            args=("quant-signals", self.process_incoming_signal), 
            daemon=True
        ).start()
        
        threading.Thread(
            target=self.broker.start_consumer, 
            args=("news-signals", self.process_incoming_signal), 
            daemon=True
        ).start()

        threading.Thread(
            target=self.broker.start_consumer, 
            args=("system-commands", self.process_system_command), 
            daemon=True
        ).start()

        while self.running:
            import time
            time.sleep(1)

if __name__ == "__main__":
    agent = PortfolioManagerAgent()
    agent.start()