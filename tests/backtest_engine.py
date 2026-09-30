import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime
from typing import List, Dict

# Import existing schemas and risk models
from src.core.schemas import MarketTick, AgentSignal, ExecutionOrder
from src.agents.agent_1_quant.indicator_engine import IndicatorEngine
from src.agents.agent_2_macro.sentiment_engine import MacroSentimentEngine
from src.agents.agent_3_synthesizer.portfolio_manager import PortfolioManagerAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [BacktestEngine] - %(levelname)s - %(message)s")
logger = logging.getLogger("Backtester")

class EventDrivenBacktester:
    """
    Chronological Event-Driven Backtest Engine for Multi-Agent Trading System.
    """
    def __init__(self, initial_capital: float = 100000.0, commission_per_contract: float = 2.0, slippage_pct: float = 0.0005):
        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.commission = commission_per_contract
        self.slippage_pct = slippage_pct
        
        # Instantiate System Agents
        self.indicator_engine = IndicatorEngine(kafka_broker="mock")
        self.macro_engine = MacroSentimentEngine(kafka_broker="mock")
        self.portfolio_manager = PortfolioManagerAgent(kafka_broker="mock", account_balance=initial_capital)
        
        # State Tracking
        self.positions: Dict[str, float] = {"CL=F": 0, "GC=F": 0, "NG=F": 0}
        self.trade_log: List[Dict] = []
        self.equity_curve: List[Dict] = []

    def simulate_order_execution(self, order: ExecutionOrder, current_market_price: float):
        """Simulates exchange order matching with slippage and commission costs."""
        # Apply slippage (Buy orders fill higher, Sell orders fill lower)
        slippage = current_market_price * self.slippage_pct
        fill_price = current_market_price + slippage if order.action == "BUY" else current_market_price - slippage
        
        cost = order.quantity * fill_price
        total_commission = order.quantity * self.commission
        
        # Calculate Position PnL impact
        position_change = order.quantity if order.action == "BUY" else -order.quantity
        self.positions[order.commodity] += position_change
        self.capital -= total_commission

        trade_record = {
            "order_id": order.order_id,
            "timestamp": order.timestamp,
            "commodity": order.commodity,
            "action": order.action,
            "quantity": order.quantity,
            "fill_price": fill_price,
            "commission": total_commission,
            "confidence": order.synthesized_confidence,
            "reasoning": order.reasoning
        }
        self.trade_log.append(trade_record)
        logger.info(f"FILLED [{order.action}] {order.quantity} x {order.commodity} @ ${fill_price:.2f} (Slippage: ${slippage:.2f})")

    def run_backtest(self, historical_ticks: pd.DataFrame, historical_news: pd.DataFrame):
        """
        Processes price ticks and news headlines in chronological order.
        """
        logger.info("🚀 Starting Multi-Agent Backtest Simulation...")

        # Standardize timestamps and combine into a single event timeline
        historical_ticks["event_type"] = "tick"
        historical_news["event_type"] = "news"

        # Combine and sort strictly by timestamp to eliminate look-ahead bias
        timeline = pd.concat([historical_ticks, historical_news]).sort_values("timestamp")

        latest_prices = {}

        for idx, event in timeline.iterrows():
            timestamp = event["timestamp"]

            if event["event_type"] == "tick":
                symbol = event["symbol"]
                price = float(event["price"])
                latest_prices[symbol] = price

                # 1. Feed Tick to Indicator Engine
                tick_obj = MarketTick(symbol=symbol, price=price, volume=float(event.get("volume", 0)), timestamp=timestamp)
                
                # Check for Quant Signal
                self.indicator_engine.price_history[symbol].append(price)
                if len(self.indicator_engine.price_history[symbol]) >= 20:
                    ema, rsi = self.indicator_engine.calculate_indicators(list(self.indicator_engine.price_history[symbol]))
                    quant_sig = self.indicator_engine.evaluate_strategy(symbol, price, ema, rsi)
                    
                    if quant_sig:
                        self.portfolio_manager.signal_cache[symbol]["quant"] = quant_sig

            elif event["event_type"] == "news":
                symbol = event["symbol"]
                headline = event["headline"]

                # 2. Feed News to Sentiment Engine (Local Ollama / Rule Fallback)
                score, confidence, reasoning = self.macro_engine.analyze_headline_with_llm(headline, symbol)
                action = "BUY" if score >= 0.3 else ("SELL" if score <= -0.3 else "HOLD")

                if action != "HOLD":
                    news_sig = AgentSignal(
                        agent_id="agent_2_macro",
                        commodity=symbol,
                        action=action,
                        confidence=confidence,
                        reasoning=f"[News Impact Score: {score:+.2f}] {reasoning}",
                        timestamp=timestamp
                    )
                    self.portfolio_manager.signal_cache[symbol]["news"] = news_sig

            # 3. Trigger Portfolio Manager Synthesizer Decision
            for symbol in ["CL=F", "GC=F", "NG=F"]:
                if symbol in latest_prices:
                    action, conf, reason = self.portfolio_manager.synthesize_signals(symbol)
                    if action != "HOLD":
                        passed, qty, sl, tp = self.portfolio_manager.apply_risk_management(symbol, action, conf)
                        if passed:
                            order = ExecutionOrder(
                                order_id=f"BT-{len(self.trade_log)+1}",
                                commodity=symbol,
                                action=action,
                                quantity=qty,
                                stop_loss=sl,
                                take_profit=tp,
                                synthesized_confidence=conf,
                                reasoning=reason,
                                timestamp=timestamp
                            )
                            self.simulate_order_execution(order, latest_prices[symbol])
                            # Clear processed signals
                            self.portfolio_manager.signal_cache[symbol] = {}

            # Record portfolio equity
            portfolio_value = self.capital
            for sym, pos in self.positions.items():
                portfolio_value += pos * latest_prices.get(sym, 0.0)

            self.equity_curve.append({"timestamp": timestamp, "equity": portfolio_value})

        logger.info("✅ Backtest Simulation Complete.")
        self.generate_performance_metrics()

    def generate_performance_metrics(self):
        """Calculates institutional portfolio performance metrics."""
        df_equity = pd.DataFrame(self.equity_curve)
        if df_equity.empty:
            logger.warning("No equity data generated.")
            return

        total_return = ((self.equity_curve[-1]["equity"] - self.initial_capital) / self.initial_capital) * 100
        
        # Calculate Drawdown
        df_equity["peak"] = df_equity["equity"].cummax()
        df_equity["drawdown"] = (df_equity["equity"] - df_equity["peak"]) / df_equity["peak"]
        max_drawdown = df_equity["drawdown"].min() * 100

        print("\n" + "=" * 50)
        print("📈 MULTI-AGENT BACKTEST PERFORMANCE REPORT")
        print("=" * 50)
        print(f"Initial Capital:         ${self.initial_capital:,.2f}")
        print(f"Ending Portfolio Equity: ${self.equity_curve[-1]['equity']:,.2f}")
        print(f"Total Net Return:        {total_return:+.2f}%")
        print(f"Maximum Drawdown (MDD):  {max_drawdown:.2f}%")
        print(f"Total Trades Executed:   {len(self.trade_log)}")
        print("=" * 50 + "\n")

if __name__ == "__main__":
    # Create synthetic test dataset for backtest demonstration
    timestamps = pd.date_range("2026-01-01", periods=100, freq="5min")
    
    ticks_data = pd.DataFrame({
        "timestamp": timestamps,
        "symbol": ["GC=F"] * 100,
        "price": [2000.0 + i*0.5 + (np.random.randn() * 2) for i in range(100)],
        "volume": [100.0] * 100
    })

    news_data = pd.DataFrame({
        "timestamp": [timestamps[20], timestamps[50]],
        "symbol": ["GC=F", "GC=F"],
        "headline": [
            "Central Bank announces surge in official gold purchases",
            "Inflation concerns cool down as gold prices slide"
        ]
    })

    backtester = EventDrivenBacktester()
    backtester.run_backtest(ticks_data, news_data)