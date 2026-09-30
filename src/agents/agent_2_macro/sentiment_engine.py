import time
import logging
import signal
import sys
import os
import json
import feedparser
from datetime import datetime, timezone
from openai import OpenAI

# Import core infrastructure
from src.core.schemas import AgentSignal, NewsSentiment
from src.core.kafka_client import EventBroker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Agent 2 MacroSentiment] - %(levelname)s - %(message)s")
logger = logging.getLogger("MacroSentimentEngine")

class MacroSentimentEngine:
    """
    Core Agent 2: Scrapes news, scores sentiment using an LLM,
    and publishes macro sentiment signals to Kafka.
    """
    def __init__(self, kafka_broker: str = "localhost:9092", poll_interval: int = 30):
        self.poll_interval = poll_interval
        self.running = True
        self.seen_headlines = set()  # Prevent processing the same headline twice
        
        # Target Commodities to track
        self.commodity_keywords = {
            "CL=F": ["crude", "oil", "opec", "brent", "petroleum", "gasoline"],
            "GC=F": ["gold", "precious metals", "bullion", "fed rate", "inflation"],
            "NG=F": ["natural gas", "gas storage", "lng", "pipeline"]
        }

        # News RSS Feeds
        self.rss_feeds = [
            "https://finance.yahoo.com/rss/topfrequentlysent",
            "https://news.google.com/rss/search?q=commodities+crude+oil+gold&hl=en-US&gl=US&ceid=US:en"
        ]

        # LLM Initialization (OpenAI or compatible endpoint like Ollama/LocalAI)
        api_key = os.getenv("OPENAI_API_KEY", "mock-key")
        self.openai_client = OpenAI(api_key=api_key)
        
        # Kafka Event Broker
        self.broker = EventBroker(broker_url=kafka_broker, group_id="agent_2_sentiment")

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Cleaning up...")
        self.running = False
        self.broker.flush()
        sys.exit(0)

    def analyze_headline_with_llm(self, headline: str, commodity: str) -> tuple[float, float, str]:
        """
        Passes headline to LLM to produce a structured JSON response.
        Returns: (sentiment_score [-1.0 to 1.0], confidence [0.0 to 1.0], reasoning)
        """
        # If no OpenAI API key is set, fallback to mock score for safe local testing
        if os.getenv("OPENAI_API_KEY") is None:
            logger.warning("No OPENAI_API_KEY found. Running in local mock mode.")
            return 0.5, 0.8, "Mock LLM reasoning: Price expected to rise due to demand news."

        system_prompt = (
            "You are an expert Wall Street commodity analyst. Evaluate the given news headline "
            "and determine its impact on the target commodity. Output ONLY a valid JSON object with keys: "
            "'sentiment_score' (float from -1.0 extreme bearish to +1.0 extreme bullish), "
            "'confidence' (float from 0.0 to 1.0), and 'reasoning' (concise 1-sentence explanation)."
        )

        user_prompt = f"Target Commodity: {commodity}\nHeadline: {headline}"

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini", # Lightweight and low latency
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1
            )
            result = json.loads(response.choices[0].message.content)
            return (
                float(result.get("sentiment_score", 0.0)),
                float(result.get("confidence", 0.5)),
                result.get("reasoning", "Parsed from headline.")
            )
        except Exception as e:
            logger.error(f"Error invoking LLM: {e}")
            return 0.0, 0.0, "LLM analysis failed."

    def process_news_feeds(self):
        """Scrapes RSS feeds and generates AgentSignals for matching commodities."""
        for feed_url in self.rss_feeds:
            logger.info(f"Scraping RSS feed: {feed_url}")
            parsed_feed = feedparser.parse(feed_url)

            for entry in parsed_feed.entries:
                headline = entry.title
                
                # Skip duplicate headlines
                if headline in self.seen_headlines:
                    continue
                self.seen_headlines.add(headline)

                # Match headline against target commodities
                headline_lower = headline.lower()
                for symbol, keywords in self.commodity_keywords.items():
                    if any(kw in headline_lower for kw in keywords):
                        logger.info(f"Matched News for [{symbol}]: '{headline}'")
                        
                        # 1. Analyze Sentiment via LLM
                        score, confidence, reasoning = self.analyze_headline_with_llm(headline, symbol)
                        
                        # 2. Determine Action
                        action = "HOLD"
                        if score >= 0.3:
                            action = "BUY"
                        elif score <= -0.3:
                            action = "SELL"

                        # 3. Build & Publish Signal
                        if action != "HOLD":
                            signal = AgentSignal(
                                agent_id="agent_2_macro",
                                commodity=symbol,
                                action=action,
                                confidence=confidence,
                                reasoning=f"[News Impact Score: {score:+.2f}] {reasoning}"
                            )
                            logger.info(f"Publishing News Signal: {symbol} -> {action} | {reasoning}")
                            self.broker.publish_event("news-signals", signal.model_dump())

    def start(self):
        """Main execution loop for polling RSS news."""
        logger.info("Starting Core Agent 2 (Macro & Sentiment Engine)...")
        while self.running:
            try:
                self.process_news_feeds()
            except Exception as e:
                logger.error(f"Unexpected error during news feed processing: {e}")
            
            time.sleep(self.poll_interval)

if __name__ == "__main__":
    agent = MacroSentimentEngine(poll_interval=30)
    agent.start()