from pydantic import BaseModel, Field
from typing import Literal
from datetime import datetime

class MarketTick(BaseModel):
    """Payload emitted by the Data Ingestion Sub-Agent."""
    symbol: str
    price: float
    volume: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class NewsSentiment(BaseModel):
    """Payload emitted by the NLP Sentiment Sub-Agent."""
    commodity: str
    sentiment_score: float = Field(ge=-1.0, le=1.0, description="-1.0 is extreme bearish, 1.0 is extreme bullish")
    confidence: float = Field(ge=0.0, le=1.0)
    source: str
    headline: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class AgentSignal(BaseModel):
    """Standardized prediction payload emitted by Core Agent 1 and Core Agent 2."""
    agent_id: str
    commodity: str
    action: Literal["BUY", "SELL", "HOLD"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)