from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class MarketTick:
    timestamp: str
    symbol: str
    price: float
    volume: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
        }
