import time
from typing import Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class TimeplusRecord:
    """Represents a record"""

    topic: str
    value: Any
    key: Optional[str] = None
    partition: int = 0
    offset: Optional[int] = None
    timestamp: Optional[int] = None
    headers: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)  # milliseconds


@dataclass
class ProducerRecord:
    """Represents a record to be produced"""

    topic: str
    value: Any
    key: Optional[str] = None
    partition: Optional[int] = None
    timestamp: Optional[int] = None
    headers: Optional[Dict[str, Any]] = None
