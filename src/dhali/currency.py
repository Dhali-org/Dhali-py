from dataclasses import dataclass
from typing import Optional


@dataclass
class Currency:
    network: str
    code: str
    scale: int
    token_address: Optional[str] = None
