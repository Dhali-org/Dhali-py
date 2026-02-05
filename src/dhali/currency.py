from dataclasses import dataclass
from typing import Optional


@dataclass
class Currency:
    code: str
    scale: int
    token_address: Optional[str] = None
