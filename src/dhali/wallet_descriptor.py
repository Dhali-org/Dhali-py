from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class WalletDescriptor:
    address: str
    protocol: str
    type: str = 'Dhali-py'

    def to_json(self) -> Dict[str, Any]:
        """
        Converts to JSON format expected by Dhali backend
        """
        return {
            "address": self.address,
            "wallet_id": self.address,
            "type": self.type,
            "protocol": self.protocol
        }
