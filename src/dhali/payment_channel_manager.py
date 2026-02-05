from abc import ABC, abstractmethod
from typing import Any


class ChannelNotFound(Exception):
    """Raised when an expected payment channel is not found on-chain or in Firestore."""

    pass


class PaymentChannelManager(ABC):
    @abstractmethod
    def get_auth_token(self, amount: int) -> str:
        """
        Generates an authentication token (claim) signed by the wallet.
        """
        pass

    @abstractmethod
    def deposit(self, amount: int) -> Any:
        """
        Deposits funds into the payment channel.
        """
        pass
