from typing import Optional, Any, Dict
from .dhali_xrpl_channel_manager import DhaliXrplChannelManager
from .dhali_eth_channel_manager import DhaliEthChannelManager
from .payment_channel_manager import ChannelNotFound
from .currency import Currency

ChannelNotFound = ChannelNotFound


class DhaliChannelManager:
    """
    Factory for creating channel managers.
    """

    @staticmethod
    def xrpl(
        wallet: Any,
        rpc_client: Any,
        protocol: str,
        currency: Currency,
        http_client: Optional[Any] = None,
        public_config: Optional[Dict[str, Any]] = None,
    ) -> DhaliXrplChannelManager:
        return DhaliXrplChannelManager(
            wallet=wallet,
            rpc_client=rpc_client,
            protocol=protocol,
            currency=currency,
            http_client=http_client,
            public_config=public_config,
        )

    @staticmethod
    def evm(
        account: Any,
        w3: Any,
        protocol: str,
        currency: Currency,
        http_client: Optional[Any] = None,
        public_config: Optional[Dict[str, Any]] = None,
    ) -> DhaliEthChannelManager:
        return DhaliEthChannelManager(
            account=account,
            w3=w3,
            protocol=protocol,
            currency=currency,
            http_client=http_client,
            public_config=public_config,
        )
