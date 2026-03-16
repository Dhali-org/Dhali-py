from typing import Optional, Any
from .dhali_xrpl_asset_manager import DhaliXrplAssetManager
from .dhali_eth_asset_manager import DhaliEthAssetManager

class DhaliAssetManager:
    """
    Factory for creating asset managers.
    """
    @staticmethod
    def xrpl(wallet: Any, base_url: Optional[str] = None) -> DhaliXrplAssetManager:
        return DhaliXrplAssetManager(wallet, base_url)

    @staticmethod
    def evm(account: Any, base_url: Optional[str] = None) -> DhaliEthAssetManager:
        return DhaliEthAssetManager(account, base_url)
