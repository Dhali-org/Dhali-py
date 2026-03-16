import json
from xrpl.core.keypairs import sign
from .base_asset_manager import BaseAssetManager
from .wallet_descriptor import WalletDescriptor

class DhaliXrplAssetManager(BaseAssetManager):
    """
    DhaliAssetManager for XRPL protocol.
    """
    async def _perform_signing(self, typed_data, wallet_descriptor: WalletDescriptor):
        # Backend expects canonical JSON string for XRPL
        message_to_sign = json.dumps(typed_data, separators=(",", ":")).encode()
        signature = sign(message_to_sign, self.wallet.private_key)
        
        return {
            "schema": "api_admin_gateway_signed_message_response",
            "signature": signature,
            "public_key": self.wallet.public_key
        }
