from eth_account.messages import encode_typed_data
from .base_asset_manager import BaseAssetManager
from .wallet_descriptor import WalletDescriptor

class DhaliEthAssetManager(BaseAssetManager):
    """
    DhaliAssetManager for EVM protocol.
    """
    async def _perform_signing(self, typed_data, wallet_descriptor: WalletDescriptor):
        signable = encode_typed_data(full_message=typed_data)
        signed = self.wallet.sign_message(signable)
        
        return {
            "schema": "api_admin_gateway_signed_message_response",
            "signature": signed.signature.hex()
        }
