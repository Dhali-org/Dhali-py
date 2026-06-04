import asyncio
import json
from websockets import connect  # type: ignore
import websockets.exceptions
from typing import Optional, Any, Dict
from dhali.currency import Currency
from dhali.wallet_descriptor import WalletDescriptor
from dhali.asset_updates import AssetUpdates
from .config_utils import get_public_config

class BaseAssetManager:
    """
    Base class for Dhali asset management via WebSocket.
    """
    def __init__(self, wallet: Any, base_url: Optional[str] = None):
        if base_url is None:
            config = get_public_config()
            root_url = config.get("ROOT_API_ADMIN_URL")
            if not root_url:
                raise RuntimeError("ROOT_API_ADMIN_URL not found in public config")
            base_url = root_url

        if base_url.startswith("https://"):
            self.base_url = base_url.replace("https://", "wss://", 1)
        elif base_url.startswith("http://"):
            self.base_url = base_url.replace("http://", "ws://", 1)
        else:
            self.base_url = base_url

        self.wallet = wallet

    async def _perform_signing(self, typed_data: Dict[str, Any], wallet_descriptor: WalletDescriptor) -> Dict[str, Any]:
        """
        Abstract method to handle protocol-specific signing.
        """
        raise NotImplementedError("_perform_signing must be implemented by subclass")

    async def _handle_auth(self, ws, message, wallet_descriptor):
        if message.get("schema") == "api_admin_gateway_message_to_be_signed":
            typed_data = message["message"]
            auth_response = await self._perform_signing(typed_data, wallet_descriptor)
            await ws.send(json.dumps(auth_response))
            return True
        return False

    async def create_asset(self, wallet_descriptor: WalletDescriptor, currency: Currency):
        if not isinstance(wallet_descriptor, WalletDescriptor):
            raise ValueError("wallet_descriptor must be an instance of WalletDescriptor")
        if not isinstance(currency, Currency):
            raise ValueError("currency must be an instance of Currency")

        async with connect(f"{self.base_url}/create", open_timeout=30) as ws:
            await ws.send(json.dumps({
                "owner": wallet_descriptor.to_json(),
                "currency": {
                    "code": currency.code,
                    "scale": currency.scale,
                    "issuer": currency.token_address
                }
            }))

            while True:
                try:
                    message_str = await ws.recv()
                    message = json.loads(message_str)

                    if await self._handle_auth(ws, message, wallet_descriptor):
                        continue

                    if message.get("schema") == "api_admin_gateway_request_wallet_json":
                        await ws.send(json.dumps({
                            "schema": "api_admin_gateway_wallet_json_response",
                            "wallet": wallet_descriptor.to_json()
                        }))
                    elif message.get("schema") == "api_admin_gateway_create_successful":
                        return message
                    elif "qr_code_url" in message:
                        print(f"Scan this QR code to authenticate: {message['qr_code_url']}")
                    elif "error" in message:
                        raise Exception(message["error"])
                except websockets.exceptions.ConnectionClosed as e:
                    if e.code not in [1000, 1005]:
                        raise Exception(f"WebSocket closed with code {e.code}: {e.reason}")
                    break

    async def update_asset(self, dhali_id: str, wallet_descriptor: WalletDescriptor, updates: AssetUpdates):
        if not isinstance(wallet_descriptor, WalletDescriptor):
            raise ValueError("wallet_descriptor must be an instance of WalletDescriptor")
        if not isinstance(updates, AssetUpdates):
            raise ValueError("updates must be an instance of AssetUpdates")

        async with connect(f"{self.base_url}/{dhali_id}/update", open_timeout=30) as ws:
            while True:
                try:
                    message_str = await ws.recv()
                    message = json.loads(message_str)

                    if await self._handle_auth(ws, message, wallet_descriptor):
                        continue

                    if message.get("schema") == "api_admin_gateway_request_wallet_json":
                        await ws.send(json.dumps({
                            "schema": "api_admin_gateway_wallet_json_response",
                            "wallet": wallet_descriptor.to_json()
                        }))
                    elif message.get("schema") == "api_admin_gateway_authentication_successful":
                        await ws.send(json.dumps({
                            "schema": "api_admin_gateway_prefill_request",
                            "schema_version": "1.0"
                        }))
                    elif message.get("schema") == "api_admin_gateway_prefill_response":
                        await ws.send(json.dumps({
                            "schema": "api_admin_gateway_update_request",
                            "schema_version": "1.0",
                            "updates": updates.to_gateway_format()
                        }))
                    elif message.get("schema") == "api_admin_gateway_update_response":
                        return message
                    elif "qr_code_url" in message:
                        print(f"Scan this QR code to authenticate: {message['qr_code_url']}")
                    elif "error" in message or message.get("status") == "failed":
                        raise Exception(message.get("error") or "Update failed")
                except websockets.exceptions.ConnectionClosed as e:
                    if e.code not in [1000, 1005]:
                        raise Exception(f"WebSocket closed with code {e.code}: {e.reason}")
                    break
