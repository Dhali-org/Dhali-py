import os
import json
import base64
import asyncio
import pytest
import requests
import nest_asyncio
import websockets

nest_asyncio.apply()
from eth_account import Account
from web3 import Web3
from xrpl.wallet import Wallet
from xrpl.clients import JsonRpcClient
from eth_account.messages import encode_typed_data
import xrpl

from dhali.dhali_asset_manager import DhaliAssetManager
from dhali.dhali_channel_manager import DhaliChannelManager
from dhali.wallet_descriptor import WalletDescriptor
from dhali.currency import Currency
from dhali.asset_updates import AssetUpdates
from dhali.utils import wrap_as_x402_payment_payload
from dhali.config_utils import get_public_config

# Secrets from environment variables
XRPL_SECRET = os.getenv("XRPL_TESTNET_SECRET")
SEPOLIA_SECRET = os.getenv("SEPOLIA_TESTNET_SECRET")

def get_facilitator_url(public_config):
    env_url = os.getenv("DHALI_FACILITATOR_URL")
    if env_url:
        return env_url
    return public_config.get("ROOT_X402_FACILITATOR_URL", "https://x402.api.dhali.io")

@pytest.mark.asyncio
async def test_xrpl_comprehensive_integration():
    
    if not XRPL_SECRET:
        pytest.fail("XRPL_TESTNET_SECRET not set")

    # 1. Setup Wallet and Asset Manager
    wallet = Wallet.from_secret(XRPL_SECRET)
    asset_manager = DhaliAssetManager.xrpl(wallet)
    wallet_descriptor = WalletDescriptor(wallet.classic_address, "XRPL.TESTNET")
    currency = Currency("XRPL.TESTNET", "XRP", 6)

    # 2. Create Asset
    print(f"\nCreating XRPL asset for wallet: {wallet.classic_address}")
    create_result = await asset_manager.create_asset(wallet_descriptor, currency)
    assert create_result['schema'] == 'api_admin_gateway_create_successful'
    asset_uuid = create_result['uuid']
    print(f"Asset created with UUID: {asset_uuid}")

    # 3. Update Asset
    print("Updating XRPL asset...")
    updates = AssetUpdates(
        name="Comprehensive Integration Test Asset XRPL",
        earning_rate=100.0,
        earning_type="per_request"
    )
    update_result = await asset_manager.update_asset(asset_uuid, wallet_descriptor, updates)
    assert update_result['schema'] == 'api_admin_gateway_update_response'
    print("Asset updated successfully")

    # 4. Create Channel (Deposit)
    rpc_client = JsonRpcClient("https://s.altnet.rippletest.net:51234/")
    channel_manager = DhaliChannelManager.xrpl(wallet=wallet, rpc_client=rpc_client, currency=currency)
    print("Performing XRPL deposit...")
    amount_drops = 1000000 # 1 XRP
    deposit_result = channel_manager.deposit(amount_drops)
    assert deposit_result is not None
    print("XRPL Deposit successful")

    # 5. Generate Auth Token
    print("Generating XRPL auth token...")
    auth_token = channel_manager.get_auth_token()
    assert auth_token is not None
    print(f"XRPL Auth Token generated: {auth_token[:20]}...")

    # 6. Settle via Facilitator using the newly created asset
    print(f"Settling via facilitator using asset {asset_uuid}...")
    public_config = get_public_config()
    facilitator_url = get_facilitator_url(public_config)
    settle_url = f"{facilitator_url}/v2/{asset_uuid}/settle"
    
    # Use the wrap function as suggested by the user
    requirements = {
        "scheme": "dhali",
        "network": "xrpl:1",
        "asset": "xrpl:1/native:xrp",
        "amount": "100",
        "payTo": asset_uuid,
        "maxTimeoutSeconds": 60
    }
    requirements_base64 = base64.b64encode(json.dumps(requirements).encode("utf-8")).decode("utf-8")
    wrapped_base64 = wrap_as_x402_payment_payload(auth_token, requirements_base64)
    wrapped_payload = json.loads(base64.b64decode(wrapped_base64).decode("utf-8"))
    
    settle_payload = {
        "paymentRequirements": requirements,
        "paymentPayload": wrapped_payload
    }
    
    resp = requests.post(settle_url, json=settle_payload)
    if resp.status_code != 200 or not resp.json().get("success"): print(f"XRPL Facilitator settlement failed: {resp.status_code} {resp.text}")
    assert resp.status_code == 200
    assert resp.json().get("success") is True
    print("Facilitator settlement successful")

    # 7. Close Channel via WebSockets
    print("Closing channel via Admin Gateway...")
    root_url = public_config["ROOT_API_ADMIN_URL"]
    ws_url = root_url.replace("http", "ws", 1) + "/ws/close-channel"
    
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({
            "schema": "api_admin_gateway_closure_request",
            "schema_version": "1.0",
            "wallet": {
                "type": "Dhali-py",
                "address": wallet.classic_address,
                "protocol": "XRPL.TESTNET",
                "publicKey": wallet.public_key,
                "currency": {
                    "code": "XRP",
                    "scale": 6,
                    "issuer": None
                }
            },
            "protocol": "XRPL.TESTNET",
            "currency": "XRP",
            "issuer": None
        }))
        
        msg = json.loads(await ws.recv())
        assert msg["schema"] == "api_admin_gateway_message_to_be_signed"
        
        message_bytes = json.dumps(msg["message"], separators=(",", ":")).encode()
        signature = xrpl.core.keypairs.sign(message_bytes, wallet.private_key)
        
        await ws.send(json.dumps({
            "schema": "api_admin_gateway_signed_message_response",
            "schema_version": "1.1",
            "signature": signature,
            "public_key": wallet.public_key
        }))
        
        success_msg = json.loads(await ws.recv())
        if success_msg.get("schema") == "api_admin_gateway_authentication_successful":
             success_msg = json.loads(await ws.recv())
             
        assert success_msg.get("success") is True
        print(f"Channel closure initiated: {success_msg.get('message')}")

@pytest.mark.asyncio
async def test_evm_comprehensive_integration():

    if not SEPOLIA_SECRET:
        pytest.fail("SEPOLIA_TESTNET_SECRET not set")

    # 1. Setup Wallet and Asset Manager
    account = Account.from_key(SEPOLIA_SECRET)
    asset_manager = DhaliAssetManager.evm(account)
    wallet_descriptor = WalletDescriptor(account.address, "SEPOLIA")
    currency = Currency("SEPOLIA", "ETH", 18)

    # 2. Create Asset
    print(f"\nCreating EVM asset for wallet: {account.address}")
    create_result = await asset_manager.create_asset(wallet_descriptor, currency)
    assert create_result['schema'] == 'api_admin_gateway_create_successful'
    asset_uuid = create_result['uuid']
    print(f"EVM Asset created with UUID: {asset_uuid}")

    # 3. Update Asset
    print("Updating EVM asset...")
    updates = AssetUpdates(
        name="Comprehensive Integration Test Asset EVM",
        earning_rate=0.001,
        earning_type="per_request"
    )
    update_result = await asset_manager.update_asset(asset_uuid, wallet_descriptor, updates)
    assert update_result['schema'] == 'api_admin_gateway_update_response'
    print("EVM Asset updated successfully")

    # 4. Create Channel (Deposit)
    w3 = Web3(Web3.HTTPProvider("https://ethereum-sepolia.publicnode.com", request_kwargs={'timeout': 120}))
    channel_manager = DhaliChannelManager.evm(account=account, w3=w3, currency=currency)
    print("Performing EVM deposit...")
    amount_wei = 10**14 # 0.0001 ETH
    deposit_receipt = channel_manager.deposit(amount_wei)
    assert deposit_receipt.status == 1
    print("EVM Deposit successful")

    # 5. Generate Auth Token
    print("Generating EVM auth token...")
    auth_token = channel_manager.get_auth_token()
    assert auth_token is not None
    print(f"EVM Auth Token generated: {auth_token[:20]}...")

    # 6. Settle via Facilitator using the newly created asset
    print(f"Settling via facilitator using asset {asset_uuid}...")
    public_config = get_public_config()
    facilitator_url = get_facilitator_url(public_config)
    settle_url = f"{facilitator_url}/v2/{asset_uuid}/settle"
    
    # Use the wrap function as suggested by the user
    requirements = {
        "scheme": "dhali",
        "network": "eip155:11155111",
        "asset": "eip155:11155111/native:eth",
        "amount": "100",
        "payTo": asset_uuid,
        "maxTimeoutSeconds": 60
    }
    requirements_base64 = base64.b64encode(json.dumps(requirements).encode("utf-8")).decode("utf-8")
    wrapped_base64 = wrap_as_x402_payment_payload(auth_token, requirements_base64)
    wrapped_payload = json.loads(base64.b64decode(wrapped_base64).decode("utf-8"))
    
    settle_payload = {
        "paymentRequirements": requirements,
        "paymentPayload": wrapped_payload
    }
    
    resp = requests.post(settle_url, json=settle_payload)
    if resp.status_code != 200 or not resp.json().get("success"): print(f"EVM Facilitator settlement failed: {resp.status_code} {resp.text}")
    assert resp.status_code == 200
    assert resp.json().get("success") is True
    print("Facilitator settlement successful")

    # 7. Close Channel via WebSockets
    print("Closing channel via Admin Gateway...")
    root_url = public_config["ROOT_API_ADMIN_URL"]
    ws_url = root_url.replace("http", "ws", 1) + "/ws/close-channel"
    
    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({
            "schema": "api_admin_gateway_closure_request",
            "schema_version": "1.0",
            "wallet": {
                "type": "Dhali-py",
                "address": account.address,
                "protocol": "SEPOLIA",
                "publicKey": None,
                "currency": {
                    "code": "ETH",
                    "scale": 18,
                    "issuer": None
                }
            },
            "protocol": "SEPOLIA",
            "currency": "ETH",
            "issuer": None
        }))
        
        msg = json.loads(await ws.recv())
        assert msg["schema"] == "api_admin_gateway_message_to_be_signed"
        
        signable = encode_typed_data(full_message=msg["message"])
        signed = account.sign_message(signable)
        
        await ws.send(json.dumps({
            "schema": "api_admin_gateway_signed_message_response",
            "schema_version": "1.1",
            "signature": signed.signature.hex()
        }))
        
        success_msg = json.loads(await ws.recv())
        if success_msg.get("schema") == "api_admin_gateway_authentication_successful":
             success_msg = json.loads(await ws.recv())
             
        assert success_msg.get("success") is True
        print(f"Channel closure initiated: {success_msg.get('message')}")
