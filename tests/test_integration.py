import pytest
import asyncio
from xrpl.wallet import Wallet
from dhali.dhali_asset_manager import DhaliAssetManager
from dhali.wallet_descriptor import WalletDescriptor
from dhali.currency import Currency
from dhali.asset_updates import AssetUpdates

@pytest.mark.asyncio
async def test_create_and_update_asset_integration():
    # 1. Generate Wallet
    wallet = Wallet.create()
    manager = DhaliAssetManager.xrpl(wallet)
    
    wallet_descriptor = WalletDescriptor(wallet.classic_address, "XRPL.TESTNET")
    currency = Currency("XRPL.TESTNET", "XRP", 6)
    
    # 2. Create Asset
    print(f"\nCreating asset for wallet: {wallet.classic_address}")
    create_result = await manager.create_asset(wallet_descriptor, currency)
    assert create_result['schema'] == 'api_admin_gateway_create_successful'
    asset_id = create_result['uuid']
    print(f"Asset created with UUID: {asset_id}")
    
    # 3. Update Asset
    print("Updating asset...")
    updates = AssetUpdates(
        name="Integration Test Asset",
        earning_rate=100.0,
        earning_type="per_request"
    )
    
    update_result = await manager.update_asset(asset_id, wallet_descriptor, updates)
    assert update_result['schema'] == 'api_admin_gateway_update_response'
    print("Asset updated successfully")

@pytest.mark.asyncio
async def test_create_and_update_asset_evm_integration():
    # 1. Generate Wallet
    from eth_account import Account
    account = Account.create()
    manager = DhaliAssetManager.evm(account)
    
    wallet_descriptor = WalletDescriptor(account.address, "SEPOLIA")
    currency = Currency("SEPOLIA", "ETH", 18)
    
    # 2. Create Asset
    print(f"\nCreating EVM asset for wallet: {account.address}")
    create_result = await manager.create_asset(wallet_descriptor, currency)
    assert create_result['schema'] == 'api_admin_gateway_create_successful'
    asset_id = create_result['uuid']
    print(f"EVM Asset created with UUID: {asset_id}")
    
    # 3. Update Asset
    print("Updating EVM asset...")
    updates = AssetUpdates(
        name="Integration Test Asset EVM",
        earning_rate=0.001,
        earning_type="per_request"
    )
    
    update_result = await manager.update_asset(asset_id, wallet_descriptor, updates)
    assert update_result['schema'] == 'api_admin_gateway_update_response'
    print("EVM Asset updated successfully")
