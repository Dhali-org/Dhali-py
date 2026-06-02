import pytest
import base64
import json
from unittest.mock import MagicMock, patch
from xrpl.models.requests import AccountChannels
from dhali.dhali_xrpl_channel_manager import DhaliXrplChannelManager, ChannelNotFound
from dhali.currency import Currency

@pytest.fixture
def mock_wallet():
    wallet = MagicMock()
    wallet.classic_address = "rSource"
    wallet.public_key = "PubKey"
    wallet.private_key = "PrivKey"
    return wallet

@pytest.fixture
def mock_rpc():
    return MagicMock()

@pytest.fixture
def currency():
    return Currency("XRPL.TESTNET", "XRP", 6)

@pytest.fixture
def public_config():
    return {
        "DHALI_PUBLIC_ADDRESSES": {
            "XRPL.TESTNET": {"XRP": {"wallet_id": "rDest"}}
        }
    }

def test_xrpl_manager_init_config_error(mock_wallet, mock_rpc, currency):
    bad_config = {"DHALI_PUBLIC_ADDRESSES": {}}
    with pytest.raises(ValueError, match="Destination address not found"):
        DhaliXrplChannelManager(mock_wallet, mock_rpc, currency, public_config=bad_config)

def test_find_channel_pagination(mock_wallet, mock_rpc, currency, public_config):
    manager = DhaliXrplChannelManager(mock_wallet, mock_rpc, currency, public_config=public_config)
    manager._retrieve_channel_id_from_firestore = MagicMock(return_value="TARGET_ID")
    
    # Mock pagination: page 1 has marker and no target, page 2 has target
    def mock_request(req):
        if isinstance(req, AccountChannels):
            if not req.marker:
                return MagicMock(result={
                    "channels": [{"channel_id": "OTHER_ID"}],
                    "marker": "MARKER_1"
                })
            elif req.marker == "MARKER_1":
                return MagicMock(result={
                    "channels": [{"channel_id": "TARGET_ID"}],
                })
        return MagicMock(result={})
    
    mock_rpc.request.side_effect = mock_request
    
    channel = manager._find_channel()
    assert channel["channel_id"] == "TARGET_ID"
    assert mock_rpc.request.call_count == 2

def test_find_channel_not_found(mock_wallet, mock_rpc, currency, public_config):
    manager = DhaliXrplChannelManager(mock_wallet, mock_rpc, currency, public_config=public_config)
    manager._retrieve_channel_id_from_firestore = MagicMock(return_value="TARGET_ID")
    
    mock_rpc.request.return_value = MagicMock(result={"channels": []})
    
    with pytest.raises(ChannelNotFound, match="not found on-chain"):
        manager._find_channel()

def test_get_auth_token_excessive_amount(mock_wallet, mock_rpc, currency, public_config):
    manager = DhaliXrplChannelManager(mock_wallet, mock_rpc, currency, public_config=public_config)
    manager._find_channel = MagicMock(return_value={"channel_id": "ID", "amount": "100"})
    
    with pytest.raises(ValueError, match="exceeds channel capacity"):
        manager.get_auth_token(amount=200)

@patch("dhali.dhali_xrpl_channel_manager.build_paychan_auth_hex_string_to_be_signed")
@patch("dhali.dhali_xrpl_channel_manager.sign")
def test_get_auth_token_success(mock_sign, mock_build, mock_wallet, mock_rpc, currency, public_config):
    manager = DhaliXrplChannelManager(mock_wallet, mock_rpc, currency, public_config=public_config)
    manager._find_channel = MagicMock(return_value={"channel_id": "ID", "amount": "1000"})
    
    mock_build.return_value = "hex_claim"
    mock_sign.return_value = "signature"
    
    token_b64 = manager.get_auth_token(amount=500)
    token = json.loads(base64.b64decode(token_b64).decode())
    
    assert token["authorized_to_claim"] == "500"
    assert token["signature"] == "signature"
    assert token["channel_id"] == "ID"
