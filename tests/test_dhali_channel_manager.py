import base64
import json
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, ANY
import dhali.dhali_xrpl_channel_manager as manager_module
from dhali import DhaliXrplChannelManager as DhaliChannelManager, ChannelNotFound
import dhali.create_signed_claim as create_signed_claim


class FakeWallet:
    def __init__(self):
        self.classic_address = "rTESTADDRESS"
        self.public_key = "PUBKEY"
        self.private_key = "PRIVKEY"


@pytest.fixture
def wallet():
    return FakeWallet()


@pytest.fixture
def rpc_client():
    client = MagicMock()
    client.url = "https://s1.ripple.com:51234/"
    return client


@pytest.fixture
def currency():
    from dhali.currency import Currency

    return Currency("XRP", 6)


@pytest.fixture
def public_config():
    return {
        "DHALI_PUBLIC_ADDRESSES": {
            "XRPL.MAINNET": {"XRP": {"wallet_id": "rnDestination"}}
        }
    }


@pytest.fixture
def manager(wallet, rpc_client, currency, public_config):
    mock_query = MagicMock()
    return DhaliChannelManager(
        wallet=wallet,
        rpc_client=rpc_client,
        protocol="XRPL.MAINNET",
        currency=currency,
        http_client=mock_query,
        public_config=public_config,
    )


def test_init_sets_defaults(wallet, rpc_client, currency, public_config):
    mock_query = MagicMock()
    manager = DhaliChannelManager(
        wallet,
        rpc_client,
        "XRPL.MAINNET",
        currency,
        http_client=mock_query,
        public_config=public_config,
    )
    assert manager.wallet == wallet
    assert manager.rpc_client == rpc_client
    assert manager.currency == currency
    assert manager.http_client == mock_query
    assert manager.public_config == public_config
    assert manager.destination == "rnDestination"


def test_init_without_http_client(wallet, rpc_client, currency, public_config):
    import requests
    manager = DhaliChannelManager(
        wallet=wallet,
        rpc_client=rpc_client,
        protocol="XRPL.MAINNET",
        currency=currency,
        http_client=None,
        public_config=public_config,
    )
    assert manager.http_client == requests

@patch("dhali.dhali_xrpl_channel_manager.query_public_claim_info_rest")
def test_firestore_query_uses_default_http_client(mock_rest, wallet, rpc_client, currency, public_config):
    import requests
    manager = DhaliChannelManager(
        wallet=wallet,
        rpc_client=rpc_client,
        protocol="XRPL.MAINNET",
        currency=currency,
        http_client=None,
        public_config=public_config,
    )
    mock_rest.return_value = "CHAN_REST"
    
    result = manager._retrieve_channel_id_from_firestore()
    assert result == "CHAN_REST"
    mock_rest.assert_called_once_with(
        "XRPL.MAINNET", ANY, wallet.classic_address, http_client=requests
    )


def test_find_channel_success(monkeypatch, manager):
    fake_channel = {"channel_id": "CHAN123", "amount": "1000"}
    # Mock Firestore to return the specific ID
    monkeypatch.setattr(manager, "_retrieve_channel_id_from_firestore", lambda: "CHAN123")
    monkeypatch.setattr(
        manager.rpc_client,
        "request",
        lambda req: SimpleNamespace(result={"channels": [fake_channel]}),
    )
    result = manager._find_channel()
    assert result == fake_channel


def test_find_channel_raises_when_no_channels(monkeypatch, manager, wallet):
    monkeypatch.setattr(manager, "_retrieve_channel_id_from_firestore", lambda: None)
    monkeypatch.setattr(
        manager.rpc_client,
        "request",
        lambda req: SimpleNamespace(result={"channels": []}),
    )
    with pytest.raises(ChannelNotFound) as excinfo:
        manager._find_channel()
    msg = str(excinfo.value)
    assert wallet.classic_address in msg
    assert manager.destination in msg

def test_find_channel_raises_channel_not_found_when_firestore_empty(monkeypatch, manager, wallet):
    # Mock _retrieve_channel_id_from_firestore to return None
    monkeypatch.setattr(manager, "_retrieve_channel_id_from_firestore", lambda: None)
    
    with pytest.raises(ChannelNotFound) as excinfo:
        manager._find_channel()
    msg = str(excinfo.value)
    assert wallet.classic_address in msg
    assert manager.destination in msg


def test_find_channel_raises_on_firestore_id_mismatch(monkeypatch, manager, wallet):
    # Firestore returns one ID
    monkeypatch.setattr(manager, "_retrieve_channel_id_from_firestore", lambda: "FIRESTORE_ID")
    # XRPL returns a different ID
    fake_channel = {"channel_id": "XRPL_ID", "amount": "1000"}
    monkeypatch.setattr(
        manager.rpc_client,
        "request",
        lambda req: SimpleNamespace(result={"channels": [fake_channel]}),
    )
    with pytest.raises(ChannelNotFound) as excinfo:
        manager._find_channel()
    assert "FIRESTORE_ID" in str(excinfo.value)
    assert "not found on-chain" in str(excinfo.value)


def test_deposit_funds_existing_channel(monkeypatch, manager, wallet):
    fake_channel = {"channel_id": "CHANID", "amount": "500"}
    monkeypatch.setattr(manager, "_find_channel", lambda: fake_channel)
    captured = {}

    def fake_submit(tx, client, wallet_arg):
        captured["tx"] = tx
        return SimpleNamespace(result={"status": "funded"})

    # Patch submit_and_wait where it's imported in the module
    monkeypatch.setattr(manager_module, "submit_and_wait", fake_submit)
    result = manager.deposit(100)
    assert result == {"status": "funded"}

    from xrpl.models.transactions import PaymentChannelFund

    assert isinstance(captured["tx"], PaymentChannelFund)
    assert captured["tx"].account == wallet.classic_address
    assert captured["tx"].channel == fake_channel["channel_id"]
    assert captured["tx"].amount == str(100)


def test_deposit_create_channel_when_none(monkeypatch, manager, wallet):
    def raise_not_found():
        raise ChannelNotFound()

    monkeypatch.setattr(manager, "_find_channel", raise_not_found)
    captured = {}

    def fake_submit(tx, client, wallet_arg):
        captured["tx"] = tx
        return SimpleNamespace(result={"status": "created"})

    # Patch submit_and_wait on the DhaliChannelManager module
    monkeypatch.setattr(manager_module, "submit_and_wait", fake_submit)
    result = manager.deposit(200)
    assert result == {"status": "created"}

    from xrpl.models.transactions import PaymentChannelCreate

    tx = captured["tx"]
    assert isinstance(tx, PaymentChannelCreate)
    assert tx.account == wallet.classic_address
    assert tx.destination == manager.destination
    assert tx.amount == str(200)
    assert tx.public_key == wallet.public_key
    assert hasattr(tx, "settle_delay")


def test_get_auth_token_success_default_amount(monkeypatch, manager, wallet):
    fake_channel = {"channel_id": "AB" * 32, "amount": "1001"}
    monkeypatch.setattr(manager, "_find_channel", lambda: fake_channel)
    monkeypatch.setattr(
        create_signed_claim,
        "build_paychan_auth_hex_string_to_be_signed",
        lambda channel_id_hex, amount_str: "CLAIMHEX",
    )
    monkeypatch.setattr(manager_module, "sign", lambda claim, priv_key: "SIGVALUE")

    token = manager.get_auth_token()
    decoded = base64.b64decode(token).decode("utf-8")
    data = json.loads(decoded)
    assert data["version"] == "2"
    assert data["account"] == wallet.classic_address
    assert data["protocol"] == manager.protocol
    assert data["currency"]["code"] == "XRP"
    assert data["currency"]["scale"] == 6
    assert data["destination_account"] == manager.destination
    assert data["authorized_to_claim"] == str(1001)
    assert data["channel_id"] == fake_channel["channel_id"]
    assert data["signature"] == "SIGVALUE"


def test_get_auth_token_with_specific_amount(monkeypatch, manager, wallet):
    fake_channel = {"channel_id": "AB" * 32, "amount": "500"}
    monkeypatch.setattr(manager, "_find_channel", lambda: fake_channel)
    monkeypatch.setattr(
        create_signed_claim,
        "build_paychan_auth_hex_string_to_be_signed",
        lambda channel_id_hex, amount_str: "CLAIMHEX2",
    )
    monkeypatch.setattr(manager_module, "sign", lambda claim, priv_key: "SIG2")

    token = manager.get_auth_token(amount=200)
    decoded = base64.b64decode(token).decode("utf-8")
    data = json.loads(decoded)
    assert data["authorized_to_claim"] == "200"


def test_get_auth_token_amount_exceeds(monkeypatch, manager):
    fake_channel = {"channel_id": "XCHAN", "amount": "100"}
    monkeypatch.setattr(manager, "_find_channel", lambda: fake_channel)
    with pytest.raises(ValueError) as excinfo:
        manager.get_auth_token(amount=200)
    assert "exceeds channel capacity" in str(excinfo.value)


def test_build_paychan_auth_hex_string_to_be_signed_invalid_hex():
    with pytest.raises(Exception) as excinfo:
        create_signed_claim.build_paychan_auth_hex_string_to_be_signed("CHANID", "200")
    assert "Invalid channelId hex." in str(excinfo.value)



