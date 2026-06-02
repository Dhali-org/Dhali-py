import unittest
import base64
import json
from unittest.mock import MagicMock, Mock, patch, ANY
from dhali.dhali_eth_channel_manager import DhaliEthChannelManager
from dhali.currency import Currency


class TestDhaliEthChannelManager(unittest.TestCase):
    def setUp(self):
        self.mock_account = MagicMock()
        self.mock_account.address = "0xSender"
        self.mock_w3 = MagicMock()
        self.mock_w3.eth.get_transaction_count.return_value = 1
        self.mock_w3.eth.gas_price = 10**9
        self.mock_currency = Currency(
            "ETHEREUM", "USDC", 6, "0x0000000000000000000000000000000000000001"
        )
        self.mock_client = MagicMock()
        self.mock_public_config = {
            "DHALI_PUBLIC_ADDRESSES": {
                "ETHEREUM": {
                    "USDC": {"wallet_id": "0x0000000000000000000000000000000000000002"}
                },
                "SEPOLIA": {
                    "USDC": {"wallet_id": "0x0000000000000000000000000000000000000002"}
                },
            },
            "CONTRACTS": {
                "ETHEREUM": {
                    "contract_address": "0x1234567890123456789012345678901234567890"
                },
                "SEPOLIA": {
                    "contract_address": "0x1234567890123456789012345678901234567890"
                },
            },
        }

    def test_init_defaults(self):
        manager = DhaliEthChannelManager(
            account=self.mock_account,
            w3=self.mock_w3,
            currency=self.mock_currency,
            http_client=self.mock_client,
            public_config=self.mock_public_config,
        )
        self.assertEqual(
            manager.destination_address, "0x0000000000000000000000000000000000000002"
        )
        self.assertEqual(
            manager.contract_address, "0x1234567890123456789012345678901234567890"
        )
        self.assertEqual(manager.chain_id, 1)

    @patch("dhali.dhali_eth_channel_manager.get_public_config")
    def test_init_without_config(self, mock_get_config):
        mock_get_config.return_value = self.mock_public_config
        manager = DhaliEthChannelManager(
            account=self.mock_account,
            w3=self.mock_w3,
            currency=self.mock_currency,
            http_client=self.mock_client,
        )
        self.assertEqual(
            manager.destination_address, "0x0000000000000000000000000000000000000002"
        )
        self.assertEqual(manager.chain_id, 1)
        mock_get_config.assert_called_once()

    def test_protocol_name_mapping(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            Currency("SEPOLIA", "USDC", 6, "0x..."),
            self.mock_client,
            self.mock_public_config,
        )
        self.assertEqual(manager.currency.network, "SEPOLIA")
        self.assertEqual(manager.chain_id, 11155111)

    def test_init_without_http_client(self):
        from dhali.config_utils import requests as default_requests
        manager = DhaliEthChannelManager(
            account=self.mock_account,
            w3=self.mock_w3,
            currency=self.mock_currency,
            http_client=None,
            public_config=self.mock_public_config,
        )
        self.assertEqual(manager.http_client, default_requests)

    def test_claim_structure(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        # Mock channel ID resolution from Firestore
        manager._retrieve_channel_id_from_firestore_with_polling = MagicMock(
            return_value="0xChannelId"
        )
        # Mock on-chain amount
        manager._get_on_chain_channel_amount = MagicMock(return_value=1000)
        
        # Mock signing
        mock_signed = MagicMock()
        mock_signed.signature.hex.return_value = "0xSig"
        self.mock_account.sign_message.return_value = mock_signed
        
        claim_str = manager.get_auth_token(100)
        self.assertTrue(isinstance(claim_str, str))

    def test_get_auth_token_optional_amount(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        # Mock channel ID resolution from Firestore
        manager._retrieve_channel_id_from_firestore_with_polling = MagicMock(
            return_value="0xChannelId"
        )
        # Mock on-chain amount
        manager._get_on_chain_channel_amount = MagicMock(return_value=5000)
        
        # Mock signing
        mock_signed = MagicMock()
        mock_signed.signature.hex.return_value = "0xSig"
        self.mock_account.sign_message.return_value = mock_signed

        # Call without amount
        token = manager.get_auth_token()
        decoded = json.loads(base64.b64decode(token).decode("utf-8"))
        
        # Should use amount from on-chain mock
        self.assertEqual(decoded["authorized_to_claim"], "5000")
        self.assertEqual(decoded["channel_id"], "0xChannelId")

    def test_get_auth_token_amount_exceeds_capacity(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        manager._retrieve_channel_id_from_firestore_with_polling = MagicMock(
            return_value="0xChannelId"
        )
        manager._get_on_chain_channel_amount = MagicMock(return_value=1000)
        
        with self.assertRaises(ValueError) as cm:
            manager.get_auth_token(2000)
        
        self.assertIn("exceeds channel capacity", str(cm.exception))

    def test_build_tx(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        self.mock_w3.eth.gas_price = 100
        self.mock_w3.eth.get_transaction_count.return_value = 5
        self.mock_w3.eth.estimate_gas.return_value = 50000

        tx = manager._build_tx("0xTo", "0xData", 1000)

        self.assertEqual(tx["to"], "0xTo")
        self.assertEqual(tx["data"], "0xData")
        self.assertEqual(tx["value"], 1000)
        self.assertEqual(tx["gasPrice"], 110)  # 100 * 1.1
        self.mock_w3.eth.get_transaction_count.assert_called_once_with(
            self.mock_account.address, "pending"
        )
        self.assertEqual(tx["nonce"], 5)
        self.assertEqual(tx["chainId"], 1)
        self.mock_w3.eth.estimate_gas.assert_called_once()
        self.assertEqual(tx["gas"], 55000)  # 50000 * 1.1

    def test_build_tx_estimation_failure(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        self.mock_w3.eth.gas_price = 100
        self.mock_w3.eth.estimate_gas.side_effect = Exception("Estimation failed")

        with self.assertRaises(Exception) as cm:
            manager._build_tx("0xTo", "0xData", 1000)

        self.assertEqual(str(cm.exception), "Estimation failed")

    def test_calculate_channel_id(self):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        self.mock_account.address = "0x0000000000000000000000000000000000000005"
        receiver = "0x0000000000000000000000000000000000000002"
        token = "0x0000000000000000000000000000000000000001"
        nonce = 12345

        # We'll use self.mock_w3.keccak which should be called
        self.mock_w3.keccak.return_value = b"\x01" * 32

        channel_id = manager._calculate_channel_id(receiver, token, nonce)

        self.mock_w3.keccak.assert_called_once()
        self.assertEqual(channel_id, "0x" + ("01" * 32))

    @patch("dhali.dhali_eth_channel_manager.notify_admin_gateway")
    @patch("dhali.dhali_eth_channel_manager.secrets.randbits")
    def test_deposit_with_polling(self, mock_randbits, mock_notify):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        self.mock_account.address = "0x0000000000000000000000000000000000000005"
        mock_randbits.return_value = 54321
        manager._retrieve_channel_id_from_firestore = MagicMock(
            side_effect=[None, None, "found_id"]
        )

        # Mock _send_transaction
        manager._send_transaction = MagicMock(return_value={"status": 1})
        # Mock _calculate_channel_id
        manager._calculate_channel_id = MagicMock(return_value="0xcalc_id")

        # Set a small sleep to speed up test
        with patch("time.sleep"):
            receipt = manager.deposit(100)

        self.assertEqual(receipt, {"status": 1})
        # Should have polled 3 times (None, None, "found_id")
        self.assertEqual(manager._retrieve_channel_id_from_firestore.call_count, 3)

        mock_notify.assert_called_once_with(
            "ETHEREUM",
            "USDC.0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000005",
            "0xcalc_id",
            http_client=self.mock_client,
        )

    @patch("dhali.dhali_eth_channel_manager.notify_admin_gateway")
    def test_deposit_existing_channel(self, mock_notify):
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            self.mock_client,
            self.mock_public_config,
        )
        manager._retrieve_channel_id_from_firestore = MagicMock(return_value="0xExistingId")
        manager._send_transaction = MagicMock(return_value={"status": 1})

        receipt = manager.deposit(100)

        self.assertEqual(receipt, {"status": 1})
        mock_notify.assert_not_called()

    @patch("dhali.dhali_eth_channel_manager.query_public_claim_info_rest")
    def test_firestore_query_uses_injected_http_client(self, mock_query_rest):
        mock_http = MagicMock()
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            http_client=mock_http,
            public_config=self.mock_public_config,
        )
        mock_query_rest.return_value = "0xInjectedChannel"
        
        channel_id = manager._retrieve_channel_id_from_firestore()
        
        self.assertEqual(channel_id, "0xInjectedChannel")
        mock_query_rest.assert_called_once_with(
            manager.currency.network, ANY, self.mock_account.address.lower(), http_client=mock_http
        )

    @patch("dhali.dhali_eth_channel_manager.query_public_claim_info_rest")
    def test_firestore_query_uses_default_http_client(self, mock_rest):
        import requests
        manager = DhaliEthChannelManager(
            self.mock_account,
            self.mock_w3,
            self.mock_currency,
            http_client=None,
            public_config=self.mock_public_config,
        )
        mock_rest.return_value = "0xDefaultRestChannel"
        
        channel_id = manager._retrieve_channel_id_from_firestore()
        
        self.assertEqual(channel_id, "0xDefaultRestChannel")
        mock_rest.assert_called_once_with(
            manager.currency.network, ANY, self.mock_account.address.lower(), http_client=requests
        )


