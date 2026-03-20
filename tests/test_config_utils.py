import unittest
from unittest.mock import patch, Mock
from dhali.config_utils import get_available_dhali_currencies


class TestConfigUtils(unittest.TestCase):
    @patch("dhali.config_utils.requests.get")
    def test_get_available_dhali_currencies(self, mock_get):
        mock_resp = Mock()
        mock_resp.json.return_value = {
            "DHALI_PUBLIC_ADDRESSES": {
                "TEST_NET": {
                    "TEST_TOKEN": {
                        "wallet_id": "dest123",
                        "scale": 9,
                        "issuer": "iss123",
                    }
                }
            }
        }
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        configs = get_available_dhali_currencies()
        self.assertEqual(len(configs), 1)

        cfg = configs[0]
        self.assertEqual(cfg.network, "TEST_NET")
        self.assertEqual(cfg.code, "TEST_TOKEN")
        self.assertEqual(cfg.scale, 9)
        self.assertEqual(cfg.token_address, "iss123")

    @patch("dhali.config_utils.requests.post")
    def test_query_public_claim_info_rest_success(self, mock_post):
        mock_resp = Mock()
        mock_resp.json.return_value = [
            {
                "document": {
                    "fields": {
                        "channel_id": {"stringValue": "CHAN_REST"},
                        "closed": {"booleanValue": False},
                        "closing": {"booleanValue": False},
                    }
                }
            }
        ]
        mock_resp.raise_for_status = Mock()
        mock_post.return_value = mock_resp

        from dhali.config_utils import query_public_claim_info_rest

        res = query_public_claim_info_rest("XRPL", "XRP", "rADDR")
        self.assertEqual(res, "CHAN_REST")
        mock_post.assert_called_once()

    @patch("dhali.config_utils.get_public_config")
    @patch("dhali.config_utils.requests.put")
    @patch("dhali.config_utils.time.sleep")
    def test_notify_admin_gateway_success(self, mock_sleep, mock_put, mock_get_config):
        mock_get_config.return_value = {"ROOT_API_ADMIN_URL": "wss://admin.gateway"}
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_put.return_value = mock_resp
        from dhali.config_utils import notify_admin_gateway

        notify_admin_gateway("XRPL", "XRP", "rADDR", "CHANID")

        expected_url = "https://admin.gateway/public_claim_info/XRPL/XRP"
        expected_payload = {"account": "rADDR", "channel_id": "CHANID"}

        mock_put.assert_called_once_with(
            expected_url, json=expected_payload, timeout=10
        )


if __name__ == "__main__":
    unittest.main()
