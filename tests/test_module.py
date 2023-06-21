import os
import unittest
from mockito import when
import requests
from dhali.module import Module
from dhali.payment_claim_generator import (
    get_xrpl_wallet,
    get_xrpl_payment_claim,
)
from unittest.mock import patch, MagicMock
import tempfile
import gzip
from io import BytesIO, BufferedIOBase


class MockBufferedIOBase(BufferedIOBase):
    def __init__(self, content):
        self.content = content

    def read(self, *args, **kwargs):
        return self.content


class TestModule(unittest.TestCase):
    @classmethod
    @patch("dhali.payment_claim_generator.get_xrpl_wallet", return_value=MagicMock())
    def setUpClass(cls, mock_get_xrpl_wallet):
        cls.some_wallet = mock_get_xrpl_wallet.return_value

    def setUp(self):
        self.some_asset_uuid = "some_asset_uuid"
        self.payment_claim = {
            "account": "some_account",
            "destination_account": "some_other_account",
            "authorized_to_claim": "9000000",
            "signature": "some_signature",
            "channel_id": "some_channel_id",
        }
        self.some_url = "https://dhali.io"
        self.test_module = Module(self.some_asset_uuid)

    @patch.object(requests, "put")
    def test_success_with_valid_file(self, mock_put):
        mock_put.return_value.status_code = 200
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            temp_file_name = f.name
            f.write(self.some_url.encode("utf-8"))
        with open(temp_file_name, "rb") as f:
            response = self.test_module.run(f, self.payment_claim)
            self.assertEqual(response.status_code, 200)

        os.unlink(temp_file_name)

    @patch.object(requests, "put")
    def test_success_with_BytesIO_object(self, mock_put):
        mock_put.return_value.status_code = 200
        file_like_object = BytesIO(self.some_url.encode("utf-8"))
        response = self.test_module.run(file_like_object, self.payment_claim)
        self.assertEqual(response.status_code, 200)

    @patch.object(requests, "put")
    def test_success_with_gzip_file(self, mock_put):
        mock_put.return_value.status_code = 200
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".gz", delete=False) as f:
            temp_file_name = f.name
            f.write(self.some_url.encode("utf-8"))
        with open(temp_file_name, "rb") as f:
            response = self.test_module.run(f, self.payment_claim)
            self.assertEqual(response.status_code, 200)

        os.unlink(temp_file_name)

    @patch.object(requests, "put")
    def test_success_with_BufferedIOBase_object(self, mock_put):
        mock_put.return_value.status_code = 200
        file_like_object = MockBufferedIOBase(self.some_url)
        response = self.test_module.run(file_like_object, self.payment_claim)
        self.assertEqual(response.status_code, 200)

    @patch.object(requests, "put")
    def test_failure_with_invalid_file(self, mock_put):
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(self.some_url)
            f.seek(0)
            when(requests).put(...).thenReturn(requests.Response())
            with self.assertRaises(ValueError):
                self.test_module.run(f, self.payment_claim)

    def test_error_402(self):
        b = BytesIO(self.some_url.encode("utf-8"))
        response_mock = requests.Response()
        response_mock.status_code = 402
        when(requests).put(...).thenReturn(response_mock)
        response = self.test_module.run(b, self.payment_claim)
        self.assertEqual(response.status_code, 402)

    def test_error_without_read_method(self):
        class InvalidFile:
            pass

        invalid_file = InvalidFile()
        with self.assertRaises(AttributeError):
            self.test_module.run(invalid_file, self.payment_claim)


if __name__ == "__main__":
    unittest.main()
