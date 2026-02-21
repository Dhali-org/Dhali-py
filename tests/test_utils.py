import base64
import json
from dhali.utils import wrap_as_x402_payment_payload

def test_wrap_as_x402_payment_payload():
    mock_claim = {
        "version": "2",
        "account": "rAccount",
        "protocol": "XRPL.MAINNET",
        "currency": {"code": "XRP", "scale": 6},
        "destination_account": "rDest",
        "authorized_to_claim": "1000000",
        "channel_id": "chan123",
        "signature": "sig123",
    }
    claim_base64 = base64.b64encode(json.dumps(mock_claim).encode("utf-8")).decode("utf-8")

    mock_req_full = {
        "accepts": [
            {
                "scheme": "dhali",
                "network": "xrpl:0",
                "asset": "xrpl:0/native:xrp",
                "amount": "70",
                "pay_to": "rLggTEwmTe3eJgyQbCSk4wQazow2TeKrtR",
                "max_timeout_seconds": 1209600,
                "extra": {},
            }
        ]
    }
    req_full_base64 = base64.b64encode(json.dumps(mock_req_full).encode("utf-8")).decode("utf-8")

    result_base64 = wrap_as_x402_payment_payload(claim_base64, req_full_base64)
    result = json.loads(base64.b64decode(result_base64).decode("utf-8"))

    assert result["x402Version"] == 2
    assert result["payload"] == mock_claim
    assert result["accepted"]["amount"] == "70"
    assert result["accepted"]["payTo"] == "rLggTEwmTe3eJgyQbCSk4wQazow2TeKrtR"

def test_wrap_as_x402_payment_payload_dhali_app():
    mock_claim = {"foo": "bar"}
    claim_base64 = base64.b64encode(json.dumps(mock_claim).encode("utf-8")).decode("utf-8")

    mock_req_dhali_app = {
        "scheme": "dhali",
        "network": "eip155:1",
        "payTo": "0x3D85634D9EA2854F4276eE5372Bf32Eb4ACDbf77",
        "price": {
            "amount": "10000000000",
            "asset": "eip155:1/erc20:0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD",
        },
        "maxTimeoutSeconds": 1209600,
        "extra": {},
    }
    req_dhali_app_base64 = base64.b64encode(json.dumps(mock_req_dhali_app).encode("utf-8")).decode("utf-8")

    result_base64 = wrap_as_x402_payment_payload(claim_base64, req_dhali_app_base64)
    result = json.loads(base64.b64decode(result_base64).decode("utf-8"))

    assert result["accepted"]["amount"] == "10000000000"
    assert result["accepted"]["asset"] == "eip155:1/erc20:0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD"
    assert result["accepted"]["payTo"] == "0x3D85634D9EA2854F4276eE5372Bf32Eb4ACDbf77"
