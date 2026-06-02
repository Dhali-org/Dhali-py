import pytest
import base64
import json
from dhali.utils import wrap_as_x402_payment_payload

def test_wrap_as_x402_payment_payload_robustness():
    claim = base64.b64encode(json.dumps({"test": "claim"}).encode()).decode()
    
    # 1. Invalid base64
    with pytest.raises(ValueError, match="Invalid base64 or JSON input"):
        wrap_as_x402_payment_payload("invalid", "invalid")
        
    # 2. Empty accepts list
    req_empty_accepts = base64.b64encode(json.dumps({"accepts": []}).encode()).decode()
    with pytest.raises(ValueError, match="Payment requirement 'accepts' list is empty"):
        wrap_as_x402_payment_payload(claim, req_empty_accepts)
        
    # 3. Non-dict price
    req_bad_price = base64.b64encode(json.dumps({"price": "string_not_dict"}).encode()).decode()
    with pytest.raises(ValueError, match="Invalid payment requirement 'price' format"):
        wrap_as_x402_payment_payload(claim, req_bad_price)
        
    # 4. Non-numeric maxTimeoutSeconds (should fall back to default)
    req_bad_timeout = base64.b64encode(json.dumps({"maxTimeoutSeconds": "invalid"}).encode()).decode()
    result_b64 = wrap_as_x402_payment_payload(claim, req_bad_timeout)
    result = json.loads(base64.b64decode(result_b64).decode())
    assert result["accepted"]["maxTimeoutSeconds"] == 1209600
    
    # 5. Missing fields (should fall back to defaults)
    req_minimal = base64.b64encode(json.dumps({}).encode()).decode()
    result_b64 = wrap_as_x402_payment_payload(claim, req_minimal)
    result = json.loads(base64.b64decode(result_b64).decode())
    assert result["accepted"]["amount"] == "0"
    assert result["accepted"]["asset"] == ""

def test_wrap_as_x402_payment_payload_success():
    claim = base64.b64encode(json.dumps({"signature": "sig"}).encode()).decode()
    req = {
        "price": {"amount": 100, "asset": "XRP"},
        "network": "XRPL.TESTNET",
        "payTo": "rAddress"
    }
    req_b64 = base64.b64encode(json.dumps(req).encode()).decode()
    
    result_b64 = wrap_as_x402_payment_payload(claim, req_b64)
    result = json.loads(base64.b64decode(result_b64).decode())
    
    assert result["x402Version"] == 2
    assert result["payload"]["signature"] == "sig"
    assert result["accepted"]["amount"] == "100"
    assert result["accepted"]["asset"] == "XRP"
    assert result["accepted"]["network"] == "XRPL.TESTNET"
