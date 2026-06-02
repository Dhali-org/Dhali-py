import base64
import json

def wrap_as_x402_payment_payload(claim_base_64: str, payment_requirement_base_64: str) -> str:
    """
    Wraps a base64 encoded claim and a base64 encoded requirement into an x402 compliant payload.

    Args:
        claim_base_64: The base64 encoded claim string.
        payment_requirement_base_64: The base64 encoded payment requirement string.

    Returns:
        The base64 encoded x402 payment payload string.
    """
    try:
        decoded_claim = json.loads(base64.b64decode(claim_base_64).decode("utf-8"))
        req = json.loads(base64.b64decode(payment_requirement_base_64).decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Invalid base64 or JSON input: {e}")

    if "accepts" in req:
        accepts = req["accepts"]
        if isinstance(accepts, list):
            if not accepts:
                raise ValueError("Payment requirement 'accepts' list is empty")
            req = accepts[0]
        else:
            req = accepts
            
    if not isinstance(req, dict):
        raise ValueError("Invalid payment requirement format: expected a dictionary")

    price = req.get("price")
    if price and not isinstance(price, dict):
        raise ValueError("Invalid payment requirement 'price' format: expected a dictionary")

    asset = req.get("asset") or (price.get("asset") if price else "") or ""
    amount = str(req.get("amount") or (price.get("amount") if price else "0"))
    
    timeout_raw = req.get("maxTimeoutSeconds") or req.get("max_timeout_seconds") or 1209600
    try:
        max_timeout = int(timeout_raw)
    except (ValueError, TypeError):
        max_timeout = 1209600

    normalized_req = {
        "scheme": req.get("scheme") or "",
        "network": req.get("network") or "",
        "asset": asset,
        "amount": amount,
        "payTo": req.get("payTo") or req.get("pay_to") or "",
        "maxTimeoutSeconds": max_timeout,
        "extra": req.get("extra") if isinstance(req.get("extra"), dict) else {},
    }

    x402_payload = {
        "x402Version": 2,
        "payload": decoded_claim,
        "accepted": normalized_req,
    }

    return base64.b64encode(json.dumps(x402_payload).encode("utf-8")).decode("utf-8")
