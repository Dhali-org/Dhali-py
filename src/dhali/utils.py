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
    decoded_claim = json.loads(base64.b64decode(claim_base_64).decode("utf-8"))
    req = json.loads(base64.b64decode(payment_requirement_base_64).decode("utf-8"))

    if "accepts" in req:
        accepts = req["accepts"]
        if isinstance(accepts, list):
            req = accepts[0]
        else:
            req = accepts
            
    normalized_req = {
        "scheme": req.get("scheme") or "",
        "network": req.get("network") or "",
        "asset": req.get("asset") or (req.get("price", {}).get("asset") if "price" in req else "") or "",
        "amount": str(req.get("amount") or (req.get("price", {}).get("amount") if "price" in req else "0")),
        "payTo": req.get("payTo") or req.get("pay_to") or "",
        "maxTimeoutSeconds": int(req.get("maxTimeoutSeconds") or req.get("max_timeout_seconds") or 1209600),
        "extra": req.get("extra", {}),
    }

    x402_payload = {
        "x402Version": 2,
        "payload": decoded_claim,
        "accepted": normalized_req,
    }

    return base64.b64encode(json.dumps(x402_payload).encode("utf-8")).decode("utf-8")
