import requests
import time
import logging
from typing import Dict, Any, NamedTuple, Optional, List
from dhali.currency import Currency


class NetworkCurrencyConfig(NamedTuple):
    currency: Currency
    destination_address: str


_PUBLIC_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def get_available_dhali_currencies(http_client=requests) -> List[Currency]:
    """
    Fetches and parses available Dhali currencies and configurations.

    Returns:
        A list of Currency objects.
    """
    global _PUBLIC_CONFIG_CACHE
    if _PUBLIC_CONFIG_CACHE:
        data = _PUBLIC_CONFIG_CACHE
    else:
        url = "https://raw.githubusercontent.com/Dhali-org/Dhali-config/master/public.prod.json"
        try:
            response = http_client.get(url, timeout=10, headers={'Connection': 'close'})
            response.raise_for_status()
            data = response.json()
            _PUBLIC_CONFIG_CACHE = data
        except Exception as e:
            raise RuntimeError(f"Failed to fetch Dhali configuration: {e}")

    public_addresses = data.get("DHALI_PUBLIC_ADDRESSES", {})
    result: List[Currency] = []

    for network, currencies in public_addresses.items():
        for code, details in currencies.items():
            token_address = details.get(
                "issuer"
            )  # Use 'issuer' for token address, None if missing
            scale = details.get("scale", 6)
            destination = details.get("wallet_id")

            if not destination:
                continue

            # Native tokens (no issuer) should have token_address as None
            # The config uses 'issuer' for tokens.
            curr = Currency(network=network, code=code, scale=scale, token_address=token_address)
            result.append(curr)

    return result


def get_public_config(http_client=requests) -> Dict[str, Any]:
    """
    Fetches the raw Dhali public configuration JSON.
    """
    global _PUBLIC_CONFIG_CACHE
    if _PUBLIC_CONFIG_CACHE:
        return _PUBLIC_CONFIG_CACHE
    url = "https://raw.githubusercontent.com/Dhali-org/Dhali-config/master/public.prod.json"
    try:
        response = http_client.get(url, timeout=10, headers={'Connection': 'close'})
        response.raise_for_status()
        _PUBLIC_CONFIG_CACHE = response.json()
        return _PUBLIC_CONFIG_CACHE
    except Exception as e:
        raise RuntimeError(f"Failed to fetch Dhali configuration: {e}")


def get_amount_for_currency(canonical_amount: float, curency: Currency):
    return int(canonical_amount * 10**curency.scale)


FIREBASE_CONFIG = {
    "apiKey": "AIzaSyBro8QN3zyJwyo92lYUMPwsyRVPLLGOTcs",
    "authDomain": "dhali-prod.firebaseapp.com",
    "projectId": "dhali-prod",
    "storageBucket": "dhali-prod.firebasestorage.app",
    "messagingSenderId": "1042340549063",
    "appId": "1:1042340549063:web:3dc69cffe6d3c0746189e2",
    "measurementId": "G-6TPZFK7NQ6",
}


def query_public_claim_info_rest(
    protocol: str,
    currency_identifier: str,
    account_address: str,
    http_client=requests,
) -> Optional[str]:
    """
    Queries Firestore via REST API using the public API key.
    """
    project_id = FIREBASE_CONFIG["projectId"]
    api_key = FIREBASE_CONFIG["apiKey"]
    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/public_claim_info/{protocol}:runQuery?key={api_key}"

    query = {
        "structuredQuery": {
            "from": [{"collectionId": currency_identifier}],
            "where": {
                "compositeFilter": {
                    "op": "AND",
                    "filters": [
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": "account"},
                                "op": "EQUAL",
                                "value": {"stringValue": account_address},
                            }
                        },
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": "closed"},
                                "op": "NOT_EQUAL",
                                "value": {"booleanValue": True},
                            }
                        },
                    ],
                }
            },
        }
    }

    try:
        response = http_client.post(url, json=query, timeout=10)
        response.raise_for_status()
        results = response.json()

        for result in results:
            doc = result.get("document")
            if not doc:
                continue
            fields = doc.get("fields", {})

            # Check logic for closing/closed (mirroring existing logic)
            closing = fields.get("closing", {}).get("booleanValue", False)
            closed = fields.get("closed", {}).get("booleanValue", False)

            if closing or closed:
                continue

            channel_id = fields.get("channel_id", {}).get("stringValue")
            if channel_id:
                return channel_id

    except Exception:
        return None

    return None


def is_evm_protocol(protocol: str) -> bool:
    return protocol.upper() in ["ETHEREUM", "SEPOLIA", "HOLESKY", "HARDHAT"]


def notify_admin_gateway(
    protocol: str,
    currency_identifier: str,
    account_address: str,
    channel_id: str,
    http_client=requests,
):
    """
    Proactively notifies the Dhali Admin Gateway about a new payment channel.
    """
    public_config = get_public_config(http_client=http_client)
    root_url = public_config.get("ROOT_API_ADMIN_URL")
    if not root_url:
        return

    # Convert wss:// or ws:// to https:// or http://
    http_root_url = root_url.replace("wss://", "https://").replace("ws://", "http://")
    url = f"{http_root_url}/public_claim_info/{protocol}/{currency_identifier}"

    if is_evm_protocol(protocol) and not channel_id.startswith("0x"):
        channel_id = f"0x{channel_id}"

    payload = {
        "account": account_address.lower() if is_evm_protocol(protocol) else account_address,
        "channel_id": channel_id,
    }

    
    retry_count = 0
    max_retries = 10
    delay = 1.0

    while retry_count <= max_retries:
        try:
            response = http_client.put(url, json=payload, timeout=10)
            if 200 <= response.status_code < 300:
                return
        except Exception:
            pass

        if retry_count < max_retries:
            time.sleep(delay)
            delay *= 2
        retry_count += 1
    
    logger.error(f"Failed to notify public claim info after {max_retries} retries.")

