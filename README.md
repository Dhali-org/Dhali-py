[![Package Tests](https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml/badge.svg)](https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml)
[![Release](https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml/badge.svg)](https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml)


# dhali-py

A Python library for managing XRPL payment channels and generating auth tokens (i.e., payment-claims) for use with [Dhali](https://dhali.io) APIs. Leverages [xrpl-py](https://github.com/XRPLF/xrpl-py) and **only ever performs local signing**—your private key never leaves your environment.

---

## Features

- **Create** or **fund** an XRP Payment Channel on Mainnet  
- **Generate** a base64-encoded payment claim (auth token)  
- **Local signing** with your xrpl-py `Wallet` (no external key exposure)

---

## Installation

```bash
pip install dhali-py
````

---

## Quick Start

```python
import json
import requests

from dhali import ChannelNotFound, DhaliChannelManager
from xrpl.wallet import Wallet

# 1. Load your wallet from secret
seed = "sXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
wallet = Wallet.from_secret(seed=seed)

# 2. Create the manager (uses Mainnet JSON-RPC by default)
dhali_manager = DhaliChannelManager(wallet)

def get_payment_claim():
    try:
        # Get an auth token
        return dhali_manager.get_auth_token()
    except ChannelNotFound:
        # If no channel exists, create one with 1 XRP
        dhali_manager.deposit(1_000_000)
        return dhali_manager.get_auth_token()

# 3. Use the token to call a Dhali-compatible endpoint
for _ in range(2):
    token = get_payment_claim()
    url = f"https://xrplcluster.dhali.io?payment-claim={token}"
    payload = {
        "method": "account_info",
        "params": [{"account": wallet.classic_address, "ledger_index": "validated"}],
        "id": 1,
    }
    resp = requests.post(url, data=json.dumps(payload))

    if resp.status_code == 402:
        # Insufficient claim amount? Top up and retry.
        dhali_manager.deposit(1_000_000)
    else:
        break

print("Result:", resp.json())
```

---

## API Reference

### `DhaliChannelManager(wallet: xrpl.wallet.Wallet)`

Constructor.

* **wallet**: an `xrpl-py` `Wallet` instance loaded from your secret.

---

### `deposit(amount_drops: int) → dict`

* **amount\_drops**
  Number of XRP drops to deposit (e.g. `1_000_000` = 1 XRP).
* **Returns**
  The JSON result of the `PaymentChannelCreate` or `PaymentChannelFund` transaction.

---

### `get_auth_token(amount_drops: Optional[int] = None) → str`

* **amount\_drops** (optional)
  How many drops to authorize in this claim. If omitted, uses the full channel balance.
* **Returns**
  A base64-encoded JSON string containing your signed claim (`version`, `account`, `protocol`, `currency`, `authorized_to_claim`, `channel_id`, `signature`, etc.).
* **Raises**

  * `ChannelNotFound` if no open channel exists
  * `ValueError` if `amount_drops` exceeds the channel’s capacity

---

## Errors

* **ChannelNotFound**
  Thrown when you call `get_auth_token` but no channel exists from your wallet to Dhali’s receiver.
* **ValueError**
  Thrown when you request more drops than the current channel balance.

---

## Security & Signing

* **Local Signing Only**
  All XRPL transactions and auth-claim signatures are generated locally via `xrpl-py`.
* **No External Key Exposure**
  Your private key is never sent over the network or stored externally.

---
