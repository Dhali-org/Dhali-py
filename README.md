[![Package Tests](https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml/badge.svg)](https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml)
[![Release](https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml/badge.svg)](https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml)


# dhali-py

A Python library for managing XRPL payment channels and generating auth tokens (i.e., payment-claims) for use with [Dhali](https://dhali.io) APIs. Leverages [xrpl-py](https://github.com/XRPLF/xrpl-py) and **only ever performs local signing**—your private key never leaves your environment.

---

## Installation

```bash
pip install dhali-py
```

---

## Quick Start (Python)

```python
# ==== 0. Common setup ====
from dhali import ChannelNotFound, DhaliChannelManager
from xrpl.wallet import Wallet

seed    = "sXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
wallet  = Wallet.from_secret(seed=seed)
manager = DhaliChannelManager(wallet)
```


### 1. Create a Payment Claim

```python
try:
    claim = manager.get_auth_token()
except ChannelNotFound:
    manager.deposit(1_000_000)          # deposit 1 XRP
    claim = manager.get_auth_token()    # 🔑 regenerate after deposit

print("New claim:", claim)
```

### 2. Top Up Later (and Regenerate)

```python
manager.deposit(2_000_000)               # add 2 XRP
updated_claim = manager.get_auth_token()
print("Updated claim:", updated_claim)
```

---

### 3. Using APIs and Handling 402 "Payment Required" Errors

```python
import json, requests

def call_with_claim(max_retries=5):
    for i in range(1, max_retries+1):
        claim = manager.get_auth_token()
        url   = f"https://xrplcluster.dhali.io?payment-claim={claim}"
        resp  = requests.post(url, data=json.dumps({/*…RPC…*/}))

        if resp.status_code != 402:
            return resp

        print(f"Attempt {i}: topping up…")
        manager.deposit(1_000_000)       # deposit 1 XRP

    raise RuntimeError(f"402 after {max_retries} retries")

response = call_with_claim()
print("Result:", response.json())
```

---


## Class reference

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
