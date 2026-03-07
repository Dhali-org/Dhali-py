[![Package Tests](https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml/badge.svg)](https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml)
[![Release](https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml/badge.svg)](https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml)


# dhali-py

A Python library for managing payment channels (XRPL & Ethereum) and generating auth tokens (i.e., payment-claims) for use with [Dhali](https://dhali.io) APIs. 

Includes support for **Machine-to-Machine (M2M) payments** using seamless off-chain claims.

---

## Installation

```bash
pip install dhali-py
```

---

## Quick Start: Machine-to-Machine Payments

### 1. XRPL

All signing is performed locally using `xrpl-py`.

```python
from dhali.dhali_channel_manager import DhaliChannelManager, ChannelNotFound
from dhali.config_utils import get_available_dhali_currencies
from xrpl.wallet import Wallet
from xrpl.clients import JsonRpcClient

seed    = "sXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
wallet  = Wallet.from_secret(seed=seed)
rpc_client = JsonRpcClient("https://testnet.xrpl-labs.com/")

currencies = get_available_dhali_currencies()
# Select the correct currency from the list
xrpl_testnet = next(c for c in currencies if c.network == "XRPL.TESTNET" and c.code == "XRP")

# Use the Factory to get an XRPL Manager
manager = DhaliChannelManager.xrpl(
    wallet=wallet, 
    rpc_client=rpc_client, 
    currency=xrpl_testnet
)

# Generate a claim (Base64 encoded)
try:
    claim = manager.get_auth_token()
except ChannelNotFound:
    manager.deposit(1_000_000)          # deposit 1 XRP
    claim = manager.get_auth_token()    # 🔑 regenerate after deposit
print("XRPL Claim:", claim)
```

### 2. Ethereum (EVM)

Supports Ethereum, Sepolia, Holesky, etc. Requires `eth-account` and `web3.py`.

```python
from dhali.dhali_channel_manager import DhaliChannelManager, ChannelNotFound
from dhali.config_utils import get_available_dhali_currencies
from eth_account import Account
from web3 import Web3

# 1. Setup Account & Provider
private_key = "0x..."
account = Account.from_key(private_key)
w3 = Web3(Web3.HTTPProvider("https://ethereum-sepolia.publicnode.com"))

# 2. Fetch Available Currencies
currencies = get_available_dhali_currencies()
sepolia_rlusd = next(c for c in currencies if c.network == "SEPOLIA" and c.code == "RLUSD") # or "USDC"

# 3. Instantiate Manager with Dynamic Config
manager = DhaliChannelManager.evm(
    account=account,
    w3=w3,
    currency=sepolia_rlusd
)

# 4. Generate EIP-712 Signed Claim
amount = int(0.1 * 10**sepolia_rlusd.scale) # 0.1 RLUSD

try:
    claim = manager.get_auth_token() 
except ChannelNotFound:
    manager.deposit(amount)
    claim = manager.get_auth_token(amount=amount) 
print("EVM Claim:", claim)
```

---

## Usage in API Calls

Once you have the claim string, include it in your API request query parameters or headers as required by the Dhali Gateway.

```python
import requests

url = f"https://xrplcluster.dhali.io?payment-claim={claim}"
response = requests.post(url, json={"data": "..."})

if response.status_code == 402:
    print("Payment Required: Channel may need topping up.")
```

## Standardized x402 Payments

For APIs that follow the x402 standard, you must wrap your auth token (claim) with the payment requirement (retrieved from the `payment-required` header of a 402 response).

```python
from dhali import wrap_as_x402_payment_payload

# 1. Get your claim as usual
claim = manager.get_auth_token()

# 2. Get the payment requirement from the 'payment-required' header of a 402 response
payment_requirement = response.headers.get("payment-required") 

# 3. Wrap into an x402 payload
x402_payload = wrap_as_x402_payment_payload(claim, payment_requirement)

# 4. Use 'x402_payload' in the 'Payment' header
```

---

## Classes

### `DhaliChannelManager` (Factory)

* `xrpl(wallet, rpc_client, currency, http_client=None, public_config=None) -> DhaliXrplChannelManager`
* `evm(account, w3, currency, http_client=None, public_config=None) -> DhaliEthChannelManager`

### `get_available_dhali_currencies()`

Fetches current Dhali configuration and returns a list of currencies:
```python
[
    Currency(network="SEPOLIA", code="USDC", scale=6, token_address="..."),
    ...
]
```

Here’s a clean, minimal addition you can append near the end of the README (for example, just before the “Classes” section or after it):

---

## Async Workflows

Currently, `dhali-py` provides a synchronous interface for managing payment channels and generating claims.

If you would like to see **native async/await support** (e.g., `async` workflows using `asyncio`, async Web3 providers, or async XRPL clients), please drop us a message and let us know. Community feedback helps us prioritise features 🚀

---
