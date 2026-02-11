import base64
import json
import secrets
import time
from typing import Optional, Any, Dict, List, cast
from eth_abi import encode
from eth_account.signers.local import LocalAccount
from web3 import Web3
from web3.types import TxParams, TxReceipt, HexStr

import requests

from dhali.create_signed_claim import get_ethereum_claim_typed_data
from dhali.payment_channel_manager import PaymentChannelManager, ChannelNotFound
from dhali.currency import Currency
from dhali.config_utils import (
    get_public_config,
    query_public_claim_info_rest,
    notify_admin_gateway,
)


class DhaliEthChannelManager(PaymentChannelManager):
    def __init__(
        self,
        account: LocalAccount,
        w3: Web3,
        protocol: str,
        currency: Currency,
        http_client: Optional[Any] = requests,
        public_config: Optional[Dict[str, Any]] = None,
    ):
        self.account = account
        self.w3 = w3
        self.protocol = protocol
        self.currency = currency
        self.http_client = http_client or requests
        self.public_config = public_config or get_public_config(
            http_client=self.http_client
        )
        self.chain_id = self._get_chain_id_from_protocol(protocol)

        # Resolve destination address from config
        try:
            self.destination_address = self.public_config["DHALI_PUBLIC_ADDRESSES"][
                self.protocol
            ][self.currency.code]["wallet_id"]
        except (KeyError, TypeError):
            raise ValueError(
                "Destination address not found in public_config for this protocol/currency"
            )

        # Resolve contract address from config
        try:
            self.contract_address = self.public_config["CONTRACTS"][self.protocol][
                "contract_address"
            ]
        except (KeyError, TypeError):
            raise ValueError(
                "Contract address not found in public_config for this protocol"
            )

        if not self.contract_address:
            raise ValueError(
                f"Contract address must be provided or resolved for protocol: {self.protocol}"
            )

    def _get_chain_id_from_protocol(self, protocol: str) -> int:
        mapping = {
            "ETHEREUM": 1,
            "SEPOLIA": 11155111,
            "HOLESKY": 17000,
            "LOCALHOST": 31337,
        }
        if protocol not in mapping:
            raise ValueError(f"Unsupported protocol: {protocol}")
        return mapping[protocol]

    def _get_protocol_name(self) -> str:
        return self.protocol

    def _retrieve_channel_id_from_firestore(self) -> Optional[str]:
        currency_identifier = self.currency.code
        if self.currency.token_address:
            currency_identifier = (
                f"{self.currency.code}.{self.currency.token_address}"
            )

        return query_public_claim_info_rest(
            self.protocol,
            currency_identifier,
            self.account.address.lower(),
            http_client=self.http_client,
        )

    def _calculate_channel_id(
        self, receiver: str, token_address: str, nonce: int
    ) -> str:
        """
        Calculates the channel ID locally. This matches Dhali-wallet's calculateChannelId.
        Logic: keccak256(padded_sender + padded_receiver + padded_token + padded_nonce)
        """

        sender_bytes = encode(["address"], [self.account.address])
        receiver_bytes = encode(["address"], [receiver])
        token_bytes = encode(["address"], [token_address])
        nonce_bytes = encode(["uint256"], [nonce])

        combined = sender_bytes + receiver_bytes + token_bytes + nonce_bytes
        channel_id_hash = self.w3.keccak(combined)
        return "0x" + channel_id_hash.hex()

    def _retrieve_channel_id_from_firestore_with_polling(
        self, timeout: int = 30
    ) -> Optional[str]:
        """
        Polls Firestore until a channel ID is found or timeout is reached.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            channel_id = self._retrieve_channel_id_from_firestore()
            if channel_id:
                return channel_id
            time.sleep(2)
        return None

    def _encode_address(self, address: str) -> str:
        return address.lower().replace("0x", "").rjust(64, "0")

    def _encode_uint(self, value: int) -> str:
        return hex(value)[2:].rjust(64, "0")

    def _encode_bool(self, value: bool) -> str:
        return "1".rjust(64, "0") if value else "0".rjust(64, "0")

    def _encode_bytes32(self, value: str) -> str:
        return value.replace("0x", "").rjust(64, "0")

    def deposit(self, amount: int) -> TxReceipt:
        """
        Deposits 'amount' (in base units) into a payment channel.
        If an open channel exists, it funds it.
        If not, it opens a new one.
        """
        existing_channel_id = self._retrieve_channel_id_from_firestore()
        token_address = (
            self.currency.token_address or "0x0000000000000000000000000000000000000000"
        )
        is_native = token_address == "0x0000000000000000000000000000000000000000"

        # Selectors
        OPEN_CHANNEL_SELECTOR = "3cd880a5"
        DEPOSIT_SELECTOR = "264d06c8"
        APPROVE_SELECTOR = "095ea7b3"
        SETTLE_DELAY = 1209600  # 2 weeks
        tx_hashes: List[str] = []

        if existing_channel_id:
            # Deposit
            # deposit(bytes32 _channelId, uint256 _amount, bool _renew)
            calldata = (
                "0x"
                + DEPOSIT_SELECTOR
                + self._encode_bytes32(existing_channel_id)
                + self._encode_uint(amount)
                + self._encode_bool(True)  # renew
            )

            # If token, approve first
            if not is_native:
                self._approve_token(token_address, self.contract_address, amount)

            tx = self._build_tx(
                self.contract_address, calldata, amount if is_native else 0
            )
            receipt = self._send_transaction(tx)
            return receipt

        else:
            # Open Channel
            # openChannel(address _receiver, address _token, uint256 _amount, uint256 _settleDelay, uint256 _nonce, address _signer)
            receiver = self.destination_address
            # Generate random nonce
            nonce = secrets.randbits(256)
            dummy_signer = "0x0000000000000000000000000000000000000000"

            calldata = (
                "0x"
                + OPEN_CHANNEL_SELECTOR
                + self._encode_address(receiver)
                + self._encode_address(token_address)
                + self._encode_uint(amount)
                + self._encode_uint(SETTLE_DELAY)
                + self._encode_uint(nonce)
                + self._encode_address(dummy_signer)
            )

            if not is_native:
                self._approve_token(token_address, self.contract_address, amount)

            tx = self._build_tx(
                self.contract_address, calldata, amount if is_native else 0
            )
            receipt = self._send_transaction(tx)

            # If we just created a channel, calculate its ID and notify the gateway
            calculated_channel_id = self._calculate_channel_id(
                receiver, token_address, nonce
            )

            currency_identifier = self.currency.code
            if self.currency.token_address:
                currency_identifier = (
                    f"{self.currency.code}.{self.currency.token_address}"
                )

            notify_admin_gateway(
                self.protocol,
                currency_identifier,
                self.account.address.lower(),
                calculated_channel_id,
                http_client=self.http_client,
            )

            # Now poll Firestore to ensure it's indexed (setupBalanceListener behavior)
            self._retrieve_channel_id_from_firestore_with_polling(timeout=30)

            return receipt

    def _approve_token(self, token_address: str, spender: str, amount: int):
        APPROVE_SELECTOR = "095ea7b3"
        calldata = (
            "0x"
            + APPROVE_SELECTOR
            + self._encode_address(spender)
            + self._encode_uint(amount)
        )
        tx = self._build_tx(token_address, calldata, 0)
        self._send_transaction(tx)

    def _build_tx(self, to: str, data: str, value: int) -> Dict[str, Any]:
        gas_price = self.w3.eth.gas_price
        # Add 10% buffer to avoid 'underpriced' errors on congested or lagging RPCs
        buffered_gas_price = int(gas_price * 1.1)

        tx_params = {
            "from": self.account.address,
            "to": to,
            "value": value,
            "data": data,
            "gasPrice": buffered_gas_price,
            "nonce": self.w3.eth.get_transaction_count(self.account.address, "pending"),
            "chainId": self.chain_id,
        }

        # Estimate gas
        gas_limit = self.w3.eth.estimate_gas(cast(TxParams, tx_params))
        # Add 10% buffer to gas limit as well for safety
        tx_params["gas"] = int(gas_limit * 1.1)

        return tx_params

    def _send_transaction(self, tx: Dict[str, Any]) -> TxReceipt:
        signed_tx = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)

        # Robust polling for receipt (handles transient RPC disconnections)
        import time

        max_retries = 5
        for i in range(max_retries):
            try:
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
                if receipt["status"] != 1:
                    raise Exception(f"Transaction failed: {tx_hash.hex()}")
                return receipt
            except (requests.exceptions.ConnectionError, Exception) as e:
                if i == max_retries - 1:
                    raise e
                time.sleep(2**i)  # Exponential backoff
        raise Exception("Failed to get transaction receipt")

    def _get_on_chain_channel_amount(self, channel_id: str) -> int:
        """
        Calls getChannel(bytes32) on the smart contract to retrieve the total amount.
        Selector: 0x831c2b82
        """
        # Ensure channel_id is 32 bytes hex without 0x
        clean_id = channel_id.replace("0x", "").rjust(64, "0")
        calldata = "0x831c2b82" + clean_id
        
        try:
            result = self.w3.eth.call({
                "to": self.contract_address,
                "data": cast(HexStr, calldata)
            })
            # Based on Dhali-wallet, the amount is the 5th 32-byte word (index 4)
            # result is bytes, so each word is 32 bytes.
            # Index 4 is from byte 128 to 160.
            if len(result) < 160:
                raise Exception(f"Unexpected getChannel return length: {len(result)}")
            
            amount_bytes = result[128:160]
            return int.from_bytes(amount_bytes, byteorder="big")
        except Exception as e:
            raise Exception(f"Failed to retrieve on-chain channel amount: {str(e)}")

    def get_auth_token(self, amount: Optional[int] = None) -> str:
        # Check Firestore for existing channel, polling if necessary
        channel_id = self._retrieve_channel_id_from_firestore_with_polling(timeout=10)
        if not channel_id:
            raise ChannelNotFound(
                "No open payment channel found in Firestore. Please deposit first."
            )

        if not channel_id.startswith("0x"):
            channel_id = "0x" + channel_id

        total_amount = self._get_on_chain_channel_amount(channel_id)
        allowed = amount if amount is not None else total_amount

        if allowed > total_amount:
            raise ValueError(
                f"Requested auth {allowed} exceeds channel capacity {total_amount}"
            )

        token = (
            self.currency.token_address or "0x0000000000000000000000000000000000000000"
        )

        typed_data = get_ethereum_claim_typed_data(
            channel_id=channel_id,
            token_address=token,
            max_amount=str(allowed),
            chain_id=self.chain_id,
            contract_address=self.contract_address,
        )

        signed = self.account.sign_message(typed_data)
        signature = signed.signature.hex()

        claim = {
            "version": "2",
            "account": self.account.address,
            "protocol": self.protocol,
            "currency": {
                "code": self.currency.code,
                "scale": self.currency.scale,
                "issuer": self.currency.token_address,
            },
            "destination_account": self.destination_address,
            "authorized_to_claim": str(allowed),
            "channel_id": channel_id,
            "signature": signature,
        }
        return base64.b64encode(json.dumps(claim).encode("utf-8")).decode("utf-8")
