import base64
import json
from typing import Optional, Dict, Any, Union
from dhali.create_signed_claim import (
    build_paychan_auth_hex_string_to_be_signed,
)
from .payment_channel_manager import PaymentChannelManager, ChannelNotFound
from dhali.currency import Currency
from xrpl.clients import JsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests.account_channels import AccountChannels
from xrpl.models.transactions import (
    PaymentChannelCreate,
    PaymentChannelFund,
)
from xrpl.core.keypairs import sign
from xrpl.transaction import submit_and_wait

import requests
from dhali.config_utils import (
    get_public_config,
    query_public_claim_info_rest,
    notify_admin_gateway,
)


class DhaliXrplChannelManager(PaymentChannelManager):
    """
    A management tool for generating payment claims for use with Dhali APIs (XRPL).
    """

    def __init__(
        self,
        wallet: Wallet,
        rpc_client: JsonRpcClient,
        currency: Currency,
        http_client: Optional[Any] = requests,
        public_config: Optional[Dict[str, Any]] = None,
    ):
        self.wallet = wallet
        self.rpc_client = rpc_client
        self.currency = currency
        self.http_client = http_client or requests
        self.public_config = public_config or get_public_config(
            http_client=self.http_client
        )

        # Resolve destination address
        try:
            self.destination = self.public_config["DHALI_PUBLIC_ADDRESSES"][
                self.currency.network
            ][self.currency.code]["wallet_id"]
        except (KeyError, TypeError):
            raise ValueError(
                "Destination address not found in public_config for this protocol/currency"
            )

    def _retrieve_channel_id_from_firestore(self) -> Optional[str]:
        currency_identifier = self.currency.code
        if self.currency.token_address:
            currency_identifier = (
                f"{self.currency.code}.{self.currency.token_address}"
            )

        return query_public_claim_info_rest(
            self.currency.network,
            currency_identifier,
            self.wallet.classic_address,
            http_client=self.http_client,
        )

    def _retrieve_channel_id_from_firestore_with_polling(self, timeout: int = 30) -> Optional[str]:
        """
        Polls Firestore until a channel ID is found or timeout is reached.
        """
        import time
        start_time = time.time()
        while time.time() - start_time < timeout:
            channel_id = self._retrieve_channel_id_from_firestore()
            if channel_id:
                return channel_id
            time.sleep(2)
        return None

    def _find_channel(self, timeout: int = 0) -> Dict[str, Any]:
        # Prioritize Firestore
        if timeout > 0:
            firestore_channel_id = self._retrieve_channel_id_from_firestore_with_polling(timeout=timeout)
        else:
            firestore_channel_id = self._retrieve_channel_id_from_firestore()
        
        if firestore_channel_id is None:
            raise ChannelNotFound(
                f"No open payment channel from "
                f"{self.wallet.classic_address} to {self.destination}"
            )

        marker = None
        while True:
            req = AccountChannels(
                account=self.wallet.classic_address,
                destination_account=self.destination,
                ledger_index="validated",
                marker=marker,
            )
            resp = self.rpc_client.request(req)
            channels = resp.result.get("channels", [])

            for ch in channels:
                if ch["channel_id"] == firestore_channel_id:
                    return ch
            
            marker = resp.result.get("marker")
            if not marker:
                break

        raise ChannelNotFound(
            f"Firestore channel {firestore_channel_id} not found on-chain for "
            f"{self.wallet.classic_address} to {self.destination}"
        )

    def deposit(self, amount: int) -> Dict[str, Any]:
        tx: Union[PaymentChannelFund, PaymentChannelCreate]
        try:
            ch = self._find_channel(timeout=0)
            tx = PaymentChannelFund(
                account=self.wallet.classic_address,
                channel=ch["channel_id"],
                amount=str(amount),
            )
        except ChannelNotFound:
            tx = PaymentChannelCreate(
                account=self.wallet.classic_address,
                destination=self.destination,
                amount=str(amount),
                settle_delay=86400 * 14,  # 2 weeks
                public_key=self.wallet.public_key,
            )
        result = submit_and_wait(tx, self.rpc_client, self.wallet)

        # If we just created a channel, notify the gateway
        if (
            isinstance(tx, PaymentChannelCreate)
        ):
            # We need to find the channel ID from the result metadata
            # For XRPL, the channel ID is in the transaction metadata
            meta = result.result.get("meta", result.result.get("metaData", {}))
            affected_nodes = meta.get("AffectedNodes", [])
            for node in affected_nodes:
                created_node = node.get("CreatedNode")
                if created_node and created_node.get("LedgerEntryType") == "PayChannel":
                    channel_id = created_node.get("LedgerIndex")
                    if channel_id:
                        currency_identifier = self.currency.code
                        if self.currency.token_address:
                            currency_identifier = (
                                f"{self.currency.code}.{self.currency.token_address}"
                            )

                        notify_admin_gateway(
                            self.currency.network,
                            currency_identifier,
                            self.wallet.classic_address,
                            channel_id,
                            http_client=self.http_client,
                        )
                    break

            # Now poll Firestore to ensure it's indexed (setupBalanceListener behavior)
            self._retrieve_channel_id_from_firestore_with_polling(timeout=30)

        return result.result

    def get_auth_token(self, amount: Optional[int] = None) -> str:
        ch = self._find_channel(timeout=10)
        total_amount = int(ch["amount"])
        allowed = amount if amount is not None else total_amount
        if allowed > total_amount:
            raise ValueError(
                f"Requested auth {allowed} exceeds channel capacity {total_amount}"
            )
        claim = build_paychan_auth_hex_string_to_be_signed(
            channel_id_hex=ch["channel_id"], amount_str=str(allowed)
        )
        signature = sign(claim, self.wallet.private_key)
        claim_dict = {
            "version": "2",
            "account": self.wallet.classic_address,
            "protocol": self.currency.network,
            "currency": {
                "code": self.currency.code,
                "scale": self.currency.scale,
                "issuer": self.currency.token_address,
            },
            "destination_account": self.destination,
            "authorized_to_claim": str(allowed),
            "channel_id": str(ch["channel_id"]),
            "signature": signature,
        }
        return base64.b64encode(json.dumps(claim_dict).encode("utf-8")).decode("utf-8")
