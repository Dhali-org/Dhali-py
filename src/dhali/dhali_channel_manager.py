import base64
import json
from typing import Optional, Dict, Any
from dhali.create_signed_claim import (
    build_paychan_auth_hex_string_to_be_signed,
)
from xrpl.clients import JsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests.account_channels import AccountChannels
from xrpl.models.transactions import (
    PaymentChannelCreate,
    PaymentChannelFund,
)
from xrpl.core.keypairs import sign
from xrpl.transaction import submit_and_wait


class ChannelNotFound(Exception):
    pass


class DhaliChannelManager:
    """
    A management tool for generating payment claims for use with Dhali APIs.
    """

    def __init__(self, wallet: Wallet):
        self.client = JsonRpcClient("https://s1.ripple.com:51234/")
        self.wallet = wallet
        self.destination = "rLggTEwmTe3eJgyQbCSk4wQazow2TeKrtR"
        self.protocol = "XRPL.MAINNET"

    def _find_channel(self) -> Dict[str, Any]:
        req = AccountChannels(
            account=self.wallet.classic_address,
            destination_account=self.destination,
            ledger_index="validated",
        )
        resp = self.client.request(req)
        channels = resp.result.get("channels", [])
        if not channels:
            raise ChannelNotFound(
                f"No open payment channel from "
                f"{self.wallet.classic_address} to {self.destination}"
            )
        return channels[0]

    def deposit(self, amount_drops: int) -> Dict[str, Any]:
        try:
            ch = self._find_channel()
            tx = PaymentChannelFund(
                account=self.wallet.classic_address,
                channel=ch["channel_id"],
                amount=str(amount_drops),
            )
        except ChannelNotFound:
            tx = PaymentChannelCreate(
                account=self.wallet.classic_address,
                destination=self.destination,
                amount=str(amount_drops),
                settle_delay=86400 * 14,  # 2 weeks
                public_key=self.wallet.public_key,
            )
        result = submit_and_wait(tx, self.client, self.wallet)
        return result.result

    def get_auth_token(self, amount_drops: Optional[int] = None) -> str:
        ch = self._find_channel()
        total_amount = int(ch["amount"])
        allowed = amount_drops if amount_drops is not None else total_amount
        if allowed > total_amount:
            raise ValueError(
                f"Requested auth {allowed} exceeds channel capacity {total_amount}"
            )
        claim = build_paychan_auth_hex_string_to_be_signed(
            channel_id_hex=ch["channel_id"], amount_str=str(allowed)
        )
        signed_claim = sign(claim, self.wallet.private_key)
        claim = {
            "version": "2",
            "account": self.wallet.classic_address,
            "protocol": self.protocol,
            "currency": {"code": "XRP", "scale": 6},
            "destination_account": self.destination,
            "authorized_to_claim": str(allowed),
            "channel_id": ch["channel_id"],
            "signature": signed_claim,
        }
        return base64.b64encode(json.dumps(claim).encode("utf-8")).decode("utf-8")
