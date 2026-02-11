import binascii
from eth_account.messages import encode_typed_data

HASH_PREFIX_PAYMENT_CHANNEL_CLAIM = 0x434C4D00


def _serialize_paychan_authorization(channel_id_bytes: bytes, drops: int) -> bytes:
    if len(channel_id_bytes) != 32:
        raise Exception(
            f"Invalid channelId length {len(channel_id_bytes)}; must be 32 bytes."
        )
    # 1) 4-byte prefix
    prefix = HASH_PREFIX_PAYMENT_CHANNEL_CLAIM.to_bytes(4, byteorder="big")
    # 2) channelId
    # 3) split drops into two 4-byte words
    high = (drops >> 32) & 0xFFFFFFFF
    low = drops & 0xFFFFFFFF
    amount_bytes = high.to_bytes(4, byteorder="big") + low.to_bytes(4, byteorder="big")
    return prefix + channel_id_bytes + amount_bytes


def build_paychan_auth_hex_string_to_be_signed(
    channel_id_hex: str, amount_str: str
) -> str:
    # parse & validate channel ID
    try:
        channel_id_bytes = binascii.unhexlify(channel_id_hex)
    except (binascii.Error, ValueError):
        raise Exception("Invalid channelId hex.")
    # parse & validate amount
    try:
        drops = int(amount_str)
        if drops < 0:
            raise Exception("Amount cannot be negative.")
    except ValueError:
        raise Exception("Invalid amount format.")

    msg = _serialize_paychan_authorization(channel_id_bytes, drops)
    # upper-case hex for the signed payload
    return binascii.hexlify(msg).decode("utf-8").upper()


def get_ethereum_claim_typed_data(
    channel_id: str,
    token_address: str,
    max_amount: str,
    chain_id: int,
    contract_address: str,
):
    domain = {
        "name": "DhaliPaymentChannel",
        "version": "2.1.0",
        "chainId": chain_id,
        "verifyingContract": contract_address,
    }
    types = {
        "DhaliClaim": [
            {"name": "channelId", "type": "bytes32"},
            {"name": "token", "type": "address"},
            {"name": "maxAmount", "type": "uint256"},
        ],
    }
    if isinstance(channel_id, str) and not channel_id.startswith("0x"):
        channel_id = "0x" + channel_id

    message = {
        "channelId": channel_id,
        "token": token_address,
        "maxAmount": int(max_amount),
    }
    return encode_typed_data(
        domain_data=domain, message_types=types, message_data=message
    )
