# create a network client
from xrpl.clients import JsonRpcClient
from xrpl.models.transactions import PaymentChannelCreate
import xrpl
from xrpl.wallet import generate_faucet_wallet, Wallet
import json


client = JsonRpcClient("https://s.altnet.rippletest.net:51234/")
# TODO : We may want to make these CLI arguments in the future
cancel_after = 1000
settle_delay = 15768000

def get_xrpl_wallet():
    return generate_faucet_wallet(client)

def get_xrpl_payment_claim(source_wallet_secret: str, destination_classic_address: str, auth_claim_amount: str, sequence: int, total_xrp_in_channel: str):
    ###################################################################
    #           create a pair of wallets on the testnet               #
    ###################################################################
    source_wallet = Wallet(seed=source_wallet_secret, sequence=sequence)

    ###################################################################
    #           https://xrpl.org/use-payment-channels.html            #
    ###################################################################
    #           Submit a payment channel create transaction           #
    ###################################################################
    last_ledger_sequence = xrpl.ledger.get_latest_validated_ledger_sequence(client) + 10
    fee = xrpl.ledger.get_fee(client=client, fee_type="dynamic")

    # TODO: We do not want to be creating a new payment channel each time this function is called.
    # Let's instead give the payment channel as an argument to the function.  We can also validate that only
    # one payment channel is open between the source classic address and Dhali somewhere else in this library.
    payment_channel = PaymentChannelCreate(
        account=source_wallet.classic_address,
        amount=total_xrp_in_channel,
        destination=destination_classic_address,
        public_key=source_wallet.public_key,
        settle_delay=settle_delay,
        last_ledger_sequence=last_ledger_sequence,
        sequence=source_wallet.sequence,
        fee=fee,
    )

    signed_transaction = xrpl.transaction.safe_sign_transaction(
        payment_channel, source_wallet, check_fee=False
    )


    xrpl.transaction.send_reliable_submission(signed_transaction, client)

    ###################################################################
    #           Check payment channel created and contains funds      #
    ###################################################################
    account_channels = xrpl.models.requests.AccountChannels(
        account=source_wallet.classic_address,
        destination_account=destination_classic_address,
    )

    account_channels_response = client.request(account_channels)

    valid_channel = False
    for channel in account_channels_response.to_dict()["result"]["channels"]:
        correct_src = channel["account"] == source_wallet.classic_address
        correct_dst = channel["destination_account"] == destination_classic_address
        correct_ant = channel["amount"] == total_xrp_in_channel
        correct_del = channel["settle_delay"] == settle_delay
        channel_expired = "cancel_after" in channel.keys()
        if (
            correct_src
            and correct_dst
            and correct_ant
            and correct_del
            and not channel_expired
        ):
            channel_id = channel["channel_id"]
            valid_channel = True
            break
    if not valid_channel:
        raise Exception("Channel invalid! Do not proceed!")

    ###################################################################
    #           Authorize claims against channel                      #
    ###################################################################
    claim = xrpl.models.requests.ChannelAuthorize(
        amount=auth_claim_amount, channel_id=channel_id, secret=source_wallet.seed
    )
    claim_response = client.request(claim).to_dict()
    signature = claim_response["result"]["signature"]
    dhali_payment_claim = {
        "account": source_wallet.classic_address, 
        "destination_account": destination_classic_address, 
        "authorized_to_claim": auth_claim_amount, 
        "signature": signature, 
        "channel_id": channel_id
    }

    return dhali_payment_claim

def print_xrpl_wallet(args):
    wallet = get_xrpl_wallet()
    print("classic_address: ", wallet.classic_address)
    print("secret_seed: ", wallet.seed)
    print("sequence: ", wallet.sequence)


def print_xrpl_payment_claim(args):
    print(json.dumps(get_xrpl_payment_claim(source_wallet_secret=args.source_secret, 
                               destination_classic_address=args.destination_classic_address, 
                               auth_claim_amount=args.auth_claim_amount, 
                               sequence=int(args.sequence_number),
                               total_xrp_in_channel=args.total_amount_contained_in_channel)))

def cli():
    import argparse

    parser = argparse.ArgumentParser(prog="dhali")
    subparsers = parser.add_subparsers()

    create_xrpl_payment_claim_parser = subparsers.add_parser("create-xrpl-payment-claim")
    create_xrpl_payment_claim_parser.add_argument('-s', '--source_secret')
    create_xrpl_payment_claim_parser.add_argument('-d', '--destination_classic_address')
    create_xrpl_payment_claim_parser.add_argument('-a', '--auth_claim_amount', help="Amount (in drops) that claim authorises to be extracted from the channel (must be less than --total_amount_contained_in_channel)")
    create_xrpl_payment_claim_parser.add_argument('-t', '--total_amount_contained_in_channel', help="Total drops to escrow in the channel (must be less than total  amount of XRP in wallet)")
    create_xrpl_payment_claim_parser.add_argument('-i', '--sequence_number')
    create_xrpl_payment_claim_parser.set_defaults(func=print_xrpl_payment_claim)

    create_xrpl_wallet_parser = subparsers.add_parser("create-xrpl-wallet")
    create_xrpl_wallet_parser.set_defaults(func=print_xrpl_wallet)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()