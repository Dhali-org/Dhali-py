import json
import xrpl
import uuid
from fastapi import HTTPException
from google.cloud import firestore

@firestore.transactional
def _transactional_validation(
    transaction,
    public_doc_ref,
    private_doc_ref,
    ledger_client,
    parsed_claim,
    single_request_cost_estimate: int,
    settle_delay,
) -> float:
    private_payment_channels_doc = next(transaction.get(private_doc_ref))
    public_payment_channels_doc = next(transaction.get(public_doc_ref))

    updating_payment_claim = True
    if private_payment_channels_doc.exists:
        # Check if payment claim was previously submitted. If so, we do not need
        # to cryptographically verify it again 
        updating_payment_claim = json.loads(private_payment_channels_doc.to_dict()["payment_claim"]) != parsed_claim
        if private_payment_channels_doc.to_dict()["currency"]["code"] != "XRP":
            raise HTTPException(
                status_code=402,
                detail="Your stored payment channel's currency code is invalid",
            )
        if private_payment_channels_doc.to_dict()["currency"]["scale"] != 0.000001:
            raise HTTPException(
                status_code=402,
                detail="Your stored payment channel's currency scale is invalid",
            )

        to_claim = (
            private_payment_channels_doc.to_dict()["to_claim"] + single_request_cost_estimate
        )
    else:
        to_claim = single_request_cost_estimate

    authorized_to_claim = parsed_claim["authorized_to_claim"]
    if int(authorized_to_claim) < int(to_claim):
        raise HTTPException(
            status_code=402,
            detail=f"Your payment claim is not sufficient to fund this request: authorized_to_claim = {authorized_to_claim}, to_claim = {to_claim}",
        )

    if updating_payment_claim:
        validate(parsed_claim=parsed_claim, ledger_client=ledger_client, settle_delay=settle_delay)

    if private_payment_channels_doc.exists:
        transaction.update(private_doc_ref, {"to_claim": to_claim})
    else:
        # The expectations are:
        # authorized_to_claim >= to_claim
        transaction.set(
            private_doc_ref,
            {
                "authorized_to_claim": parsed_claim["authorized_to_claim"],
                "to_claim": to_claim, # TODO: Remove this once other infra migrated to use public firestore
                "currency": {"code": "XRP", "scale": 0.000001},
                "payment_claim": json.dumps(parsed_claim),
            },
        )

    if public_payment_channels_doc.exists:
        transaction.update(public_doc_ref, {"to_claim": to_claim})
    else:
        # The expectations are:
        # authorized_to_claim >= to_claim
        transaction.set(
            public_doc_ref,
            {
                "to_claim": to_claim,
                "currency": {"code": "XRP", "scale": 0.000001},
            },
        )

    return to_claim

def validate(parsed_claim, ledger_client, settle_delay):

    account_channels = xrpl.models.requests.AccountChannels(
        account=parsed_claim["account"],
        destination_account=parsed_claim["destination_account"],
    )

    account_channels_response = ledger_client.request(account_channels)

    valid_channel = False
    if "channels" not in account_channels_response.to_dict()["result"]:
        raise HTTPException(
            status_code=402,
            detail=f"There were no valid claims found for the specified channel",
        )

    for channel in account_channels_response.to_dict()["result"]["channels"]:
        correct_del = channel["settle_delay"] == settle_delay
        correct_src = channel["account"] == parsed_claim["account"]
        correct_dst = (
            channel["destination_account"] == parsed_claim["destination_account"]
        )
        correct_cha = channel["channel_id"] == parsed_claim["channel_id"]
        correct_amt = int(channel["amount"]) >= int(parsed_claim["authorized_to_claim"])
        expirable_channel = (
            "cancel_after" in channel.keys()
        )  # For our safety, we never accept channels that are expirable
        if (
            correct_src
            and correct_dst
            and correct_del
            and correct_cha
            and correct_amt
            and not expirable_channel
        ):
            valid_channel = True
            break
    if not valid_channel:
        raise HTTPException(
            status_code=402,
            detail=f"Your claim is invalid: correct_delay={correct_del}, correct_src={correct_src}, correct_dest={correct_dst}, correct_channel_id={correct_cha}, correct_amt={correct_amt}, not expirable_channel={not expirable_channel}",
        )

    channel_verify = xrpl.models.requests.ChannelVerify(
        amount=parsed_claim["authorized_to_claim"],
        channel_id=parsed_claim["channel_id"],
        public_key=channel["public_key"],
        signature=parsed_claim["signature"],
    )
    channel_verify_response = ledger_client.request(channel_verify)

    if (
        channel_verify_response.to_dict()["status"] == "error"
        or not channel_verify_response.to_dict()["result"]["signature_verified"]
    ):
        raise HTTPException(
            status_code=402, detail=f"Your signature could not be verified"
        )

request_charge_header_key = "Dhali-Latest-Request-Charge"
request_total_charge_header_key = "Dhali-Total-Requests-Charge"


def convert_dollars_to_xrp(dollars: float):
    if dollars < 0:
        raise ValueError("You must provide a non-negative dollar value")
    rate = 2.5  # TODO fetch dynamically
    return dollars * rate


def determine_cost_dollars(
    runtime_ms: float,
    machine_type: str = "TODO",
    request_size_bytes: int = 1,
    response_size_bytes: int = 1,
):
    if machine_type not in ["TODO"]:
        raise ValueError("Unsupported machine type requested")
    elif runtime_ms < 0:
        raise ValueError("Runtimes must be positive")
    elif request_size_bytes < 0:
        raise ValueError("The size of a request cannot be negative")
    elif response_size_bytes < 0:
        raise ValueError("The size of a response cannot be negative")

    fudge_factor = (
        5  # TODO Introducing this to ensure we do not under charge (must be >= 1)
    )

    if fudge_factor < 1:
        raise HTTPException(status_code=400, detail="Cost could not be determined")

    GiB_s_dollars_price = (
        0.000002905 * fudge_factor
    )  # TODO Fetch dynamically: currently  set to max as of 24/01/23

    GiB_memory = 1  # TODO 'machine_type' should determine this
    consumed_GiB_s = (
        GiB_memory * runtime_ms * request_size_bytes * response_size_bytes / 1000
    )

    return consumed_GiB_s * GiB_s_dollars_price


@firestore.transactional
def _transactional_update_estimated_cost_with_exact(transaction, 
                                                    public_doc_ref,
                                                    private_doc_ref,
                                                    single_request_cost_estimate: int, 
                                                    single_request_exact_cost: int):
    private_payment_channels_doc = next(transaction.get(private_doc_ref))
    public_payment_channels_doc = next(transaction.get(public_doc_ref))

    if public_payment_channels_doc.exists and private_payment_channels_doc.exists: # TODO: Remove private_payment_claim_doc
                                                                             # clause once migrated other code to use
                                                                             # public firestore
        to_claim = (
            public_payment_channels_doc.to_dict()["to_claim"]
            - single_request_cost_estimate
            + single_request_exact_cost
        )
        
        transaction.update(public_doc_ref, {"to_claim": to_claim})
        transaction.update(private_doc_ref, {"to_claim": to_claim}) # TODO: Remove this line once migrated
                                                                                  # other code to use public firestore
        return to_claim

    raise HTTPException(status_code=402)


# TODO: Make this transactional!
async def update_estimated_cost_with_exact(
    claim, single_request_cost_estimate: int, single_request_exact_cost: int, db
) -> float:
    """
    TODO

    Parameters
    ----------
    client : xrpl.clients.JsonRpcClient
    claim : str
    single_request_cost_estimate : int
    exact_cost : int

    :raises:
        HTTPException: 402 status code HTTPException

    Returns
    -------
    void
    """
    try:
        parsed_claim = json.loads(claim)
    except ValueError as e:
        raise HTTPException(
            status_code=402,
            detail=f"You must provide a payment channel claim that can be parsed via json.loads. Error: {e}.",
        )

    if (
        "channel_id" not in parsed_claim.keys()
        or single_request_exact_cost <= 0
        or single_request_cost_estimate <= 0
    ):
        raise HTTPException(
            status_code=402,
        )

    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, parsed_claim["channel_id"]))
    public_collection_name = "payment_channels"
    private_collection_name = "public_claim_info"

    public_payment_claim_doc_ref = db.collection(public_collection_name).document(uuid_channel_id)
    private_payment_claim_doc_ref = db.collection(private_collection_name).document(uuid_channel_id)
    
    transaction = db.transaction()
    to_claim = _transactional_update_estimated_cost_with_exact(transaction, 
                                                           public_payment_claim_doc_ref, 
                                                           private_payment_claim_doc_ref, 
                                                           single_request_cost_estimate, 
                                                           single_request_exact_cost)
    return to_claim



# TODO - Test this!
async def validate_claim(
    client, claim, single_request_cost_estimate: int, db, destination_account: str, settle_delay=15768000
):
    """
    TODO

    Parameters
    ----------
    client : xrpl.clients.JsonRpcClient
        Client used to verify cryptographic claimd
    claim : str
        The claim to be verified
    single_request_cost_estimate : int
        The single_request_cost_estimate to be claimed from the channel.
        This function will take 'single_request_cost_estimate' and add it to our record of
        other costs that dhali has against the channel. 'claim' should be
        able to support all previous claims dhali has, plus single_request_cost_estimate
    settle_delay : int
        The amount of time (seconds) that must elapse after requesting a payment channel to close
        before it actually closes. Defaults to 6 month

    :raises:
        HTTPException: 402 status code HTTPException is raised if claim is invalid

    Returns
    -------
    void
    """
    try:
        parsed_claim = json.loads(claim)
    except ValueError as e:
        raise HTTPException(
            status_code=402,
            detail="You must provide a payment channel claim that can be parsed via json.loads",
        )

    # The following keys must be present in the Json request claim. These are defined here:
    # https://xrpl.org/account_channels.html

    keys = [
        "account",
        "destination_account",
        "signature",
        "channel_id",
        "authorized_to_claim",
    ]

    for key in keys:
        if key not in parsed_claim.keys():
            raise HTTPException(
                status_code=402,
                detail=f"Your claim must be in Json format, providing the following fields: {keys}",
            )

    if destination_account != parsed_claim["destination_account"]:
        raise HTTPException(
            status_code=402,
            detail=f"Your claim has an incorrect destination_account",
        )


    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, parsed_claim["channel_id"]))
    private_collection_name = "payment_channels"
    public_collection_name = "public_claim_info"

    transaction = db.transaction()
    public_payment_claim_doc_ref = db.collection(public_collection_name).document(uuid_channel_id)
    private_payment_claim_doc_ref = db.collection(private_collection_name).document(uuid_channel_id)

    to_claim = _transactional_validation(
        transaction,
        public_payment_claim_doc_ref,
        private_payment_claim_doc_ref,
        ledger_client=client,
        parsed_claim=parsed_claim,
        single_request_cost_estimate=single_request_cost_estimate,
        settle_delay=settle_delay,
    )

    return to_claim
