from unittest import mock
import mockfirestore
import mockito
import pytest
import uuid
import json

import xrpl

import dhali.transaction_utils as dtx

__author__ = "Dhali-org"
__copyright__ = "Dhali-org"
__license__ = "MIT"


def test_rate_converters_dollars_to_xrp():
    """Test Dollar to XRP converter"""
    assert dtx.convert_dollars_to_xrp(0) == 0
    assert dtx.convert_dollars_to_xrp(1) == 2.5
    assert dtx.convert_dollars_to_xrp(2.234) == pytest.approx(2.234 * 2.5, 1e-8)
    with pytest.raises(ValueError):
        dtx.convert_dollars_to_xrp(-1)

def test_determine_cost_dollars():
    """Test determine instance cost in dollars"""
    fudge_factor = 5
    GiB_s_dollars_price = (
        0.000002905 * fudge_factor
    )
    GiB_memory = 1  # TODO 'machine_type' should determine this
    assert dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = 2, response_size_bytes = 3) == pytest.approx(GiB_s_dollars_price * GiB_memory * 20 * 2 * 3 / 1000)
    assert dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = 1.982, response_size_bytes = 1.7835) == pytest.approx(GiB_s_dollars_price * GiB_memory * 20 * 1.982 * 1.7835 / 1000)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODONT", runtime_ms = 20, request_size_bytes = 2, response_size_bytes = 3)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = -1, request_size_bytes = 2, response_size_bytes = 3)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = -2, response_size_bytes = 3)
    with pytest.raises(ValueError):
        dtx.determine_cost_dollars(machine_type = "TODO", runtime_ms = 20, request_size_bytes = 2, response_size_bytes = -3)

@pytest.mark.asyncio
async def test_to_claim_in_sync():
    """Test to make sure that `to_claim` is kept in sync across firestore databases"""

    claim = {"account": "some_valid_account", "destination_account": "some_other_valid_account", "authorized_to_claim": "9001", "signature": "some_valid_signature", "channel_id": "some_valid_channel_id"}

    db = mockito.spy(mockfirestore.MockFirestore())

    public_collection_name = "public_claim_info"
    private_collection_name = "payment_channels"

    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, claim["channel_id"]))

    private_payment_claim_doc_ref = db.collection(private_collection_name).document(uuid_channel_id)
    public_payment_claim_doc_ref = db.collection(public_collection_name).document(uuid_channel_id)

    private_payment_claim_doc_ref.set({
        "authorized_to_claim": claim["authorized_to_claim"],
        "currency": {"code": "XRP", "scale": 0.000001},
        "to_claim": 5,
        "payment_claim": json.dumps(claim),
    })
    public_payment_claim_doc_ref.set({
        "to_claim": 5,
        "currency": {"code": "XRP", "scale": 0.000001},
    })

    the_expected_to_claim = await dtx.update_estimated_cost_with_exact(
        claim=json.dumps(claim),
        single_request_cost_estimate=5,
        single_request_exact_cost=20,
        db=db,
    )

    assert db.collection(public_collection_name).document(uuid_channel_id).get().to_dict()["to_claim"] == the_expected_to_claim, "Document not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["to_claim"] == the_expected_to_claim, "Document not updated correctly in firestore"

@pytest.mark.asyncio
async def test_payment_claim_updated():
    """Test to make sure that the payment documents are updated as new claims come through"""


    authorized_amount = 9000
    new_authorized_amount =10000
    valid_signature = "some_valid_signature"
    new_valid_signature = "new_valid_signature"
    some_valid_account = "a_valid_source_account"
    some_other_valid_account = "a_valid_destination_account"

    claim = {"account": f"{some_valid_account}", "destination_account" : f"{some_other_valid_account}", "authorized_to_claim": f"{authorized_amount}", "signature": f"{valid_signature}", "channel_id": "some_valid_channel_id"}
    updated_claim = {"account": f"{some_valid_account}", "destination_account": f"{some_other_valid_account}", "authorized_to_claim": f"{new_authorized_amount}", "signature" : f"{new_valid_signature}", "channel_id": "some_valid_channel_id"}

    result = {
                "account": "r4y5HTy8PLg32nNNZ7cuXTTHQQkVMvXz6a",
                "channels": [
                    {
                        "account": claim["account"],
                        "amount": "1000000",
                        "balance": "0",
                        "channel_id": claim["channel_id"],
                        "destination_account": claim["destination_account"],
                        "public_key": "aKEEmUpJxWmbJKb1W1vmPjpLTv8DxMMX6FBpfKXFCkzQNVNkDVhF",
                        "public_key_hex": "ED404F8DD7B7BFB0427032A6B7346C18972F713C2EA7C4477F52F5968477AEA69C",
                        "settle_delay": 15768000,
                    }
                ],
                "ledger_current_index": 32474977,
                "validated": False,
            }
    
    new_result = {
                "account": "r4y5HTy8PLg32nNNZ7cuXTTHQQkVMvXz6a",
                "channels": [
                    {
                        "account": updated_claim["account"],
                        "amount": "1000000",
                        "balance": "0",
                        "channel_id": updated_claim["channel_id"],
                        "destination_account": updated_claim["destination_account"],
                        "public_key": "aKEEmUpJxWmbJKb1W1vmPjpLTv8DxMMX6FBpfKXFCkzQNVNkDVhF",
                        "public_key_hex": "ED404F8DD7B7BFB0427032A6B7346C18972F713C2EA7C4477F52F5968477AEA69C",
                        "settle_delay": 15768000,
                    }
                ],
                "ledger_current_index": 32474977,
                "validated": False,
            }

    mock_xrpl_json_rpc = mock.Mock()
    mock_xrpl_json_rpc.request.side_effect = [
        xrpl.models.response.Response(
            status=xrpl.models.response.ResponseStatus("success"),
            result=result,
            type=xrpl.models.response.ResponseType("response"),
        ),
        xrpl.models.response.Response(
            status=xrpl.models.response.ResponseStatus("success"),
            result={"signature_verified": True},
            type=xrpl.models.response.ResponseType("response"),
        ),
        xrpl.models.response.Response(
            status=xrpl.models.response.ResponseStatus("success"),
            result=new_result,
            type=xrpl.models.response.ResponseType("response"),
        ),
        xrpl.models.response.Response(
            status=xrpl.models.response.ResponseStatus("success"),
            result={"signature_verified": True},
            type=xrpl.models.response.ResponseType("response"),
        ),
    ]

    db = mockito.spy(mockfirestore.MockFirestore())

    private_collection_name = "payment_channels"

    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, claim["channel_id"]))
    await dtx.validate_claim(
        client=mock_xrpl_json_rpc,
        claim=json.dumps(claim),
        single_request_cost_estimate=5,
        db=db,
        destination_account=some_other_valid_account
    )

    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["authorized_to_claim"] == f"{authorized_amount}", "authorized_to_claim not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["payment_claim"] == json.dumps(claim), "payment_claim not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["to_claim"] == 5, "to_claim not updated correctly in firestore"

    await dtx.validate_claim(
        client=mock_xrpl_json_rpc,
        claim=json.dumps(updated_claim),
        single_request_cost_estimate=5,
        db=db,
        destination_account=some_other_valid_account
    )

    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["authorized_to_claim"] == f"{new_authorized_amount}", "authorized_to_claim not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["payment_claim"] == json.dumps(updated_claim), "payment_claim not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).get().to_dict()["to_claim"] == 2 * 5, "to_claim not updated correctly in firestore"
