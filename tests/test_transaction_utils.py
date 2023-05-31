from unittest import mock
import mockfirestore
import mockito
import pytest
import uuid
import json

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

    public_collection_name = "payment_channels"
    private_collection_name = "public_claim_info"

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
