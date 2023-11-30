import asyncio
import datetime
from unittest import mock
from fastapi import HTTPException
import mockfirestore
import mockito
import pytest
import uuid
import json

import xrpl
from dhali import rate_limiter

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

@pytest.mark.asyncio
async def test_payment_claim_estimate_limited():
    """Test to make sure that the rate limiter is being applied"""


    authorized_amount = 9000
    number_claims_staged = 10
    valid_signature = "some_valid_signature"
    some_valid_account = "a_valid_source_account"
    some_other_valid_account = "a_valid_destination_account"

    claim = {
                "account": f"{some_valid_account}", 
                "destination_account" : f"{some_other_valid_account}", 
                "authorized_to_claim": f"{authorized_amount}", 
                "signature": f"{valid_signature}", 
                "channel_id": "some_valid_channel_id"
            }

    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, claim["channel_id"]))
    db = mockito.spy(mockfirestore.MockFirestore())
    db.collection("payment_channels").document(uuid_channel_id).set({
        "authorized_to_claim": claim["authorized_to_claim"],
        "currency": {"code": "XRP", "scale": 0.000001},
        "to_claim": 5,
        "payment_claim": json.dumps(claim),
        "timestamp": datetime.datetime.now(datetime.timezone.utc),
        "number_of_claims_staged": number_claims_staged
    })
 
    mock_xrpl_json_rpc = mock.Mock()
    payment_claim_buffer_strategy = rate_limiter.PaymentClaimBufferStrategy(
        claim_buffer_size_limit=number_claims_staged
    )
    payment_claim_buffer_limiter = rate_limiter.RateLimiter(payment_claim_buffer_strategy)

    # Check that it raises
    with pytest.raises(HTTPException) as e:
        await dtx.throw_if_claim_invalid(
                client=mock_xrpl_json_rpc,
                claim=json.dumps(claim),
                single_request_cost_estimate=5,
                db=db,
                destination_account=some_other_valid_account,
                rate_limiter=payment_claim_buffer_limiter
            )
    # Also check that it has the 429 code
    try:
        await dtx.throw_if_claim_invalid(
            client=mock_xrpl_json_rpc,
            claim=json.dumps(claim),
            single_request_cost_estimate=5,
            db=db,
            destination_account=some_other_valid_account,
            rate_limiter=payment_claim_buffer_limiter
        )
    except HTTPException as e:
        assert e.status_code == 429
    

@pytest.mark.asyncio
async def test_xrpl_client_not_called_with_duplicate_claim():
    """Test to make sure xrpl client not called for duplicate claim"""

    claim = {"account": "some_valid_account", "destination_account": "some_valid_account", "authorized_to_claim": "9001", "signature": "some_valid_signature", "channel_id": "some_valid_channel_id"}

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

    mock_xrpl_json_rpc = mock.Mock()

    await dtx.throw_if_claim_invalid(
        client=mock_xrpl_json_rpc,
        claim=json.dumps(claim),
        single_request_cost_estimate=5,
        db=db,
        destination_account="some_valid_account"
    )

    mock_xrpl_json_rpc.assert_not_called

@pytest.mark.asyncio
async def test_payment_claim_estimate_fails():
    """Test to make sure that validate_estimated_claim raises 402"""

    # Should raise 402 because to_claim_amount + single_request_cost_estimate > authorized_amount
    authorized_amount = 9000
    to_claim_amount = 8996
    single_request_cost_estimate = 5

    valid_signature = "some_valid_signature"
    some_valid_account = "a_valid_source_account"
    some_other_valid_account = "a_valid_destination_account"

    claim = {
                "account": f"{some_valid_account}", 
                "destination_account" : f"{some_other_valid_account}", 
                "authorized_to_claim": f"{authorized_amount}", 
                "signature": f"{valid_signature}", 
                "channel_id": "some_valid_channel_id"
            }

    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, claim["channel_id"]))
    db = mockito.spy(mockfirestore.MockFirestore())
    db.collection("payment_channels").document(uuid_channel_id).set({
        "authorized_to_claim": claim["authorized_to_claim"],
        "currency": {"code": "XRP", "scale": 0.000001},
        "to_claim": to_claim_amount,
        "payment_claim": json.dumps(claim),
        "timestamp": datetime.datetime.now(datetime.timezone.utc),
        "number_of_claims_staged": 1
    })
 
    mock_xrpl_json_rpc = mock.Mock()

    # Check that it raises
    with pytest.raises(HTTPException) as e:
        await dtx.throw_if_claim_invalid(
                client=mock_xrpl_json_rpc,
                claim=json.dumps(claim),
                single_request_cost_estimate=single_request_cost_estimate,
                db=db,
                destination_account=some_other_valid_account,
            )
    # Also check that it has the 402 code
    try:
        await dtx.throw_if_claim_invalid(
            client=mock_xrpl_json_rpc,
            claim=json.dumps(claim),
            single_request_cost_estimate=single_request_cost_estimate,
            db=db,
            destination_account=some_other_valid_account,
        )
    except HTTPException as e:
        assert e.status_code == 402

@pytest.mark.asyncio
async def test_payment_claim_estimate_passes():
    """Test to make sure that validate_estimated_claim does not raise"""

    # Should not raise 402 because to_claim_amount + single_request_cost_estimate < authorized_amount
    authorized_amount = 9000
    to_claim_amount = 8994
    single_request_cost_estimate = 5
    
    valid_signature = "some_valid_signature"
    some_valid_account = "a_valid_source_account"
    some_other_valid_account = "a_valid_destination_account"

    claim = {
                "account": f"{some_valid_account}", 
                "destination_account" : f"{some_other_valid_account}", 
                "authorized_to_claim": f"{authorized_amount}", 
                "signature": f"{valid_signature}", 
                "channel_id": "some_valid_channel_id"
            }

    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, claim["channel_id"]))
    db = mockito.spy(mockfirestore.MockFirestore())
    db.collection("payment_channels").document(uuid_channel_id).set({
        "authorized_to_claim": claim["authorized_to_claim"],
        "currency": {"code": "XRP", "scale": 0.000001},
        "to_claim": to_claim_amount,
        "payment_claim": json.dumps(claim),
        "timestamp": datetime.datetime.now(datetime.timezone.utc),
        "number_of_claims_staged": 1
    })
 
    mock_xrpl_json_rpc = mock.Mock()

    # Should not raise
    await dtx.throw_if_claim_invalid(
                client=mock_xrpl_json_rpc,
                claim=json.dumps(claim),
                single_request_cost_estimate=5,
                db=db,
                destination_account=some_other_valid_account,
            )

@pytest.mark.asyncio
async def test_payment_claim_estimate_and_exact():
    """Test to make sure that the payment documents are updated as new claims come through"""


    authorized_amount = 9000
    valid_signature = "some_valid_signature"
    some_valid_account = "a_valid_source_account"
    some_other_valid_account = "a_valid_destination_account"

    claim = {"account": f"{some_valid_account}", "destination_account" : f"{some_other_valid_account}", "authorized_to_claim": f"{authorized_amount}", "signature": f"{valid_signature}", "channel_id": "some_valid_channel_id"}

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
    ]

    db = mockito.spy(mockfirestore.MockFirestore())


    private_collection_name = "payment_channels"
    estimates_collection_name = "estimate"
    exact_collection_name = "exact"
    
    uuid_channel_id = str(uuid.uuid5(uuid.NAMESPACE_URL, claim["channel_id"]))

    await dtx.throw_if_claim_invalid(
        client=mock_xrpl_json_rpc,
        claim=json.dumps(claim),
        single_request_cost_estimate=5,
        db=db,
        destination_account=some_other_valid_account
    )
    
    assert not db.collection(private_collection_name).document(uuid_channel_id).get().exists, "Root should be empty"
    doc_uuid = await dtx.store_exact_claim(json.dumps(claim), 3, db)
    assert list(db.collection(private_collection_name).document(uuid_channel_id).get().to_dict().keys()) == ["exact"], "Root should be empty, but contain the \"exact\" subcollection"
    assert not db.collection(private_collection_name).document(uuid_channel_id).collection(estimates_collection_name).document(doc_uuid).get().exists
    # Ensure the new exact is created
    assert db.collection(private_collection_name).document(uuid_channel_id).collection(exact_collection_name).document(doc_uuid).get().to_dict()["authorized_to_claim"] == f"{authorized_amount}", "authorized_to_claim not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).collection(exact_collection_name).document(doc_uuid).get().to_dict()["payment_claim"] == json.dumps(claim), "payment_claim not updated correctly in firestore"
    assert db.collection(private_collection_name).document(uuid_channel_id).collection(exact_collection_name).document(doc_uuid).get().to_dict()["to_claim"] == 3, "to_claim not updated correctly in firestore"

def test_root_private_payment_claim_doc_ref_returns_none_and_doc_does_not_exist():
    
    root_private_payment_claim_doc_ref = mock.Mock()

    # Setting up the mock to return None when get().to_dict() is called
    root_private_payment_claim_doc_ref.get.return_value.to_dict.return_value = None
    root_private_payment_claim_doc_ref.get.return_value.exists = False

    # When: Calling the function under test
    try:
        dtx._validation(None, root_private_payment_claim_doc_ref, None, None, None, None, None)
    except TypeError as e:
        # Check if the exception message contains key substrings related to unpacking operator
        if "argument after **" in str(e) and "not NoneType" in str(e):
            raise Exception(f"_validation raised a specific TypeError related to unpacking unexpectedly: {e}")



@pytest.mark.asyncio
async def test_concurrent_move_document():
    source_collection_name = 'sourceCollection'
    target_collection_name = 'targetCollection'
    document_id = 'myDoc'
    concurrent_requests = 100

    # Initialize a spy Firestore client
    db = mockito.spy(mockfirestore.MockFirestore())


    source_ref = db.collection(source_collection_name).document(document_id)
    source_ref.set({'field': 'value'})

    async def move_document_coroutine(idx):
        target_ref = db.collection(target_collection_name).document(document_id + str(idx))
        
        await asyncio.sleep(0)  # Yield control to the event loop
        dtx.move_document(db, source_ref, target_ref)

    # Run move_document concurrently in `concurrent_requests` coroutines
    await asyncio.gather(*(move_document_coroutine(idx) for idx in range(concurrent_requests)))

    assert not db.collection(source_collection_name).document(document_id).get().exists, "Document still exists in source collection!"
    
    target_docs = [db.collection(target_collection_name).document(document_id + str(idx)).get().exists == True for idx in range(concurrent_requests)]
    assert sum(target_docs) == 1, "More than one document found in the target collection!"

    for idx in range(concurrent_requests):
        # Clean up: Delete the moved document from the target collection after the test
        db.collection(target_collection_name).document(document_id + str(idx)).delete()

@pytest.mark.asyncio
async def test_concurrent_consolidate_documents():
    source_collection_name = 'sourceCollection'
    target_collection_name = 'targetCollection'
    public_target_collection_name = 'targetCollectionPublic'
    document_id = 'myDoc'
    db = mockito.spy(mockfirestore.MockFirestore())
    concurrent_requests = 100
    
    async def consolidate_documents_coroutine(idx, source_docs):
        target_ref = db.collection(target_collection_name).document(idx)
        public_target_ref = db.collection(public_target_collection_name).document(idx)
        await asyncio.sleep(0)  # Yield control to the event loop
        dtx.consolidate_payment_claim_documents(db, source_docs, target_ref, public_target_ref)

    def prepare_source_collection(unconsolidated_claim_data):
        for idx, data in enumerate(unconsolidated_claim_data):
            db.collection(source_collection_name).document(document_id + str(idx)).set(data)

        source_docs = []
        for doc in db.collection(source_collection_name).stream():
            source_docs.append(doc)
        return source_docs

    #####################
    # First consolidation
    #####################
    unconsolidated_claim_data = [{
                    "authorized_to_claim": "4",
                    "to_claim": 1,
                    "payment_claim": "sig1",
                },
                {
                    "authorized_to_claim": "5",
                    "to_claim": 2,
                    "payment_claim": "sig2",
                },
                {
                    "authorized_to_claim": "6",
                    "to_claim": 3,
                    "payment_claim": "largest signatire",
                }]

    source_docs = prepare_source_collection(unconsolidated_claim_data)

    # Run consolidate_payment_claim_documents concurrently in coroutines
    await asyncio.gather(*(consolidate_documents_coroutine(document_id + str(idx), source_docs) for idx in range(concurrent_requests)))
    
    target_docs = []
    idx_inserted_at = 0
    for idx in range(concurrent_requests):
        if db.collection(target_collection_name).document(document_id + str(idx)).get().exists == True:
            idx_inserted_at = idx
            target_docs.append(db.collection(target_collection_name).document(document_id + str(idx)).get().to_dict())
    
    idx_public_inserted_at = 0
    public_target_docs = []
    for idx in range(concurrent_requests):
        if db.collection(public_target_collection_name).document(document_id + str(idx)).get().exists == True:
            idx_public_inserted_at = idx
            public_target_docs.append(db.collection(public_target_collection_name).document(document_id + str(idx)).get().to_dict())
    
    for idx, doc in enumerate(source_docs):
        with pytest.raises(KeyError):
            fresh_doc = await doc.reference.get()
            assert not fresh_doc.exists, f"At least one source document (idx {idx}) was not deleted!"
    assert len(target_docs) == 1, "More than one document found in the target collection!"
    assert target_docs[0]["authorized_to_claim"] == "6", "Authorised to claim is incorrect"
    assert target_docs[0]["to_claim"] == 6, "To claim should be the sum of all claims"
    assert target_docs[0]["payment_claim"] == "largest signatire", "The payment claim should correspond to authorized_to_claim"
    assert target_docs[0]["number_of_claims_staged"] == 3

    assert idx_public_inserted_at == idx_inserted_at

    assert len(public_target_docs) == 1, "More than one document found in the target collection!"
    assert not "authorized_to_claim" in public_target_docs[0], "Authorised to claim is incorrect"
    assert public_target_docs[0]["to_claim"] == 6, "To claim should be the sum of all claims"
    assert not "payment_claim" in public_target_docs[0], "The payment claim should correspond to authorized_to_claim"

    ######################
    # Second consolidation
    ######################
    unconsolidated_claim_data = [{
                    "authorized_to_claim": "8",
                    "to_claim": 1,
                    "payment_claim": "sig3",
                },
                {
                    "authorized_to_claim": "9",
                    "to_claim": 2,
                    "payment_claim": "sig4",
                },
                {
                    "authorized_to_claim": "10",
                    "to_claim": 1.1,
                    "payment_claim": "new largest signatire",
                }]

    source_docs = prepare_source_collection(unconsolidated_claim_data)  

    await asyncio.gather(*(consolidate_documents_coroutine(document_id + str(idx_inserted_at), source_docs) for _ in range(concurrent_requests)))
    
    idx_inserted_at = 0
    target_docs = []
    for idx in range(concurrent_requests):
        if db.collection(target_collection_name).document(document_id + str(idx)).get().exists == True:
            idx_inserted_at = idx
            target_docs.append(db.collection(target_collection_name).document(document_id + str(idx)).get().to_dict())
    idx_public_inserted_at = 0
    public_target_docs = []
    for idx in range(concurrent_requests):
        if db.collection(public_target_collection_name).document(document_id + str(idx)).get().exists == True:
            idx_public_inserted_at = idx
            public_target_docs.append(db.collection(public_target_collection_name).document(document_id + str(idx)).get().to_dict())
    for idx, doc in enumerate(source_docs):
        with pytest.raises(KeyError):
            fresh_doc = await doc.reference.get()
            assert not fresh_doc.exists, f"At least one source document (idx {idx}) was not deleted!"
    assert len(target_docs) == 1, "More than one document found in the target collection!"
    assert target_docs[0]["authorized_to_claim"] == "10", "Authorised to claim is incorrect"
    assert target_docs[0]["to_claim"] == 10.1, "To claim should be the sum of all claims"
    assert target_docs[0]["payment_claim"] == "new largest signatire", "The payment claim should correspond to authorized_to_claim"
    assert target_docs[0]["number_of_claims_staged"] == 3
    
    assert idx_public_inserted_at == idx_inserted_at
    
    assert len(public_target_docs) == 1, "More than one document found in the target collection!"
    assert not "authorized_to_claim" in public_target_docs[0], "Authorised to claim is incorrect"
    assert public_target_docs[0]["to_claim"] == 10.1, "To claim should be the sum of all claims"
    assert not "payment_claim" in public_target_docs[0], "The payment claim should correspond to authorized_to_claim"


@pytest.mark.asyncio
async def test_private_exists_but_public_does_not():
    source_collection_name = 'sourceCollection'
    target_collection_name = 'targetCollection'
    public_target_collection_name = 'targetCollectionPublic'
    document_id = 'myDoc'
    db = mockito.spy(mockfirestore.MockFirestore())
    
    data = {
                    "authorized_to_claim": "4",
                    "to_claim": 1,
                    "payment_claim": "sig1",
                }

    src_ref = db.collection(source_collection_name).document("test")
    src_ref.set(data)

    target_ref = db.collection(target_collection_name).document("test")
    target_ref.set(data)

    public_target_ref = db.collection(public_target_collection_name).document("test")
    public_target_ref.delete()
    
    dtx.consolidate_payment_claim_documents(db, [src_ref.get()], target_ref, public_target_ref)

    target_docs = [target_ref.get().to_dict()]
    public_target_docs = [public_target_ref.get().to_dict()]

    with pytest.raises(KeyError):
        fresh_doc = await src_ref.get()
        assert not fresh_doc.exists, f"The source document was not deleted!"

    assert len(target_docs) == 1, "More than one document found in the target collection!"
    assert target_docs[0]["authorized_to_claim"] == "4", "Authorised to claim is incorrect"
    assert target_docs[0]["to_claim"] == 2, "To claim should be the sum of all claims"
    assert target_docs[0]["payment_claim"] == "sig1", "The payment claim should correspond to authorized_to_claim"
    assert target_docs[0]["number_of_claims_staged"] == 1

    assert len(public_target_docs) == 1, "More than one document found in the target collection!"
    assert not "authorized_to_claim" in "4", "Authorised to claim is incorrect"
    assert public_target_docs[0]["to_claim"] == 2, "To claim should be the sum of all claims"
    assert not "payment_claim" in "sig1", "The payment claim should correspond to authorized_to_claim"
