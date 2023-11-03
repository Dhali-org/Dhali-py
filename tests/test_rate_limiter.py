import pytest
from fastapi import HTTPException
from dhali.rate_limiter import (
    PaymentClaimBufferStrategy,
    NFTMetaBufferStrategy,
    RateLimiter,
)
from datetime import datetime, timedelta, timezone



def test_default_strategy_does_not_limit():
    limiter = RateLimiter()
    assert limiter() is None  # Default strategy does not limit


def test_claim_buffer_strategy_limits():
    strategy = PaymentClaimBufferStrategy(claim_buffer_size_limit=5)
    limiter = RateLimiter(strategy)

    claim_within_limit = {
        "number_of_claims_staged": 4,
        "timestamp": datetime.now(timezone.utc),
    }

    assert limiter(**claim_within_limit) is None  # Does not limit

    claim_exceeding_limit = {
        "number_of_claims_staged": 6,
        "timestamp": datetime.now(timezone.utc),
    }

    with pytest.raises(HTTPException) as excinfo:
        limiter(**claim_exceeding_limit)
    assert str(excinfo.value.detail) == "Too Many Requests"


def test_claim_buffer_strategy_timestamps():
    strategy = PaymentClaimBufferStrategy(claim_buffer_size_limit=5)
    limiter = RateLimiter(strategy)

    recent_timestamp = datetime.now(timezone.utc) - timedelta(milliseconds=500)
    claim_with_recent_timestamp = {
        "number_of_claims_staged": 6,
        "timestamp": recent_timestamp,
    }

    with pytest.raises(HTTPException) as excinfo:
        limiter(**claim_with_recent_timestamp)
    assert str(excinfo.value.detail) == "Too Many Requests"

    older_timestamp = datetime.now(timezone.utc) - timedelta(minutes=10)
    claim_with_older_timestamp = {
        "number_of_claims_staged": 6,
        "timestamp": older_timestamp,
    }

    assert (
        limiter(**claim_with_older_timestamp) is None
    )  # Does not limit due to older timestamp


def test_nft_buffer_strategy_limits():
    strategy = NFTMetaBufferStrategy(buffer_size_limit=5)
    limiter = RateLimiter(strategy)

    nft_within_limit = {
        "number_of_metadata_updates_staged": 4,
        "timestamp": datetime.now(timezone.utc),
    }

    assert limiter(**nft_within_limit) is None  # Does not limit

    nft_exceeding_limit = {
        "number_of_metadata_updates_staged": 6,
        "timestamp": datetime.now(timezone.utc),
    }

    with pytest.raises(HTTPException) as excinfo:
        limiter(**nft_exceeding_limit)
    assert str(excinfo.value.detail) == "Too Many Requests"


def test_nft_buffer_strategy_timestamps():
    strategy = NFTMetaBufferStrategy(buffer_size_limit=5)
    limiter = RateLimiter(strategy)

    recent_timestamp = datetime.now(timezone.utc) - timedelta(milliseconds=500)
    nft_with_recent_timestamp = {
        "number_of_metadata_updates_staged": 6,
        "timestamp": recent_timestamp,
    }

    with pytest.raises(HTTPException) as excinfo:
        limiter(**nft_with_recent_timestamp)
    assert str(excinfo.value.detail) == "Too Many Requests"

    older_timestamp = datetime.now(timezone.utc) - timedelta(minutes=10)
    nft_with_older_timestamp = {
        "number_of_metadata_updates_staged": 6,
        "timestamp": older_timestamp,
    }

    assert (
        limiter(**nft_with_older_timestamp) is None
    )  # Does not limit due to older timestamp
