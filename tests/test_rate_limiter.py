import pytest
from fastapi import HTTPException
from dhali.rate_limiter import PaymentClaimBufferStrategy, RateLimiter
import datetime

def test_default_strategy_does_not_limit():
    limiter = RateLimiter()
    assert limiter() is None  # Default strategy does not limit

def test_claim_buffer_strategy_limits():
    strategy = PaymentClaimBufferStrategy(claim_buffer_size_limit=5)
    limiter = RateLimiter(strategy)

    claim_within_limit = {
        "number_of_claims_staged": 4,
        "timestamp": datetime.datetime.utcnow()
    }
    
    assert limiter(**claim_within_limit) is None  # Does not limit

    claim_exceeding_limit = {
        "number_of_claims_staged": 6,
        "timestamp": datetime.datetime.utcnow()
    }
    
    with pytest.raises(HTTPException) as excinfo:
        limiter(**claim_exceeding_limit)
    assert str(excinfo.value.detail) == "Too Many Requests"

def test_claim_buffer_strategy_timestamps():
    strategy = PaymentClaimBufferStrategy(claim_buffer_size_limit=5)
    limiter = RateLimiter(strategy)
    
    recent_timestamp = datetime.datetime.utcnow() - datetime.timedelta(milliseconds=500)
    claim_with_recent_timestamp = {
        "number_of_claims_staged": 6,
        "timestamp": recent_timestamp
    }
    
    with pytest.raises(HTTPException) as excinfo:
        limiter(**claim_with_recent_timestamp)
    assert str(excinfo.value.detail) == "Too Many Requests"

    older_timestamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
    claim_with_older_timestamp = {
        "number_of_claims_staged": 6,
        "timestamp": older_timestamp
    }
    
    assert limiter(**claim_with_older_timestamp) is None  # Does not limit due to older timestamp

