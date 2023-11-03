from fastapi import HTTPException
import datetime
from datetime import datetime, timedelta, timezone


class RateLimitStrategy:
    """
    Base rate limit strategy class.

    Provides a default implementation for rate limiting checks, which always returns False,
    meaning no rate limiting by default.
    """

    def should_limit(self, *args, **kwargs):
        """
        Determine if a request should be rate limited.

        :return: False by default, meaning no rate limiting.
        """
        return False


class PaymentClaimBufferStrategy(RateLimitStrategy):
    """
    A specific rate limit strategy based on the number of claims staged and their timestamps.

    Rate limits if the number of claims staged exceeds the limit within the last {seconds_to_apply_over} second.
    """

    def __init__(self, claim_buffer_size_limit: int, seconds_to_apply_over: float = 1):
        """
        Initializes with a specified claim buffer size limit.

        :param claim_buffer_size_limit: The maximum number of claims allowed within the buffer.
        """
        self.apply_over_last = seconds_to_apply_over
        self._claim_buffer_size_limit = claim_buffer_size_limit

    def should_limit(self, *args, **kwargs):
        """
        Determine if a request should be rate limited based on claim count and timestamp.

        :return: True if the claims exceed the limit and the timestamp is within the last {self.time_difference}, else False.
        """
        if (
            "number_of_claims_staged" in kwargs
            and kwargs["number_of_claims_staged"] >= self._claim_buffer_size_limit
        ):
            if "timestamp" in kwargs:
                timestamp = kwargs["timestamp"]
                utc_now = datetime.now(timezone.utc)  # This is now offset-aware, set to UTC
                time_difference = utc_now - timestamp

                if time_difference < timedelta(seconds=self.apply_over_last):
                    return True
        return False


class NFTMetaBufferStrategy(RateLimitStrategy):
    """
    A specific rate limit strategy based on the number of metadata updates staged, and their timestamps.

    Rate limits if the number of metadata updates staged exceeds the limit within the last {seconds_to_apply_over} second.
    """

    def __init__(self, buffer_size_limit: int, seconds_to_apply_over: float = 1):
        """
        Initializes with a specified metadata buffer size limit.

        :param buffer_size_limit: The maximum number of metadata updates allowed within the buffer.
        """
        self.apply_over_last = seconds_to_apply_over
        self._buffer_size_limit = buffer_size_limit

    def should_limit(self, *args, **kwargs):
        """
        Determine if a request should be rate limited based on claim count and timestamp.

        :return: True if the claims exceed the limit and the timestamp is within the last {self.time_difference}, else False.
        """
        if (
            "number_of_metadata_updates_staged" in kwargs
            and kwargs["number_of_metadata_updates_staged"] >= self._buffer_size_limit
        ):
            if "timestamp" in kwargs:
                timestamp = kwargs["timestamp"]
                utc_now = datetime.now(timezone.utc)  # This is now offset-aware, set to UTC
                time_difference = utc_now - timestamp

                if time_difference < timedelta(seconds=self.apply_over_last):
                    return True
        return False


class RateLimiter:
    """
    A callable rate limiter that utilizes a rate limiting strategy to determine if a request should be limited.
    """

    def __init__(self, strategy: RateLimitStrategy = RateLimitStrategy()):
        """
        Initializes with a specified rate limiting strategy.

        :param strategy: The rate limiting strategy to be used. Defaults to the base RateLimitStrategy.
        """
        self.strategy = strategy

    def __call__(self, *args, **kwargs):
        """
        Checks if a request should be rate limited based on the strategy.

        Raises an exception if the request is to be rate limited.
        """
        if self.strategy.should_limit(*args, **kwargs):
            raise HTTPException(status_code=429, detail="Too Many Requests")


if __name__ == "__main__":
    # Usage
    payment_claim_buffer_strategy = PaymentClaimBufferStrategy(
        claim_buffer_size_limit=5
    )
    payment_claim_buffer_limiter = RateLimiter(payment_claim_buffer_strategy)

    claim = {"number_of_claims_staged": 6, "timestamp": datetime.datetime.utcnow()}
    try:
        payment_claim_buffer_limiter(
            **claim
        )  # Checks the payment claim buffer strategy
    except HTTPException as e:
        print(f"This exception was expected: {e}")
